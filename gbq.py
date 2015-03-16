# For help run serviceaccount.py -h
# Using days ago flag replaces %1 in localPath path and query with the date (yyyymmdd) corresponding to days ago from current date, useful if running program from task scheduler instead of SSIS
# Examples of running job...
# Query without credential parameters: gbq.py -qu "SELECT date, fullvisitorid, visitnumber FROM [76949285.ga_sessions_20140418] LIMIT 10;"
# Query with credential parameters: gbq.py -pi 970979839129 -sa "970979839129-4dob09dg1edgrguke5erlte0vub6ljtr@developer.gserviceaccount.com" -kf tcne.p12 -qu "SELECT date, fullvisitorid, visitnumber FROM [76949285.ga_sessions_20140418] LIMIT 10;"
# Query, export and download results from cloud storage to local file : gbq.py -lp "./test/" -db tcne_temp -do qedrl_%1.csv -qu "SELECT date, fullvisitorid, visitnumber FROM [76949285.ga_sessions_20140418] LIMIT 10;"
# Large query with destinationTable, export and download results from cloud storage to local file: gbq.py -lp "./test/"  -db tcne_temp -do testtolocal_%1-*.csv -lr True -ds sahlin -dt querydest -wd WRITE_TRUNCATE -qu "SELECT * FROM [1_web_data.Travel] where calendardate >= '20150202' and calendardate <= '20150208';"
# Run Query with config file: gbq.py -cf ./session_export/session_export.cfg

import httplib2
import datetime
import pprint
import argparse
import ConfigParser
import time
import json
import io
import logging
import sys

from apiclient.discovery import build
from apiclient import discovery
from apiclient import http
from apiclient.errors import HttpError

from oauth2client.client import SignedJwtAssertionCredentials
from oauth2client.client import AccessTokenRefreshError

#Function to specify a query job
def AsyncQueryBatch(projectId,datasetId,tableId,allowLargeResults,createDisposition,writeDisposition,query):
	#Plain query job
	jobData = {
		'configuration': {
			'query': {
				'query': query,
				'priority': 'BATCH', # Set priority to BATCH
				'allowLargeResults': allowLargeResults
			}
		}
	}
	#If job should populate a destination table, add needed parameters in the configuration	
	if all((projectId,datasetId,tableId)):
		jobData['configuration']['query']['destinationTable'] =  {
			'projectId': projectId,
			'datasetId': datasetId,
			'tableId': tableId
		}
		jobData['configuration']['query']['createDisposition'] = createDisposition
		jobData['configuration']['query']['writeDisposition'] = writeDisposition
	return jobData

#Function to specify a big query export job.
def exportTable(projectId,datasetId, tableId,destinationBucket,destinationObject, destinationFormat):
	destinationUris = "gs://" + destinationBucket + "/" + destinationObject
	jobData = {
		'configuration': {
			'extract': {
				'sourceTable': {
					'projectId': projectId,
					'datasetId': datasetId,
					'tableId': tableId
				},
				'destinationUris': [destinationUris],
				'destinationFormat': destinationFormat,
			}
		}
	}
	return jobData

#Generic method to run Big Query jobs
def runJob(http, service,jobData,projectId):	
	jobCollection = service.jobs()
	insertResponse = jobCollection.insert(projectId=projectId, body=jobData).execute(http=http)
	
	while True:
		status = jobCollection.get(projectId=projectId, jobId=insertResponse['jobReference']['jobId']).execute(http=http)
		currentStatus = status['status']['state']
		
		if 'DONE' == currentStatus:
			print 'Current status: ' + currentStatus
			print time.ctime()
			if status['status'].get('errors'):
				print 'Error: %s' % str (status['status']['errors'][0]['message'])
				logging.error('Query error: %s' % str (status['status']['errors'][0]['message']))
				sys.exit(1)
			return insertResponse

		else:
			print 'Waiting for the query/job to complete...'
			print 'Current status: ' + currentStatus
			print time.ctime()
			time.sleep(10)

#Copy file(s) from google cloud storage to local file path and then delete files from gce.
#If the export is large (>1GB) it is split into multiple files and the destination object name must contain a wildcard *
def copyToLocal(service,destinationBucket, destinationObject, localPath):
	try:
		bucket_name = destinationBucket
		prefix = destinationObject.split("*")
		prefix = prefix[0]
		#list all objects that starts with the destinationObject name
		req = service.objects().list(
			bucket=bucket_name,
			prefix=prefix)
		resp = req.execute()
		
		#for each file (object), download locally and delete file afterwards
		for item in resp['items']:
			object_name = item['name']
			file_name = localPath + object_name
			# Get Metadata
			req = service.objects().get(
				bucket=bucket_name,
				object=object_name,
				fields='bucket,name')
			resp = req.execute()

			# Get Payload Data
			req = service.objects().get_media(
				bucket=bucket_name,
				object=object_name)    # optional
			# The BytesIO object may be replaced with any io.Base instance.
			fh = io.FileIO(file_name, mode='wb')
			downloader = http.MediaIoBaseDownload(fh, req, chunksize=1024*1024)
			done = False
			while not done:
				status, done = downloader.next_chunk()
				if status:
					print 'Download %d%%.' % int(status.progress() * 100)
			print 'Download complete! Delete object from cloudstorage'
			service.objects().delete(bucket=bucket_name, object=object_name).execute()
	except Exception as err:
		print 'Undefined error: %s' % err
		logging.error('Undefined error: %s' % err)
		sys.exit(1)

def main():
	logfile = './logs/gbq_' + datetime.datetime.today().strftime('%Y%m%d') + '.log'
	logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - row: %(lineno)d - %(message)s',filename=logfile, level=logging.WARNING)
	
	parser = argparse.ArgumentParser(version='1.0',add_help=True, description='Run asyncronous Big Query job and/or download result to local file. This script can run queries, exports and download result files.')
	parser.add_argument('-pi', '--projectId', help='Big Query Project ID. Required')
	parser.add_argument('-kf', '--keyFile', help='Path to key file (p12). Required')
	parser.add_argument('-sa', '--serviceAccountEmail', help='Service account email. Ensure the account has enough priviligies in the project. Required')
	parser.add_argument('-lp', '--localPath', help='Set path to local directory if results should be downloaded locally')
	parser.add_argument('-qu', '--query', help='Query (SQL). Ex. SELECT date, fullvisitorid FROM [76949285.ga_sessions_20140418] LIMIT 10')
	parser.add_argument('-cf', '--configFile', help='Path to config file if properties are set by file. Ex. myconfigfile.cfg')
	parser.add_argument('-da', '--daysAgo', nargs='*', default=[1], type=int, help='Specify number of days ago counted from runtime. Replaces %%1 (%%2,%%3,...) in query, local file, destination object and tableId, ex. "-da 1 7" replaces %%1 and %%2 with 1 and respectively')
	parser.add_argument('-lr', '--allowLargeResults', default=False, help='Allow large results: False (default) or True')
	parser.add_argument('-cd','--createDisposition', choices=('CREATE_IF_NEEDED', 'CREATE_NEVER'), default='CREATE_IF_NEEDED', help='Create destination table. Default: CREATE_IF_NEEDED')
	parser.add_argument('-wd','--writeDisposition', choices=('WRITE_EMPTY', 'WRITE_TRUNCATE','WRITE_APPEND'), default='WRITE_EMPTY', help='Write to destination table. Default: WRITE_EMPTY')
	parser.add_argument('-ds', '--datasetId', help='Destination dataset ID to populate with query results, ex. travels')
	parser.add_argument('-dt', '--tableId', help='Destination table ID to populate with query results, ex. cities')
	parser.add_argument('-db', '--destinationBucket', help='Destination bucket in Google Cloud Storage if downloading large results, ex. my_export')
	parser.add_argument('-do', '--destinationObject', help='Destination object in Google Cloud Storage and the name of local file (if downloaded). If exporting results larger than 1 GB, set allowLargeResults to TRUE and add a * to filename to export to multiple files, ex. paris-*.csv')
	parser.add_argument('-df', '--destinationFormat', choices=('CSV', 'JSON'), default='CSV',help='Destination Format: CSV (default) or JSON')
	args = parser.parse_args()
	
	#Default settings
	defaultConfig = ConfigParser.RawConfigParser()
	defaultConfig.read('gbq.cfg')
	projectId = defaultConfig.get('gbq', 'projectId')
	serviceAccountEmail = defaultConfig.get('gbq', 'serviceAccountEmail')
	keyFile = defaultConfig.get('gbq', 'keyFile')
	
	#Initiate variables, use config file if it is specified, else use submitted command line parameters
	if args.configFile: 
		jobConfig = ConfigParser.RawConfigParser(allow_no_value=True)
		jobConfig.read(args.configFile)
		if jobConfig.get('job', 'projectId'):
			projectId = jobConfig.get('job', 'projectId')
		if jobConfig.get('job', 'serviceAccountEmail'):
			serviceAccountEmail = jobConfig.get('job', 'serviceAccountEmail')
		if jobConfig.get('job', 'keyFile'):
			keyFile = jobConfig.get('job', 'keyFile')
		localPath = jobConfig.get('job', 'localPath')
		query = jobConfig.get('job', 'query')
		daysAgo = map(int, jobConfig.get('job', 'daysAgo').split(' '))
		allowLargeResults = jobConfig.get('job', 'allowLargeResults')
		createDisposition = jobConfig.get('job', 'createDisposition')
		writeDisposition = jobConfig.get('job', 'writeDisposition')
		datasetId= jobConfig.get('job', 'datasetId')
		tableId= jobConfig.get('job', 'tableId')
		destinationBucket = jobConfig.get('job', 'destinationBucket')
		destinationObject = jobConfig.get('job', 'destinationObject')
		destinationFormat = jobConfig.get('job', 'destinationFormat')
	else:
		if args.projectId:
			projectId = args.projectId
		if args.serviceAccountEmail:
			serviceAccountEmail = args.serviceAccountEmail
		if args.keyFile:
			keyFile = args.keyFile
		localPath = args.localPath
		query = args.query
		daysAgo = args.daysAgo
		allowLargeResults = args.allowLargeResults
		createDisposition = args.createDisposition
		writeDisposition = args.writeDisposition
		datasetId = args.datasetId
		tableId = args.tableId
		destinationBucket = args.destinationBucket
		destinationObject = args.destinationObject
		destinationFormat = args.destinationFormat

	#replace days ago placeholders (%1, %2, etc.) with respective date.
	if daysAgo:
		c = 1
		for day in daysAgo:
			today = datetime.datetime.today() - datetime.timedelta(days=int(day))
			rep = "%" + str(c)
			if localPath:
				localPath = localPath.replace(rep, today.strftime('%Y%m%d'))
			if query:
				query = query.replace(rep, today.strftime('%Y%m%d'))
			if destinationObject:
				destinationObject = destinationObject.replace(rep, today.strftime('%Y%m%d'))
			if tableId:
				tableId = tableId.replace(rep, today.strftime('%Y%m%d'))
			c=c+1
	
	try:
		f = file(keyFile, 'rb')
		key = f.read()
		f.close()
		credentials = SignedJwtAssertionCredentials(serviceAccountEmail,key,scope='https://www.googleapis.com/auth/bigquery')
		http = httplib2.Http()
		http = credentials.authorize(http)
		service = build('bigquery', 'v2')
		if query: #run query
			jobData = AsyncQueryBatch(projectId,datasetId,tableId,allowLargeResults,createDisposition,writeDisposition,query)
			insertResponse = runJob(http, service,jobData,projectId)
			if insertResponse['configuration']['query'].get('destinationTable'): #Check if query has destinationTable
				datasetId = insertResponse['configuration']['query']['destinationTable']['datasetId']
				tableId = insertResponse['configuration']['query']['destinationTable']['tableId']
		if all((destinationBucket,destinationObject)): #export result via cloudstorage
			jobData = exportTable(projectId,datasetId, tableId,destinationBucket,destinationObject,destinationFormat)
			insertResponse = runJob(http, service,jobData,projectId)
			if localPath: #download to local file(s)
				credentials = SignedJwtAssertionCredentials(serviceAccountEmail,key,scope='https://www.googleapis.com/auth/devstorage.full_control')
				http = httplib2.Http()
				http = credentials.authorize(http)
				service = discovery.build('storage', 'v1', http=http)
				copyToLocal(service,destinationBucket, destinationObject, localPath)
		logging.warning('Job complete! Query: %s' % query)
	
	except HttpError as err:
		print 'HttpError:', pprint.pprint(err.content)
		logging.error('HttpError: %s' % err)
		sys.exit(1)
		
	except AccessTokenRefreshError:
		print ("Credentials have been revoked or expired, please re-run the application to re-authorize")
		logging.error("Credentials have been revoked or expired, please re-run the application to re-authorize")
		sys.exit(1)
	
	except Exception as err:
		print 'Undefined error: %s' % err
		logging.error('Undefined error: %s' % err)
		sys.exit(1)


if __name__ == '__main__':
  main()
