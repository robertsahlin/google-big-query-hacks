# For help run serviceaccount.py -h
# Using days ago flag replaces %1 in localPath path and query with the date (yyyymmdd) corresponding to days ago from current date, useful if running program from task scheduler instead of SSIS
# Examples of running job...
# Query and write results to local file directly: serviceaccount9.py -pi 769868728018 -lp "./test/" -sa "769868728018-4dob09dg1edgrguke5erlte0vub6ljtr@developer.gserviceaccount.com" -kf tcne.p12 -qu "SELECT date, fullvisitorid, visitnumber FROM [32549285.ga_sessions_20140418] LIMIT 10;"
# Query, export and download results from cloud storage to local file : serviceaccount9.py -pi 769868728018 -lp "./test/" -sa "769868728018-4dob09dg1edgrguke5erlte0vub6ljtr@developer.gserviceaccount.com" -kf tcne.p12 -qu "SELECT date, fullvisitorid, visitnumber FROM [32549285.ga_sessions_20140418] LIMIT 10;" -db tcne_temp -do queryexportdownloadresultstolocal_%1.csv
# Query with destinationTable, export and download results from cloud storage to local file : serviceaccount9.py -pi 769868728018 -ds sahlin -dt querydest -lp "./test/" -sa "769868728018-4dob09dg1edgrguke5erlte0vub6ljtr@developer.gserviceaccount.com" -kf tcne.p12 -qu "SELECT date, fullvisitorid, visitnumber FROM [32549285.ga_sessions_20140418] LIMIT 10;" -db tcne_temp -do queryexportdownloadresultstolocal_%1.csv
# Large query with destinationTable, export and download results from cloud storage to local file: serviceaccount9.py -pi 769868728018 -lp "./test/" -sa "769868728018-4dob09dg1edgrguke5erlte0vub6ljtr@developer.gserviceaccount.com" -kf tcne.p12 -qu "SELECT date, brandlabel, count(distinct sessionid) sessions FROM (SELECT [fullvisitorid] as fullvisitorid, concat(string([visitid]), [fullvisitorid]) as sessionid, [hits.page.hostname] as brandlabel, [date] as date FROM [32548779.ga_sessions_%1] GROUP EACH BY sessionid, [hits.hitnumber], [date], [fullvisitorid], [brandlabel])  group by brandlabel, date;" -db tcne_temp -do testtolocal_%1-*.csv -lr True -ds sahlin -dt querydest -wd WRITE_TRUNCATE
# Run Query with config file: serviceaccount9.py -c ./session_export/serviceaccount.cfg

import httplib2
import datetime
import pprint
import argparse
import ConfigParser
import time
import json
import io

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
	print destinationUris
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
	print 'jobCollection ok'
	print 'insertResponse start'
	insertResponse = jobCollection.insert(projectId=projectId, body=jobData).execute(http)
	print 'insertResponse ok'
	
	while True:
		status = jobCollection.get(projectId=projectId, jobId=insertResponse['jobReference']['jobId']).execute(http)
		currentStatus = status['status']['state']

		if 'DONE' == currentStatus:
			print 'Current status: ' + currentStatus
			print time.ctime()
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

def main():
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
	#print args
	
	#Initiate variables, use config file if it is specified, else use submitted command line parameters
	if args.configFile: 
		config = ConfigParser.RawConfigParser(allow_no_value=True)
		config.read(args.configFile)
		projectId = config.get('GBQ', 'projectId')
		localPath = config.get('GBQ', 'localPath')
		serviceAccountEmail = config.get('GBQ', 'serviceAccountEmail')
		keyFile = config.get('GBQ', 'keyFile')
		query = config.get('GBQ', 'query')
		#daysAgo = config.get('GBQ', 'daysAgo')
		daysAgo = map(int, config.get('GBQ', 'daysAgo').split(' '))
		allowLargeResults = config.get('GBQ', 'allowLargeResults')
		createDisposition = config.get('GBQ', 'createDisposition')
		writeDisposition = config.get('GBQ', 'writeDisposition')
		datasetId= config.get('GBQ', 'datasetId')
		tableId= config.get('GBQ', 'tableId')
		destinationBucket = config.get('GBQ', 'destinationBucket')
		destinationObject = config.get('GBQ', 'destinationObject')
		destinationFormat = config.get('GBQ', 'destinationFormat')
	else:
		projectId = args.projectId
		localPath = args.localPath
		serviceAccountEmail = args.serviceAccountEmail
		keyFile = args.keyFile
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
			print 'AsyncQueryBatch start'
			jobData = AsyncQueryBatch(projectId,datasetId,tableId,allowLargeResults,createDisposition,writeDisposition,query)
			insertResponse = runJob(http, service,jobData,projectId)
			datasetId = insertResponse['configuration']['query']['destinationTable']['datasetId']
			tableId = insertResponse['configuration']['query']['destinationTable']['tableId']
			print 'AsyncQueryBatch end'
		if all((destinationBucket,destinationObject)): #export result via cloudstorage
			print 'ExportTable start'
			jobData = exportTable(projectId,datasetId, tableId,destinationBucket,destinationObject,destinationFormat)
			#print jobData
			insertResponse = runJob(http, service,jobData,projectId)
			print 'ExportTable end'
		#if all((localPath,destinationBucket,destinationObject)):
			if localPath:
				print 'CopyToLocal start'
				credentials = SignedJwtAssertionCredentials(serviceAccountEmail,key,scope='https://www.googleapis.com/auth/devstorage.full_control')
				http = httplib2.Http()
				http = credentials.authorize(http)
				service = discovery.build('storage', 'v1', http=http)
				copyToLocal(service,destinationBucket, destinationObject, localPath)
				print 'CopyToLocal end'
	
	except HttpError as err:
		print 'HttpError:', pprint.pprint(err.content)
		
	except AccessTokenRefreshError:
		print ("Credentials have been revoked or expired, please re-run the application to re-authorize")
	
	except Exception as err:
		print 'Undefined error: %s' % err


if __name__ == '__main__':
  main()
