# Big-Query
Python scripts to export web analytics data from Google Big Query. Scripts can be executed from SSIS or as a scheduled task.

#Service account authentication
Set up a service account in your big query project. See https://cloud.google.com/bigquery/authorization#service-accounts
Download p12 key file and store it on the server (in the same folder as the script).

#Install python and required python packages on the server that runs the script
1.	Install Python 2.7.8 (add python to PATH) https://www.python.org/download/
2.	Install PIP 1.5.6 (add to PATH) https://pypi.python.org/pypi/pip
3.	pip install --upgrade httplib2
4.	pip install --upgrade google-api-python-client
5.  pip install --upgrade oauth2client
6.  pip install --upgrade pyopenssl

#The script perform 3 major tasks that can be combined by setting the right parameters:
1. Run a query and save the result to a temporary or permanent (requires datasetId and tableId) table in Big Query.
2. Export the result to Google Cloud Storage (GCS) (requires destinationBucket and destinationObject)
3. Copy result files from GCS to a local directory (requires localPath) on the server running the script and thereafter delete files on GCS.

#Command line parameters:
The script is run by calling *gbq.py* and accept the following parameters:
- **-pi** or **--projectId** Big Query Project ID.
- **-kf** or **--keyFile** Path to key file (p12).
- **-sa** or **--serviceAccountEmail** Service account email. Ensure the account has enough priviligies in the project.
- **-lp** or **--localPath** Set path to local directory if results should be downloaded locally
- **-qu** or **--query** Query (SQL). *Ex. SELECT date, fullvisitorid FROM [76949285.ga_sessions_20140418] LIMIT 10*
- **-cf** or **--configFile** Path to a job specific config file if properties are set by file. *Ex. /sessionexport/sessionexportjob.cfg*
- **-da** or **--daysAgo** Specify number of days ago counted from runtime. Replaces %%1 (%%2,%%3,...) in query, local file, destination object and tableId. *Ex. "-da 1 7" replaces %1 and %2 with 1 and respectively*
- **-lr** or **--allowLargeResults** Allow large results. Options: False (default) or True
- **-cd** or **--createDisposition** Create destination table. Options: CREATE_IF_NEEDED (default), CREATE_NEVER
- **-wd** or **--writeDisposition** Write to destination table. Options: WRITE_EMPTY (default), WRITE_TRUNCATE, WRITE_APPEND
- **-ds** or **--datasetId** Destination dataset ID to populate with query results. *Ex. session_export*
- **-dt** or **--tableId** Destination table ID to populate with query results. *Ex. cities*
- **-db** or **--destinationBucket** Destination bucket in Google Cloud Storage if downloading large results. *Ex. my_export*
- **-do** or **--destinationObject** Destination object in Google Cloud Storage and the name of local file (if downloaded). If exporting results larger than 1 GB, set allowLargeResults to TRUE and add a wildcard (\*) to destinationObject to export to multiple files. *Ex. paris-*\**.csv*
- **-df** or **--destinationFormat** Destination Format. Options: CSV (default), JSON

#Examples:
- Query with default settings: 
  - *gbq.py -qu "SELECT date, fullvisitorid, visitnumber FROM [76949285.ga_sessions_20140418] LIMIT 10;"*

- Query with job specific credential parameters: 
  - *gbq.py -pi 123456789 -sa "123456789-4dob09dg1edgrguke5erlte0vub6ljtr@developer.gserviceaccount.com" -kf tcne.p12 -qu "SELECT date, fullvisitorid, visitnumber FROM [76949285.ga_sessions_20140418] LIMIT 10;"*

- Query, export and download results from cloud storage to local file : 
  - *gbq.py -lp "./test/" -db tcne_temp -do qedrl_%1.csv -qu "SELECT date, fullvisitorid, visitnumber FROM [76949285.ga_sessions_20140418] LIMIT 10;"*

- Large query with destinationTable, export and download results from cloud storage to local file(s): 
  - *gbq.py -lp "./test/"  -db tcne_temp -do testtolocal_%1-*\**.csv -lr True -ds mydataset -dt mydatatable -wd WRITE_TRUNCATE -qu "SELECT * FROM [1_web_data.Travel] where calendardate >= '20150202' and calendardate <= '20150208';"*

- Run Query with config file: 
  - *gbq.py -cf ./session_export/session_export.cfg*

#Configure default settings for the script.
The default settings can be overriden by runtime parameters or job specific config file. Default settings are set in gbq.cfg. Parameters that can be set
1.  projectId
2.  serviceAccountEmail
3.  keyFile

