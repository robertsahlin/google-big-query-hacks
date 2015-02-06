# Big-Query
Python scripts to export web analytics data from Google Big Query. Scripts can be executed from SSIS or as a scheduled task.

#Service account authentication
Set up a service account in your big query project. See https://cloud.google.com/bigquery/authorization#service-accounts
Download p12 key file and store it on the server, preferable in the same folder as the script.

#Install python and required python packages on the server that runs the script
1.	Install Python 2.7.8 (add python to PATH) https://www.python.org/download/
2.	Install PIP 1.5.6 (add to PATH) https://pypi.python.org/pypi/pip
3.	pip install --upgrade httplib2
4.	pip install --upgrade google-api-python-client
5.  pip install --upgrade oauth2client
6.  pip install --upgrade pyopenssl

#The script perform 3 major tasks that can be combined by setting the right parameters:
1. Run a query and save the result to a temporary or permanent (requires datasetId and tableId) table in Big Query.
2. Export result to Google Cloud Storage (requires destinationBucket and destinationObject)
3. Copy result files from Google Cloud Storage to local path (requires localPath) on server and thereafter delete files on GCE.

If the resulting file is large (> 1GB) you need to allow large results (requires allowLargeResults) and give the destinationObject a name containing a wildcard * to let Big Query export the result to multiple files. 
