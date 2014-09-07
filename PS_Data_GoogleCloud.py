#!/usr/bin/env python

import httplib2
import pprint
import sys
import time

from apiclient.discovery import build
from apiclient.errors import HttpError
from apiclient.http import MediaFileUpload
from oauth2client.client import SignedJwtAssertionCredentials
from json import dumps as json_dumps

import logging
logging.basicConfig() #included to avoid message when oauth2client tries to write to log

#some of this code comes from the following link: https://developers.google.com/bigquery/bigquery-api-quickstart
# to build the service object follow setps in the service account section here: https://developers.google.com/bigquery/docs/authorization#service-accounts
# for more on the API... https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/python/latest/

# Number of bytes to send/receive in each request.
CHUNKSIZE = 2 * 1024 * 1024
# Mimetype to use if one can't be guessed from the file extension.
DEFAULT_MIMETYPE = 'application/octet-stream'


def queryTable(service, projectId,query):
    """ Run a query against Google BigQuery.  Returns a list with the results.
    
    Args:
        service: string, BigQuery service object that is authenticated.  Example: service = build('bigquery','v2', http=http)
        projectId: string, Name of google project which you want to query.
        query: string, Query to excecute on BigQuery.  Example: 'Select max(Date) from dataset.table'
    
    Returns:
        A list with the query results (excluding column names)
     """
    jobCollection = service.jobs()

    try:
        queryData = {"query": query}
        queryResult = jobCollection.query(projectId=projectId,body=queryData).execute()

        resultList=[]
        for row in queryResult['rows']:
            resultRow=[]
            for field in row['f']:
                resultRow.append(field['v'])
            resultList.append(resultRow)

        return resultList

    except HttpError as err:
        print 'Error:', pprint.pprint(err.content)


    except AccessTokenRefreshError:
        print ("Credentials have been revoked or expired, please re-run"
               "the application to re-authorize")


def uploadToCloudStorage(service, projectId, bucket, sourceFile,destFile):
    """Upload a local file to a Cloud Storage bucket.

    Args:
        service: string, BigQuery service object that is authenticated.  Example: service = build('bigquery','v2', http=http)
        projectId: string, Name of Google project to upload to
        bucket: string, Name of Cloud Storage bucket (exclude the "gs://" prefix)
        sourceFile: string, Path to the local file to upload
        destFile: string, Name to give the file on Cloud Storage

    Returns:
        Response of the upload in a JSON format
    """
    #Starting code for this function is a combination from these sources:
    #   https://code.google.com/p/google-cloud-platform-samples/source/browse/file-transfer-json/chunked_transfer.py?repo=storage
    #   https://developers.google.com/api-client-library/python/guide/media_upload

    filename = sourceFile
    bucket_name = bucket
    object_name = destFile
    assert bucket_name and object_name

    print 'Building upload request...'
    media = MediaFileUpload(filename, chunksize=CHUNKSIZE, resumable=True)
    if not media.mimetype():
        media = MediaFileUpload(filename, DEFAULT_MIMETYPE, resumable=True)
    request = service.objects().insert(bucket=bucket_name, name=object_name,
                                     media_body=media)

    response = request.execute()

    return response

##delete table from Big Query
def deleteTable(service, projectId,datasetId,tableId):
    """Delete a BigQuery table.

    Args:
        service: string, BigQuery service object that is authenticated.  Example: service = build('bigquery','v2', http=http)
        projectId: string, Name of Google project table resides in
        datasetId: string, Name of dataset table resides in
        tableId: string, Name of table to delete (make sure you get this one right!)

    Returns:
        Response from BigQuery in a JSON format
    """
    tablesObject = service.tables()
    req = tablesObject.delete(projectId=projectId,datasetId=datasetId,tableId=tableId)
    result = req.execute()
    
    return result


def jobStatusLoop(projectId, jobCollection, insertResponse,waitTimeSecs=10):
    """Monitors BigQuery job and prints out status until the job is complete.

    Args:
        projectId: string, Name of Google project table resides in
        jobCollection: jobs() object, Name of jobs() object that called the job insert
        insertResponse: JSON object, The JSON object returned when calling method jobs().insert().execute()
        waitTimeSecs: integer, Number of seconds to wait between checking job status

    Returns:
        Nothing
    """
    while True:
        job = jobCollection.get(projectId=projectId,
                                 jobId=insertResponse['jobReference']['jobId']).execute()
     

        if 'DONE' == job['status']['state']:
            print 'Done Loading!'
            if 'errorResult' in job['status']:
                print 'Error loading table: ', pprint.pprint(job)
            return

        print 'Waiting for loading to complete...'
        time.sleep(waitTimeSecs)

        if 'errorResult' in job['status']:
            print 'Error loading table: ', pprint.pprint(job)
            return


def listDatasets(service, projectId):
    """Lists BigQuery datasets.

    Args:
        service: string, BigQuery service object that is authenticated.  Example: service = build('bigquery','v2', http=http)
        projectId: string, Name of Google project

    Returns:
        List containing dataset names
    """
    datasets = service.datasets()
    response = datasets.list(projectId=PROJECT_NUMBER).execute()

    datasetList = []
    for field in response['datasets']:
        datasetList.append(field['datasetReference']['datasetId'])

    return datasetList


def loadTableFromFile(service, projectId, datasetId, targetTableId, sourceCSV,schema,delimiter='|',skipLeadingRows=0):
    """[UNDER CONSTRUCTION - may not work properly] Loads a table in BigQuery from a CSV file.

    Args:
        service: string, BigQuery service object that is authenticated.  Example: service = build('bigquery','v2', http=http)
        projectId: string, Name of Google project
        datasetId: string, Name of dataset table resides in
        targetTableId: string, Name of table to create or append data to
        sourceCSV: string, Path of the file to load
        schema: JSON, Schema of the file to be loaded
        delimiter: string, Column delimiter for file, default is | (optional)
        skipLeadingRows: integer, Number of rows to skip, default is 0 (optional)

    Returns:
        Returns job response object.  Prints out job status every 10 seconds.
    """
    
    # example schema setup:
    # schema = {
    #             'fields': [
    #             {
    #               'name': 'ID',
    #               'type': 'INTEGER'
    #             },
    #             {
    #               'name': 'Day',
    #               'type': 'TIMESTAMP'
    #             {
    #               'name': 'Description',
    #               'type': 'STRING'
    #             },
    #             {
    #               'name': 'ViewTimeInMinutes',
    #               'type': 'FLOAT'
    #             },
    #             {
    #               'name': 'LoadDate',
    #               'type': 'TIMESTAMP'
    #             }
    #           ]
    #         }

    jobCollection = service.jobs()
        
    jobData = {
        'projectId': projectId,
        'configuration': {
            'load': {
              'sourceUris': [sourceCSV],
              'fieldDelimiter': delimiter,
              'schema': schema,
            'destinationTable': {
              'projectId': projectId,
              'datasetId': datasetId,
              'tableId': targetTableId
            },
            'skipLeadingRows': skipLeadingRows
          }
        }
      }

    insertResponse = jobCollection.insert(projectId=projectId, body=jobData).execute()
    jobStatusLoop(projectId,jobCollection,insertResponse)

    return insertResponse

#create and run job
# def loadPartition_SampleTable(service, projectId, datasetId, targetTableId, sourceQuery):
    
#     jobCollection = service.jobs()
    
#     jobData = {
#         'projectId': projectId,
#         'configuration': {
#             'query': {
#                 'flattenResults': 'True', # [Experimental] Flattens all nested and repeated fields in the query results. The default value is true. allowLargeResults must be true if this is set to false.
#                 'allowLargerResults': 'True',
#                 'destinationTable': {
#                     'projectId': projectId,
#                     'datasetId': datasetId,
#                     'tableId': targetTableId,
#                 },
#                 'priority': 'BATCH',
#                 'writeDisposition': 'WRITE_APPEND', # [Optional] Specifies the action that occurs if the destination table already exists. The following values are supported: WRITE_TRUNCATE: If the table already exists, BigQuery overwrites the table data. WRITE_APPEND: If the table already exists, BigQuery appends the data to the table. WRITE_EMPTY: If the table already exists and contains data, a 'duplicate' error is returned in the job result. The default value is WRITE_EMPTY. Each action is atomic and only occurs if BigQuery is able to complete the job successfully. Creation, truncation and append actions occur as one atomic update upon job completion.
#                 'createDisposition': 'CREATE_IF_NEEDED', # [Optional] Specifies whether the job is allowed to create new tables. The following values are supported: CREATE_IF_NEEDED: If the table does not exist, BigQuery creates the table. CREATE_NEVER: The table must already exist. If it does not, a 'notFound' error is returned in the job result. The default value is CREATE_IF_NEEDED. Creation, truncation and append actions occur as one atomic update upon job completion.
#                 'query':sourceQuery,    
#             },
#         },
#     }

#     insertResponse = jobCollection.insert(projectId=projectId, body=jobData).execute()
#     print insertResponse
#     jobStatusLoop(projectId,jobCollection,insertResponse)
