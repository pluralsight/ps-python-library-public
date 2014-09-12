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

# some of this code built on this project: https://code.google.com/p/google-bigquery-tools/source/browse/samples/python/appengine-bq-join
# some of this code comes from the following link: https://developers.google.com/bigquery/bigquery-api-quickstart
# to build the service object follow setps in the service account section here: https://developers.google.com/bigquery/docs/authorization#service-accounts
# for more on the API... https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/python/latest/

# Number of bytes to send/receive in each request.
CHUNKSIZE = 2 * 1024 * 1024
# Mimetype to use if one can't be guessed from the file extension.
DEFAULT_MIMETYPE = 'application/octet-stream'


def query_table(service, project_id,query):
    """ Run a query against Google BigQuery.  Returns a list with the results.
    
    Args:
        service: string, BigQuery service object that is authenticated.  Example: service = build('bigquery','v2', http=http)
        project_id: string, Name of google project which you want to query.
        query: string, Query to excecute on BigQuery.  Example: 'Select max(Date) from dataset.table'
    
    Returns:
        A list with the query results (excluding column names)
     """
    jobCollection = service.jobs()

    try:
        query_body = {"query": query}
        query_result = jobCollection.query(projectId=project_id,body=query_body).execute()

        result_list=[]
        for row in query_result['rows']:
            result_row=[]
            for field in row['f']:
                result_row.append(field['v'])
            result_list.append(result_row)

        return result_list

    except HttpError as err:
        print 'Error:', pprint.pprint(err.content)

    except AccessTokenRefreshError:
        print ("Credentials have been revoked or expired, please re-run"
               "the application to re-authorize")


def cloudstorage_upload(service, project_id, bucket, source_file,dest_file):
    """Upload a local file to a Cloud Storage bucket.

    Args:
        service: string, BigQuery service object that is authenticated.  Example: service = build('bigquery','v2', http=http)
        project_id: string, Name of Google project to upload to
        bucket: string, Name of Cloud Storage bucket (exclude the "gs://" prefix)
        source_file: string, Path to the local file to upload
        dest_file: string, Name to give the file on Cloud Storage

    Returns:
        Response of the upload in a JSON format
    """
    #Starting code for this function is a combination from these sources:
    #   https://code.google.com/p/google-cloud-platform-samples/source/browse/file-transfer-json/chunked_transfer.py?repo=storage
    #   https://developers.google.com/api-client-library/python/guide/media_upload
    filename = source_file
    bucket_name = bucket
    object_name = dest_file
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
def delete_table(service, project_id,dataset_id,table):
    """Delete a BigQuery table.

    Args:
        service: string, BigQuery service object that is authenticated.  Example: service = build('bigquery','v2', http=http)
        project_id: string, Name of Google project table resides in
        dataset_id: string, Name of dataset table resides in
        table: string, Name of table to delete (make sure you get this one right!)

    Returns:
        Response from BigQuery in a JSON format
    """
    tables_object = service.tables()
    req = tables_object.delete(projectId=project_id,datasetId=dataset_id,tableId=table)
    result = req.execute()
    
    return result


def job_status_loop(project_id, jobCollection, insertResponse,waitTimeSecs=10):
    """Monitors BigQuery job and prints out status until the job is complete.

    Args:
        project_id: string, Name of Google project table resides in
        jobCollection: jobs() object, Name of jobs() object that called the job insert
        insertResponse: JSON object, The JSON object returned when calling method jobs().insert().execute()
        waitTimeSecs: integer, Number of seconds to wait between checking job status

    Returns:
        Nothing
    """
    while True:
        job = jobCollection.get(projectId=project_id,
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


def list_datasets(service, project_id):
    """Lists BigQuery datasets.

    Args:
        service: string, BigQuery service object that is authenticated.  Example: service = build('bigquery','v2', http=http)
        project_id: string, Name of Google project

    Returns:
        List containing dataset names
    """
    datasets = service.datasets()
    response = datasets.list(projectId=PROJECT_NUMBER).execute()

    dataset_list = []
    for field in response['datasets']:
        dataset_list.append(field['datasetReference']['datasetId'])

    return dataset_list


def load_table_from_file(service, project_id, dataset_id, targettable, sourceCSV,schema,delimiter='|',skipLeadingRows=0):
    """[UNDER CONSTRUCTION - may not work properly] Loads a table in BigQuery from a CSV file.

    Args:
        service: string, BigQuery service object that is authenticated.  Example: service = build('bigquery','v2', http=http)
        project_id: string, Name of Google project
        dataset_id: string, Name of dataset table resides in
        targettable: string, Name of table to create or append data to
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
        'projectId': project_id,
        'configuration': {
            'load': {
              'sourceUris': [sourceCSV],
              'fieldDelimiter': delimiter,
              'schema': schema,
            'destinationTable': {
              'projectId': project_id,
              'datasetId': dataset_id,
              'tableId': targettable
            },
            'skipLeadingRows': skipLeadingRows
          }
        }
      }

    insertResponse = jobCollection.insert(projectId=project_id, body=jobData).execute()
    job_status_loop(project_id,jobCollection,insertResponse)

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
