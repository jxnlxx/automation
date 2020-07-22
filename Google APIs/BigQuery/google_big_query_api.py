# -*- coding: utf-8 -*-
"""
Created on Wed Jul  1 10:04:37 2020

@author: JLee35
"""


import os
import time
import json
import datetime as dt
import requests
import openpyxl
import pandas as pd
import xlsxwriter as xl
import pandas_gbq

from uuid import uuid4
from calendar import monthrange
from xlsxwriter.utility import xl_range
from urllib.parse import urlparse

import gspread
import gspread_formatting
from gspread_dataframe import get_as_dataframe, set_with_dataframe


from google.cloud import bigquery
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery


# =============================================================================
# Definitions
# =============================================================================

def create_dataset(dataset_id):

    # [START bigquery_create_dataset]
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # TODO(developer): Set dataset_id to the ID of the dataset to create.
    dataset_id = '{}.{}'.format(client.project, dataset_id)

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # TODO(developer): Specify the geographic location where the dataset should reside.
    dataset.location = 'EU'
    
    # Send the dataset to the API for creation.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = client.create_dataset(dataset)  # Make an API request.
    print('Created dataset {}.{}'.format(client.project, dataset.dataset_id))
    # [END bigquery_create_dataset]
    
def create_table(dataset_id, table_id):

    # [START bigquery_create_table]
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = '{}.{}.{}'.format(client.project, dataset_id, table_id)
    #table_id = 'your-project.your_dataset.your_table_name'
    schema = [
        bigquery.SchemaField('keyword_id', 'INTEGER', mode="REQUIRED"),
        bigquery.SchemaField('keyword', 'STRING', mode="REQUIRED"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )
    # [END bigquery_create_table]


def create_table(dataset_id, table_id):

    # [START bigquery_create_table]
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    try:
        # TODO(developer): Set dataset_id to the ID of the dataset to create.
        dataset_id = '{}.{}'.format(client.project, dataset_id)

        # Construct a full Dataset object to send to the API.
        dataset = bigquery.Dataset(dataset_id)
    
        # TODO(developer): Specify the geographic location where the dataset should reside.
        dataset.location = 'EU'
        
        # Send the dataset to the API for creation.
        # Raises google.api_core.exceptions.Conflict if the Dataset already
        # exists within the project.
        dataset = client.create_dataset(dataset)  # Make an API request.
        # TODO(developer): Set table_id to the ID of the table to create.

    except:
        pass
    
    try:
        table_id = '{}.{}.{}'.format(client.project, dataset_id, table_id)
    
        #table_id = 'your-project.your_dataset.your_table_name'
        schema = [
            bigquery.SchemaField('keyword_id', 'INTEGER', mode="REQUIRED"),
            bigquery.SchemaField('keyword', 'STRING', mode="REQUIRED"),
        ]
    
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)  # Make an API request.
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )
    except:
        pass
    # [END bigquery_create_table]
    
    
# =============================================================================
# START
# =============================================================================

start_time = dt.datetime.now().replace(microsecond=0)
request_counter = 0 # stat requests counter

# =============================================================================
# SETTINGS
# =============================================================================

key_path = r'C:\Users\JLee35\OneDrive - Dentsu Aegis Network\CURRENT PROJECTS\Python\APIs\keys\jon.lee\bigquery_auth.json'

# =============================================================================
# SCRIPT
# =============================================================================

client_secret = r'C:\Users\JLee35\OneDrive - Dentsu Aegis Network\CURRENT PROJECTS\Python\APIs\keys\jon.lee\bigquery_auth.json'
credentials = service_account.Credentials.from_service_account_file(
    client_secret, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

client = bigquery.Client(credentials=credentials, project=credentials.project_id)


# =============================================================================
# 
# =============================================================================


dataset_id = 'test_project'

df = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\Client Ranks\Google Base Rank\www.fragrancedirect.co.uk_google_base_rank.csv')
table_id = 'www_fragrancedirect_co_uk_google_base_rank'  
client = bigquery.Client(credentials=credentials, project=credentials.project_id)
table_id = '{}.{}'.format(dataset_id, table_id,)
df.to_gbq(table_id, project_id=client.project, table_schema =[{'name':'regional_search_volume', 'type':'INTEGER', 'mode': 'REQUIRED'}])
# Since string columns use the "object" dtype, pass in a (partial) schema
# to ensure the correct BigQuery data type.
job_config = bigquery.LoadJobConfig(schema=[bigquery.SchemaField("my_string", "STRING"),])

job = client.load_table_from_dataframe(df, project_id=client.project)
#, job_config=job_config
)

# Wait for the load job to complete.
job.result()

# =============================================================================
# END TIMER
# =============================================================================

end_time = dt.datetime.now().replace(microsecond=0)
time_elapsed = end_time - start_time

print('\n'+'DURN!')
print('\n'+f'Time elapsed: {time_elapsed}'
      '\n'+f'Requests made: {request_counter}')