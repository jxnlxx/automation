# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 12:27:30 2020

@author: JLee35
"""


import os
import time
import json
import datetime
import requests
import openpyxl
import pandas as pd
import xlsxwriter as xl

from uuid import uuid4
from calendar import monthrange
from xlsxwriter.utility import xl_range
from urllib.parse import urlparse

import gspread
import gspread_formatting
from gspread_dataframe import get_as_dataframe, set_with_dataframe

from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery

from getstat import stat_subdomain, stat_key      


all_kws = pd.DataFrame()

for filename in os.listdir(r'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\SERP Trends\Keywords List'):
    if filename.endswith(".csv"):
        df = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\SERP Trends\Keywords List\{filename}')
        clientName = filename.replace('_keywords_list_after_2019-06-01.csv', '')
        clientName = clientName.replace('_', '/')
        df = df.assign(client=clientName)
#        jobs_all = jobs_all.assign(Status='').astype(str)
        all_kws = all_kws.append(df)
        print(clientName,'appended!')        
        continue
    else:
        continue