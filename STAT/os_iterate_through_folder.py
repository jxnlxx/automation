# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 10:31:18 2020

@author: JLee35
"""

import sys
import time
import json
import datetime as dt
import requests
import pandas as pd
import os

from calendar import monthrange

import gspread
import gspread_formatting
from gspread_dataframe import get_as_dataframe, set_with_dataframe

from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery

from getstat import stat_subdomain, stat_key, stat_base_url      


for filename in os.listdir(r'L:\Commercial\Operations\Technical SEO\Automation\STAT\SQL\Requests'):
    if filename.endswith(".csv"):
        print(filename)
        df = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\SQL\Requests\{filename}')
        try:
            df = df.drop(columns=['Status'])
        except KeyError:
            pass
        df.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\SQL\Requests\Backup\{filename}',index=False)
