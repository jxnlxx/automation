# -*- coding: utf-8 -*-
"""
Created on Fri Jun 12 08:30:30 2020

@author: JLee35
"""
#%%
import os
import datetime
import requests
import openpyxl
import json
import pandas as pd
import xlsxwriter as xl
from calendar import monthrange
from xlsxwriter.utility import xl_range
from urllib.parse import urlparse
from collections import defaultdict
from getstat import stat_subdomain, stat_key, stat_base_url                     # saved locally in C:\Users\USERNAME\AppData\Local\Programs\Python\Python37-32\Lib

# =============================================================================
# SETTINGS


# =============================================================================
# SCRIPT

url = f'{base_url}/projects/list?results=1000&format=json'

response = requests.get(url)
response = response.json()
response = response.get('Response').get('Result')

df = pd.DataFrame(response)

df.to_csv(r'L:\Commercial\Operations\Technical SEO\Automation\Data\STAT\stat_projects_list.csv', index = False)

print('\n'+'DURN!')
# %%
