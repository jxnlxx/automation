# -*- coding: utf-8 -*-
"""
Created on Tue Apr 21 09:49:56 2020

@author: JLee35
"""

import gspread
import gspread_formatting
import pandas as pd
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery
from uuid import uuid4



#####
gspread_id = '1H3qkPyGolEbq3pMpHYEWEb0kZkudy6mnbJT90L2kl1k'
gsheet_name = 'STAT Automation'
#file_name = 'STAT API Master'
#NUMBERCOLS = <NUMBER OF COLS TO READ>

#####


scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
google_auth = r'C:\Users\JLee35\OneDrive - Dentsu Aegis Network\CURRENT PROJECTS\Python\APIs\keys\iprospectseonorth\google_auth.json'
creds = ServiceAccountCredentials.from_json_keyfile_name( google_auth, scope )
client = gspread.authorize(creds)
sheet = client.open_by_key(gspread_id)
worksheet = sheet.worksheet(gsheet_name)
# load data to dataframe from gsheet
df = get_as_dataframe(sheet.worksheet(gsheet_name), parse_dates=True)# usecols=range(0, NUMBERCOLS))
print(df)

for i in df.index:
    uuid = uuid4()
    df['ClientId'][i] = uuid

print(df)

#worksheet.clear()

#sheet.add_worksheet(Sheet3)

# clear gsheet ready for adding new data
#sheet.values_clear( '{0}!A:AZ'.format(gsheet_name) )

# save data to gsheet from dataframe
set_with_dataframe(sheet.worksheet(gsheet_name), df)


# Extract and print all of the values
#list_of_hashes = sheet.get_all_records()
#(list_of_hashes)