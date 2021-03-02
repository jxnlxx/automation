#%% architect_pace_request.py

# google sheets bit

import time
import sqlite3
import requests
import pandas as pd
import datetime as dt

import gspread
import gspread_formatting
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery

from getstat import stat_subdomain, stat_key, stat_base_url                                    # saved locally in C:\Users\USERNAME\AppData\Local\Programs\Python\Python37-32\Lib

# definitions

def scrub(table_name):
    return table_name.lower().replace(' ', '_').replace('(','').replace(')','').replace(',','')

# load queries list

gspread_id = '1ckZh9TSaSp1Ucu2HKIA7TELPdd24oylFDXVEx6dtdSw'
gsheet_name = 'Client List'

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
google_auth = r'C:\Users\JLee35\OneDrive - Dentsu Aegis Network\PROJECTS\Python\APIs\keys\iprospectseonorth\google_auth.json'
creds = ServiceAccountCredentials.from_json_keyfile_name(google_auth, scope)
client = gspread.authorize(creds)

sheet = client.open_by_key(gspread_id)
worksheet = sheet.worksheet(gsheet_name)

# load data to dataframe from gsheet
print('Retrieving client list from Google Sheets...')
client_list = get_as_dataframe(sheet.worksheet(gsheet_name), parse_dates=True)# usecols=range(0, NUMBERCOLS))

# loading gsheets automatically loads 25 cols x 1k rows, so we trim it:
client_list = client_list.loc[:,~client_list.columns.str.contains('unnamed', case=False)] # remove columns containing 'unnamed' in label
client_list = client_list.dropna(axis=0, how='all') # remove rows that are empty
client_list['STAT ID'] = client_list['STAT ID'].astype(int) # ensure that STAT ID is not a float
client_list['STAT ID'] = client_list['STAT ID'].astype(str) # ensure that STAT ID is not a float
print('Done!')

#%%

client_name = 'Simply Be'
client_list = client_list[client_list['Client Name'] == client_name]

#%% request exports for all clients


for i in client_list.index:
    con = sqlite3.connect(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\architect_pace_normalised.db')
    cur = con.cursor()
    client_name = client_list['Client Name'][i]
    save_name = scrub(client_name)
    stat_id = client_list['STAT ID'][i]

    cur.execute(f'''CREATE TABLE IF NOT EXISTS requests_{save_name}(
        Date TEXT NOT NULL,
        JobId INTEGER NOT NULL,
        Status INTEGER NOT NULL,
        UNIQUE (Date)
        );''')

    con.commit()

#   get date of next request
    requests_df = pd.read_sql_query(f'SELECT * FROM requests_{save_name}', con)
    try:
        start_date = requests_df['Date'].iloc[-1] # get last value in requests_df['date'] as start_date
        start_date = dt.datetime.strptime(start_date, '%Y-%m-%d').date() #  convert start_date string to date object
        start_date = start_date + dt.timedelta(days=1) # add 1 day to start_date
    except KeyError:
        sites_all_url = f'{stat_base_url}/sites/all?&results=5000&format=json'
        print('\n'+'Requesting data...')
        response = requests.get(sites_all_url)
        response = response.json()
        print('\n'+'Data received!')
        total_results = response.get('Response').get('totalresults')
        print('\n'+'Processing..')
        start_date = response.get('Response').get('Result')
        start_date = pd.DataFrame(start_date)
        start_date = start_date.loc[start_date['Id']==stat_id, 'CreatedAt'].iloc[0]
        start_date = dt.datetime.strptime(start_date, '%Y-%m-%d').date()
        start_date = start_date + dt.timedelta(days=5)

    #       see if start date is before 2 years ago
        cutoff = (dt.date.today() - dt.timedelta(days = 365)).replace(day=1)
        if start_date < cutoff:
            start_date = cutoff
        else:
            pass

    #   ensure that start date is before end date
    end_date = dt.date.today() - dt.timedelta(days = 2)
    if start_date < end_date:
        print('Requesting ranks...')
        pass
    else:
        print('Date aready requested! \nMoving onto next client.')
        continue

#   request reports for between 'start_date' and 'end_date'
    n= 1
    for j in pd.date_range(start_date, end_date):
        date = str(j.strftime('%Y-%m-%d'))
        print(f'{n:03d} Requesting {date} rank report for {client_name}')
        url = f'{stat_base_url}/bulk/ranks?date={date}&site_id={stat_id}&engines=google&format=json'
        response = requests.get(url)
        response = response.json()
        job_id = response.get('Response').get('Result').get('Id')
        status = 0
        cur.execute(f'''INSERT OR IGNORE INTO requests_{save_name}(
            Date, JobId, Status)
        VALUES (?, ?, ?)''',
        (date, job_id, status)
        )
        n += 1

    con.commit()
    con.close()
print('DURN!')

#%%
