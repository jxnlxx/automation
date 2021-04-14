#%% architect_pace_add_keywords.py

import os
import time
import sqlite3
import requests
import numpy as np
import pandas as pd
import datetime as dt

import gspread
import gspread_formatting
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery

from getstat import stat_subdomain, stat_key, stat_base_url                                    # saved locally in C:\Users\USERNAME\AppData\Local\Programs\Python\Python37-32\Lib

#   definitions

def scrub(table_name):
    return table_name.replace(' ', '').replace('(','').replace(')','').replace(',','')

def dbize(folder_name):
    return folder_name.lower().replace(" ", "_").replace('(','').replace(')','').replace(',','') + '_ranks.db'

#   load client list

client_list = pd.read_excel(r'C:\Users\JLee35\dentsu\Automation - Documents\General\STAT Ranks - Client List.xlsx')
client_list['STAT ID'] = client_list['STAT ID'].astype(int) # ensure that STAT ID is not a float
client_list['STAT ID'] = client_list['STAT ID'].astype(str) # ensure that STAT ID is str
print('Done!')

#%% filter client list

folder_name = 'Toolstation'
client_list = client_list[client_list['Folder Name'] == folder_name]

#%% create ctr table in db

root = fr'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\STAT\Clients'

count = 0
for i in client_list.index:
    folder_name = client_list['Folder Name'][i]
    client_name = client_list['Client Name'][i]
    stat_id = client_list['STAT ID'][i]

    database_name = dbize(folder_name)
    save_name = scrub(client_name)

    con = sqlite3.connect(os.path.join(root, folder_name, database_name))
    cur = con.cursor()

    print (f'Adding Ctr_{save_name}...')
    cur.execute(f'''CREATE TABLE IF NOT EXISTS Ctr_{save_name}(
        Position INTEGER NOT NULL,
        CTR REAL NOT NULL,
        UNIQUE (Position)
        );''')
    df = pd.read_csv(os.path.join(root,folder_name,"Setup",f'{save_name}_ctr.csv'))

    for j in df.index:
        try:
            Position = int(df['Rank'][j])
        except KeyError:
            Position = int(df['Position'][j])
        CTR = float(df['CTR'][j])
        cur.execute(f'INSERT OR IGNORE INTO Ctr_{save_name}(Position, CTR) VALUES (?, ?)', (Position, CTR,))

    con.commit()
    con.close()

print('DURN!')

# %%
