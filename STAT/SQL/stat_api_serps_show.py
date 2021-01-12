#%% stat_api_serps_show.py

import re
import sqlite3
import pandas as pd
import requests
import datetime as dt
from getstat import stat_subdomain, stat_key, stat_base_url                                  # saved locally in C:\Users\USERNAME\AppData\Local\Programs\Python\Python37-32\Lib


date = '2019-01-14'
#date = dt.datetime.today() - dt.timedelta(days=7)
#date = date.strftime('%Y-%m-%d')

con = sqlite3.connect(r'C:\Users\JLee35\Automation\STAT\SERPS show\serps_show_normalised.db')

cur = con.cursor()

kw_list = pd.read_sql_query('SELECT * FROM keywords', con)

# remove kws CreatedAt after date
kw_list = kw_list.drop(kw_list[kw_list['CreatedAt'] > date ].index)
kw_list = kw_list.reset_index(drop=True)
skipped_kws = []

#remove kws from kw_list already in the database
db_kws = pd.read_sql_query('SELECT * FROM serps_show WHERE Date = ? AND Rank = ?', con, params = (date, 20,))
kw_list = kw_list[~kw_list['Id'].isin(db_kws['KeywordId'])]

#%%
# get keyword data
for i in kw_list.index:
    KeywordId = int(kw_list['Id'][i])
    StatId = kw_list['StatId'][i]
    print(f'{i+1} of {len(kw_list)}')
    print(f'Fetching data for {StatId}')
    try:
        serps_show = fr'/serps/show?keyword_id={StatId}&engine=google&date={date}&format=json'
        url = stat_base_url + serps_show
        response = requests.get(url)
        response = response.json()
        serp = response.get('Response').get('Result')
        serp = pd.json_normalize(serp)
    except AttributeError:
        print(f'No data for {StatId} on {date}')
        skipped_kws = skipped_kws + [date, StatId]
        continue
# remove Rank > 20
    serp['Rank'] = serp['Rank'].astype(int)
    serp = serp.drop(serp[serp['Rank'] > 20].index)

# replace 'None' with '0'
    serp['BaseRank'] = serp['BaseRank'].replace(to_replace=[None], value=0)

# rename ResultTypes.ResultType
    serp = serp.rename(columns={'ResultTypes.ResultType' : 'ResultType'})

    serp['Domain'], serp['Path'] = serp['Url'].str.split('/', 1).str
    serp['Path'] = '/' + serp['Path'].astype(str)

# add to database
    con = sqlite3.connect(r'C:\Users\JLee35\Automation\STAT\SERPS show\serps_show_normalised.db')

    cur = con.cursor()

    print(f'Inserting keyword {StatId} into database')
    for index, row in serp.iterrows():
        Protocol = str(row['Protocol'])
        Domain = str(row['Domain'])
        Path = str(row['Path'])
        ResultType = str(row['ResultType'])
        Rank = int(row['Rank'])
        BaseRank = int(row['BaseRank'])

# Protocol normalisation
        cur.execute('SELECT Id FROM protocols WHERE Protocol = ? LIMIT 1', (Protocol,))
        try:
            ProtocolId = int(cur.fetchone()[0])
        except:
            cur.execute('INSERT OR IGNORE INTO protocols (Protocol) VALUES (?)', (Protocol,))
            cur.execute('SELECT Id FROM protocols WHERE Protocol = ? LIMIT 1', (Protocol,))
            ProtocolId = int(cur.fetchone()[0])

# Domain normalisation
        cur.execute('SELECT Id FROM domains WHERE Domain = ? LIMIT 1', (Domain,))
        try:
            DomainId = int(cur.fetchone()[0])
        except:
            cur.execute('INSERT OR IGNORE INTO domains (Domain) VALUES (?)', (Domain,))
            cur.execute('SELECT Id FROM domains WHERE Domain = ? LIMIT 1', (Domain,))
            DomainId = int(cur.fetchone()[0])

# Path normalisation
        cur.execute('SELECT Id FROM paths WHERE Path = ? LIMIT 1', (Path,))
        try:
            PathId = int(cur.fetchone()[0])
        except:
            cur.execute('INSERT OR IGNORE INTO paths (Path) VALUES (?)', (Path,))
            cur.execute('SELECT Id FROM paths WHERE Path = ? LIMIT 1', (Path,))
            PathId = int(cur.fetchone()[0])

# ResultType normalisation
        cur.execute('SELECT Id FROM result_types WHERE ResultType = ? LIMIT 1', (ResultType,))
        try:
            ResultTypeId = int(cur.fetchone()[0])
        except:
            cur.execute('INSERT OR IGNORE INTO result_types (ResultType) VALUES (?)', (ResultType,))
            cur.execute('SELECT Id FROM result_types WHERE ResultType = ? LIMIT 1', (ResultType,))
            ResultTypeId = int(cur.fetchone()[0])

# INSERT OR IGNORE INTO serps_show (normalised)
        cur.execute('''INSERT OR IGNORE INTO serps_show (KeywordId, Date, ProtocolId, DomainId, PathId, Rank, BaseRank, ResultTypeId)
        VALUES(?,?,?,?,?,?,?,?)''',
        (KeywordId, date, ProtocolId, DomainId, PathId, Rank, BaseRank, ResultTypeId))

        con.commit()

print('DURN!')

# %%