#%% architect_pace_aggregate_from_db.py

import os
import time
import json
import sqlite3
import requests
import openpyxl
import numpy as np
import pandas as pd
import datetime as dt
import xlsxwriter as xl

from getstat import stat_subdomain, stat_key, stat_base_url                     # saved locally in C:\Users\USERNAME\AppData\Local\Programs\Python\Python37-32\Lib

import gspread
import gspread_formatting
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery

# definitions

def scrub(table_name):
    return table_name.lower().replace(' ', '_').replace('(','').replace(')','').replace(',','')

# get client list

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
client_list['STAT ID'] = client_list['STAT ID'].astype(str) # ensure that STAT ID is str (for some reason, 'int' on its own didn't work...)

print('Done!')

#%%

client_name = 'JD Williams'
client_list = client_list[client_list['Client Name'] == client_name]

#%%

for i in client_list.index:
    con = sqlite3.connect(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\architect_pace_normalised.db')
    cur = con.cursor()
    client_name = client_list['Client Name'][i]
    save_name = scrub(client_name)

    print(f'Starting {client_name}...')

    cur.execute(f'''CREATE TABLE IF NOT EXISTS visibility_{save_name}(
    Date TEXT NOT NULL,
    DeviceId INTEGER NOT NULL,
    CategoryId INTEGER NOT NULL,
    CriterionId INTEGER NOT NULL,
    Score INTEGER NOT NULL,
    FOREIGN KEY (DeviceId) REFERENCES devices (Id),
    FOREIGN KEY (CriterionId) REFERENCES criteria (Id),
    FOREIGN KEY (CategoryId) REFERENCES categories (Id)
    );''')

# fetch date from table ranks_{save_name}

    try:
#   get start_date from visibility table
        cur.execute(f'SELECT date FROM visibility_{save_name} ORDER BY date DESC LIMIT 1;')
        start_date = str(cur.fetchone()[0])
        start_date = dt.datetime.strptime(start_date, '%Y-%m-%d').date() #  convert start_date string to date object
        start_date = start_date + dt.timedelta(days=1) # add 1 day to start_date
#   get end_date from requests table
        cur.execute(f'SELECT date FROM requests_{save_name} ORDER BY date DESC LIMIT 1;')
        end_date = str(cur.fetchone()[0])
        end_date = dt.datetime.strptime(end_date, '%Y-%m-%d').date() #  convert start_date string to date object
    except TypeError:
#   get start_date from requests table
        cur.execute(f'SELECT date FROM requests_{save_name} ORDER BY date ASC LIMIT 1;')
        start_date = str(cur.fetchone()[0])
        start_date = dt.datetime.strptime(start_date, '%Y-%m-%d').date() #  convert start_date string to date object
#   get end_date from requests table
        cur.execute(f'SELECT date FROM requests_{save_name} ORDER BY date DESC LIMIT 1;')
        end_date = str(cur.fetchone()[0])
        end_date = dt.datetime.strptime(end_date, '%Y-%m-%d').date() #  convert start_date string to date object

    print(f'Beginning from {start_date}')
    counter = 1
    for j in pd.date_range(start_date, end_date):
        month = j.strftime('%b')
        date = str(j.strftime('%Y-%m-%d'))

        print(f'\n{counter} of {len(pd.date_range(start_date, end_date))} - {client_name}')
        counter += 1

        print(f'Starting {date}...')

#################### REMOVE
        keywords = pd.read_sql('SELECT * FROM keywords', con=con)
        categories = pd.read_sql('SELECT * FROM categories', con=con)
        devices = pd.read_sql('SELECT * FROM devices', con=con)
        ctr = pd.read_sql(f'select * FROM ctr_{save_name}', con=con)
####################

        sql = f'''
            SELECT ranks_{save_name}.Date, keywords.Keyword, keywords.TargetedSearchVolume, ranks_{save_name}.GoogleBaseRank as Rank, devices.Device, categories.Category, ctr_{save_name}.CTR
            FROM ranks_{save_name}
            JOIN keywords ON ranks_{save_name}.KeywordId = keywords.Id
            JOIN devices ON keywords.DeviceId = devices.Id
            JOIN categories ON keywords.CategoryId = categories.Id
            JOIN ctr_{save_name} ON ranks_{save_name}.GoogleBaseRank = ctr_{save_name}.Position
            WHERE date = :date;
            '''
        params = {'date':date}
        df = pd.read_sql(sql, params=params, con=con)
        print('Data retrieved!')
        df.to_csv(fr'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\df.csv', index=False)

#   aggreagate

        print('Aggregating data...')
        df['DailySearchVolume'] = (df['TargetedSearchVolume'].astype(int)/30).astype(int)
        df['WeightedRank'] = (df['DailySearchVolume'].astype(int) * df['Rank'].astype(int)).apply(np.floor).astype(int)
        df['CtrScore'] = round(df['CTR'] * df['DailySearchVolume'],0).apply(np.floor).astype(int)

        visibility = pd.DataFrame(columns=['Date', 'Device', 'Criteria', 'Category', 'Score'])

        devices = list(dict.fromkeys(df['Device'].to_list()))
        categories = list(dict.fromkeys(df['Category'].to_list()))
        rank_brackets = ['#1', '#2 - #5', '#6 - #10', '#11 - #20', '#21 - #30', '#31 - #40', '#41 - #50']

        for i in devices:
            device = df[df['Device'] == i]
#            device.to_csv(fr'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\{i}.csv', index=False)
            k = 'All Keywords'

            awr = round(device['WeightedRank'].sum() / device['DailySearchVolume'].sum(),0)
            temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':'Average Weighted Rank','Category':k,'Score':awr},index=[0])
            visibility = visibility.append(temp)

            vis = device['CtrScore'].sum()
            temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':'Visibility Score','Category':k,'Score':vis},index=[0])
            visibility = visibility.append(temp)

            for j in rank_brackets:
                if j == '#1':
                    score = device[device['Rank']==1].shape[0]
                    temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category':k,'Score':score},index=[0])
                    visibility = visibility.append(temp)
                elif j == '#2 - #5':
                    score = device[(device['Rank'] >= 2) & (device['Rank'] <= 5)].shape[0]
                    temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category':k,'Score':score},index=[0])
                    visibility = visibility.append(temp)
                elif j == '#6 - #10':
                    score = device[(device['Rank'] >= 6) & (device['Rank'] <= 10)].shape[0]
                    temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category':k,'Score':score},index=[0])
                    visibility = visibility.append(temp)
                elif j == '#11 - #20':
                    score = device[(device['Rank'] >= 11) & (device['Rank'] <= 20)].shape[0]
                    temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category':k,'Score':score},index=[0])
                    visibility = visibility.append(temp)
                elif j == '#21 - #30':
                    score = device[(device['Rank'] >= 21) & (device['Rank'] <= 30)].shape[0]
                    temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category':k,'Score':score},index=[0])
                    visibility = visibility.append(temp)
                elif j == '#31 - #40':
                    score = device[(device['Rank'] >= 31) & (device['Rank'] <= 40)].shape[0]
                    temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category':k,'Score':score},index=[0])
                    visibility = visibility.append(temp)
                elif j == '#41 - #50':
                    score = device[(device['Rank'] >= 41) & (device['Rank'] <= 50)].shape[0]
                    temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category':k,'Score':score},index=[0])
                    visibility = visibility.append(temp)

            for k in categories:
                category = device[device['Category'] == k]
#                category.to_csv(fr'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\{k}.csv', index=False)

                awr = round(category['WeightedRank'].sum() / category['DailySearchVolume'].sum(),0)
                temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':'Average Weighted Rank','Category':k,'Score':awr},index=[0])
                visibility = visibility.append(temp)

                vis = category['CtrScore'].sum()
                temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':'Visibility Score','Category':k,'Score':vis},index=[0])
                visibility = visibility.append(temp)

                for j in rank_brackets:
                    if j == '#1':
                        score = category[category['Rank']==1].shape[0]
                        temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category':k,'Score':score},index=[0])
                        visibility = visibility.append(temp)
                    elif j == '#2 - #5':
                        score = category[(category['Rank'] >= 2) & (category['Rank'] <= 5)].shape[0]
                        temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category':k,'Score':score},index=[0])
                        visibility = visibility.append(temp)
                    elif j == '#6 - #10':
                        score = category[(category['Rank'] >= 6) & (category['Rank'] <= 10)].shape[0]
                        temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category':k,'Score':score},index=[0])
                        visibility = visibility.append(temp)
                    elif j == '#11 - #20':
                        score = category[(category['Rank'] >= 11) & (category['Rank'] <= 20)].shape[0]
                        temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category':k,'Score':score},index=[0])
                        visibility = visibility.append(temp)
                    elif j == '#21 - #30':
                        score = category[(category['Rank'] >= 21) & (category['Rank'] <= 30)].shape[0]
                        temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category':k,'Score':score},index=[0])
                        visibility = visibility.append(temp)
                    elif j == '#31 - #40':
                        score = category[(category['Rank'] >= 31) & (category['Rank'] <= 40)].shape[0]
                        temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category':k,'Score':score},index=[0])
                        visibility = visibility.append(temp)
                    elif j == '#41 - #50':
                        score = category[(category['Rank'] >= 41) & (category['Rank'] <= 50)].shape[0]
                        temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category':k,'Score':score},index=[0])
                        visibility = visibility.append(temp)

        visibility['Score'] = visibility['Score'].astype(int)
        visibility = visibility.reset_index(drop=True)
#        visibility.to_csv(fr'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\{date}.csv',index=False)
        print(f'Adding to database...')
        for i in visibility.index:
            device  = str(visibility['Device'][i])
            criterion  = str(visibility['Criteria'][i])
            category  = str(visibility['Category'][i])
            score  = int(visibility['Score'][i])

    #   device
            cur.execute('SELECT Id FROM devices WHERE Device = ? LIMIT 1', (device,))
            try:
                DeviceId = int(cur.fetchone()[0])
            except:
                cur.execute('INSERT OR IGNORE INTO devices (Device) VALUES (?)', (device,))
                cur.execute('SELECT Id FROM devices WHERE Device = ? LIMIT 1', (device,))
                DeviceId = int(cur.fetchone()[0])

    #   criterion
            cur.execute('SELECT Id FROM criteria WHERE Criterion = ? LIMIT 1', (criterion,))
            try:
                CriterionId = int(cur.fetchone()[0])
            except:
                cur.execute('INSERT OR IGNORE INTO criteria (Criterion) VALUES (?)', (criterion,))
                cur.execute('SELECT Id FROM criteria WHERE Criterion = ? LIMIT 1', (criterion,))
                CriterionId = int(cur.fetchone()[0])

    #   category
            cur.execute('SELECT Id FROM categories WHERE Category = ? LIMIT 1', (category,))
            try:
                CategoryId = int(cur.fetchone()[0])
            except:
                cur.execute('INSERT OR IGNORE INTO categories (Category) VALUES (?)', (category,))
                cur.execute('SELECT Id FROM categories WHERE Category = ? LIMIT 1', (category,))
                CategoryId = int(cur.fetchone()[0])

            cur.execute(
                f'''INSERT OR IGNORE INTO visibility_{save_name}
                (Date, DeviceId, CriterionId, CategoryId, Score)
                VALUES (?, ?, ?, ?, ?)''',
                (date, DeviceId, CriterionId, CategoryId, score,)
            )

            con.commit()

        print(f'Completed {date}!')
    con.close()
    print(f'Completed {client_name}!')

#%%



#%%
con.commit()
con.close()

#%% retrieve reports

con = sqlite3.connect(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\architect_pace_normalised.db')
cur = con.cursor()

for i in client_list.index:
    client_name = client_list['Client Name'][i]
    client_id = client_list['STAT ID'][i]
    save_name = scrub(client_name)

    print (f'Adding {save_name} ranks...')

    requests_df = pd.read_sql_query(f'SELECT * FROM requests_{save_name} WHERE status = 0', con)

    all_kws = pd.read_sql_query(f'SELECT * FROM keywords', con)
    all_kws = all_kws['StatId'].to_list()

    for j in requests_df.index:
        con = sqlite3.connect(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\architect_pace_normalised.db')
        cur = con.cursor()

        job_id = int(requests_df['JobId'][j])
        date = requests_df['Date'][j]
        print(f'\n{j+1} of {len(requests_df)} - Fetching data for {date}')

        stream_url = f'/bulk_reports/stream_report/{job_id}'
        response = requests.get('https://iprospectman.getstat.com'+stream_url+f'?key={stat_key}')
        response = response.json()
        new_kws = response.get('Response').get('Project').get('Site').get('Keyword')

        print('Data received!')
        new_kws = pd.json_normalize(new_kws)

        print('Filtering data...')
        new_kws = new_kws[['Id','Ranking.date','Ranking.Google.Rank','Ranking.Google.BaseRank']]
        new_kws = new_kws[~new_kws['Id'].isin(all_kws)]
        new_kws = new_kws.replace('N/A', int('120'))
        new_kws = new_kws.fillna(int('120'))

        print('\nAdding keywords...')

#   add keywords to db
        for k in new_kws.index:
            if (k+1) % 500 == 0:
                print(f'Completed {k+1} of {len(new_kws)}')
            StatId = str(new_kws['Id'][k])
            Date = str(new_kws['Ranking.date'][k])
            GoogleRank = int(new_kws['Ranking.Google.Rank'][k])
            BaseRank = int(new_kws['Ranking.Google.BaseRank'][k])

            cur.execute('SELECT Id FROM keywords WHERE StatId = ? LIMIT 1', (StatId,))
            KeywordId = int(cur.fetchone()[0])

            cur.execute(f'''INSERT OR IGNORE INTO ranks_{save_name}(KeywordId, Date, GoogleRank, GoogleBaseRank)
                        VALUES (?, ?, ?, ?)''',(KeywordId, Date, GoogleRank, BaseRank))

            cur.execute(f'UPDATE requests_{save_name} SET Status = ? WHERE JobId = ?', (1, job_id,))

        print(f'Completed {k+1} of {len(new_kws)}\n\nSaving {date}...')
        con.commit()
        con.close()
        time.sleep(1)
        print(f'Done!')
print('Done!')

#%%

for i in new_kws.index:
    print(new_kws['Ranking.Google.Rank'][i])

#%%

con.close()
