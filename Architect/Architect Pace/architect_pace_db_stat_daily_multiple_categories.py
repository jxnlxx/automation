#%% architect_pace_db_stat_daily_multiple_categories.py

import time
import sqlite3
import requests
import pandas as pd
import datetime as dt
import numpy as np
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

# request exports for all clients

count = 0
for i in client_list.index:
    con = sqlite3.connect(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\architect_pace_normalised.db')
    cur = con.cursor()
    client_name = client_list['Client Name'][i]
    save_name = scrub(client_name)
    stat_id = client_list['STAT ID'][i]

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
    end_date = dt.date.today() - dt.timedelta(days=1)
    if start_date <= end_date:
        print('\nRequesting ranks...')
        pass
    else:
        print('Date aready requested! \nMoving onto next client.')
        continue

#   request reports for between 'start_date' and 'end_date'
    n= 1
    for j in pd.date_range(start_date, end_date):
        date = str(j.strftime('%Y-%m-%d'))
        print(f'{n:03} Requesting {date} rank report for {client_name}')
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
        count += 1
    con.commit()
    con.close()

print(f'{count} reports requested!')
sleep = count*20
print(f'Sleeping for {sleep} seconds...')
t = 0
while t < sleep:
    time.sleep(20)
    t += 20
    print(t)

# retrieve reports

print('Retrieving reports...')

incomplete = []
n=1
for i in client_list.index:
    con = sqlite3.connect(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\architect_pace_normalised.db')
    cur = con.cursor()
    client_name = client_list['Client Name'][i]
    client_id = client_list['STAT ID'][i]
    save_name = scrub(client_name)

    print(f'\nChecking {client_name}\'s final job status...')

    requests_df = pd.read_sql_query(f'SELECT * FROM requests_{save_name} WHERE status = 0', con)
    try:
        final_job = requests_df['JobId'].iloc[-1]
    except IndexError:
        continue
    response = requests.get(f'{stat_base_url}/bulk/status?id={final_job}&format=json')
    response = response.json()
    status = response.get('Response').get('Result').get('Status')
    if status != 'Completed':
        print(f'{client_name} - Final job status: {status}')
        incomplete += [client_name]
        continue
    else:
        print(f'{client_name} final job \'{status}\'')
        pass

    print (f'Adding {client_name} ranks...')

    all_kws = pd.read_sql_query(f'SELECT * FROM keywords', con)
    all_kws = all_kws['StatId'].to_list()

    for j in requests_df.index:
        con = sqlite3.connect(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\architect_pace_normalised.db')
        cur = con.cursor()

        job_id = int(requests_df['JobId'][j])
        date = requests_df['Date'][j]
        print(f'\nBeginning job {n} of {count}')
        n+=1

        print(f'\nFetching {client_name} data for {date}')
        stream_url = f'/bulk_reports/stream_report/{job_id}'
        response = requests.get('https://iprospectman.getstat.com'+stream_url+f'?key={stat_key}')
        response = response.json()
        new_kws = response.get('Response').get('Project').get('Site').get('Keyword')

        print('Data received!')
        new_kws = pd.json_normalize(new_kws)

        print('Filtering data...')
        new_kws = new_kws[['Id','Ranking.date','Ranking.Google.Rank','Ranking.Google.BaseRank']]

        new_kws['Id'] = new_kws['Id'].astype(int) # changes 'Id' to int so filter works - StatId is stored in the db as int
        new_kws = new_kws[new_kws['Id'].isin(all_kws)]

        new_kws = new_kws.replace('N/A', int('120'))
        new_kws = new_kws.fillna(int('120'))

        new_kws = new_kws.reset_index(drop=True)

        print(f'\nAdding keywords...')

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

        print(f'Completed {len(new_kws)} of {len(new_kws)}')
        print(f'\nSaving {date}...')
        con.commit()
        con.close()
        print(f'Done!')
        print('----------------------')
        time.sleep(1)

if len(incomplete) > 0:
    print('\nThese Clients were skipped:')
    for i in incomplete:
        print(i)
else:
    print('\nFinished retrieving reports!')

# transform data

multiple_categories = ['JD Williams']

print('\nTransforming Data...')

for i in client_list.index:
    con = sqlite3.connect(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\architect_pace_normalised.db')
    cur = con.cursor()
    client_name = client_list['Client Name'][i]
    save_name = scrub(client_name)


    cur.execute(f'''CREATE TABLE IF NOT EXISTS visibility_{save_name}2(
        Date TEXT NOT NULL,
        DeviceId INTEGER NOT NULL,
        CriterionId INTEGER NOT NULL,
        Category1Id INTEGER NOT NULL,
        Category2Id INTEGER NOT NULL,
        Score INTEGER NOT NULL,
        FOREIGN KEY (DeviceId) REFERENCES keywords (Id),
        FOREIGN KEY (CriterionId) REFERENCES keywords (Id),
        FOREIGN KEY (Category1Id) REFERENCES category1 (Id),
        FOREIGN KEY (Category2Id) REFERENCES category2 (Id),
        UNIQUE (Date, DeviceId, CriterionId, Category1Id, Category2Id)
        );''')

    print(f'Starting {client_name}...')

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

        print(f'\n{client_name} - job {counter} of {len(pd.date_range(start_date, end_date))}')
        counter += 1

        print(f'Starting {date}...')

        if client_name in multiple_categories:
            sql = f'''
                SELECT ranks_{save_name}.Date, keywords.Keyword, keywords.TargetedSearchVolume, ranks_{save_name}.GoogleBaseRank as Rank, devices.Device, category1.Category as Category1, category2.Category as Category2, ctr_{save_name}.CTR
                FROM ranks_{save_name}
                JOIN keywords ON ranks_{save_name}.KeywordId = keywords.Id
                JOIN devices ON keywords.DeviceId = devices.Id
                JOIN category1 ON keywords.Category1Id = category1.Id
                JOIN category2 ON keywords.Category2Id = category2.Id
                JOIN ctr_{save_name} ON ranks_{save_name}.GoogleBaseRank = ctr_{save_name}.Position
                WHERE date = :date;
                '''
            params = {'date':date}
            df = pd.read_sql(sql, params=params, con=con)

        else:
            sql = f'''
                SELECT ranks_{save_name}.Date, keywords.Keyword, keywords.TargetedSearchVolume, ranks_{save_name}.GoogleBaseRank as Rank, devices.Device, category1.Category as Category1, ctr_{save_name}.CTR
                FROM ranks_{save_name}
                JOIN keywords ON ranks_{save_name}.KeywordId = keywords.Id
                JOIN devices ON keywords.DeviceId = devices.Id
                JOIN category1 ON keywords.Category1Id = category1.Id
                JOIN ctr_{save_name} ON ranks_{save_name}.GoogleBaseRank = ctr_{save_name}.Position
                WHERE date = :date;
                '''
            params = {'date':date}
            df = pd.read_sql(sql, params=params, con=con)
        print('Data retrieved!')
#        df.to_csv(fr'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\df.csv', index=False)

#   aggreagate

        print('Aggregating data...')
        df['DailySearchVolume'] = (df['TargetedSearchVolume'].astype(int)/30).astype(int)
        df['WeightedRank'] = (df['DailySearchVolume'].astype(int) * df['Rank'].astype(int)).apply(np.floor).astype(int)
        df['CtrScore'] = round(df['CTR'] * df['DailySearchVolume'],0).apply(np.floor).astype(int)

        device_list = list(dict.fromkeys(df['Device'].to_list()))
        category1_list = list(dict.fromkeys(df['Category1'].to_list()))
        if client_name in multiple_categories:
            visibility = pd.DataFrame(columns=['Date', 'Device', 'Criteria', 'Category1', 'Category2', 'Score'])
            category2_list = list(dict.fromkeys(df['Category2'].to_list()))
        else:
            visibility = pd.DataFrame(columns=['Date', 'Device', 'Criteria', 'Category1', 'Score'])
            category2_list = []

        rank_brackets = ['#1', '#2 - #5', '#6 - #10', '#11 - #20', '#21 - #30', '#31 - #40', '#41 - #50']

        for i in device_list:
            device = df[df['Device'] == i]
#            device.to_csv(fr'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\{i}.csv', index=False)
            k = 'All Keywords'

            awr = round(device['WeightedRank'].sum() / device['DailySearchVolume'].sum(),0)
            temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':'Average Weighted Rank','Category1':k,'Category2':'','Score':awr},index=[0])
            visibility = visibility.append(temp)

            vis = device['CtrScore'].sum()
            temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':'Visibility Score','Category1':k,'Category2':'','Score':vis},index=[0])
            visibility = visibility.append(temp)

            for j in rank_brackets:
                if j == '#1':
                    score = device[device['Rank'] == 1].shape[0]
                    temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category1':k,'Category2':'','Score':score},index=[0])
                    visibility = visibility.append(temp)
                elif j == '#2 - #5':
                    score = device[(device['Rank'] >= 2) & (device['Rank'] <= 5)].shape[0]
                    temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category1':k,'Category2':'','Score':score},index=[0])
                    visibility = visibility.append(temp)
                elif j == '#6 - #10':
                    score = device[(device['Rank'] >= 6) & (device['Rank'] <= 10)].shape[0]
                    temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category1':k,'Category2':'','Score':score},index=[0])
                    visibility = visibility.append(temp)
                elif j == '#11 - #20':
                    score = device[(device['Rank'] >= 11) & (device['Rank'] <= 20)].shape[0]
                    temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category1':k,'Category2':'','Score':score},index=[0])
                    visibility = visibility.append(temp)
                elif j == '#21 - #30':
                    score = device[(device['Rank'] >= 21) & (device['Rank'] <= 30)].shape[0]
                    temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category1':k,'Category2':'','Score':score},index=[0])
                    visibility = visibility.append(temp)
                elif j == '#31 - #40':
                    score = device[(device['Rank'] >= 31) & (device['Rank'] <= 40)].shape[0]
                    temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category1':k,'Category2':'','Score':score},index=[0])
                    visibility = visibility.append(temp)
                elif j == '#41 - #50':
                    score = device[(device['Rank'] >= 41) & (device['Rank'] <= 50)].shape[0]
                    temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category1':k,'Category2':'','Score':score},index=[0])
                    visibility = visibility.append(temp)

            if len(category2_list) > 0:
                for l in category2_list:
                    category = device[device['Category2'] == l]
#                    category.to_csv(fr'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\{k}.csv', index=False)

                    awr = round(category['WeightedRank'].sum() / category['DailySearchVolume'].sum(),0)
                    temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':'Average Weighted Rank','Category1':k,'Category2':l,'Score':awr},index=[0])
                    visibility = visibility.append(temp)

                    vis = category['CtrScore'].sum()
                    temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':'Visibility Score','Category1':k,'Category2':l,'Score':vis},index=[0])
                    visibility = visibility.append(temp)

                    for j in rank_brackets:
                        if j == '#1':
                            score = category[category['Rank']==1].shape[0]
                            temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category1':k,'Category2':l,'Score':score},index=[0])
                            visibility = visibility.append(temp)
                        elif j == '#2 - #5':
                            score = category[(category['Rank'] >= 2) & (category['Rank'] <= 5)].shape[0]
                            temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category1':k,'Category2':l,'Score':score},index=[0])
                            visibility = visibility.append(temp)
                        elif j == '#6 - #10':
                            score = category[(category['Rank'] >= 6) & (category['Rank'] <= 10)].shape[0]
                            temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category1':k,'Category2':l,'Score':score},index=[0])
                            visibility = visibility.append(temp)
                        elif j == '#11 - #20':
                            score = category[(category['Rank'] >= 11) & (category['Rank'] <= 20)].shape[0]
                            temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category1':k,'Category2':l,'Score':score},index=[0])
                            visibility = visibility.append(temp)
                        elif j == '#21 - #30':
                            score = category[(category['Rank'] >= 21) & (category['Rank'] <= 30)].shape[0]
                            temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category1':k,'Category2':l,'Score':score},index=[0])
                            visibility = visibility.append(temp)
                        elif j == '#31 - #40':
                            score = category[(category['Rank'] >= 31) & (category['Rank'] <= 40)].shape[0]
                            temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category1':k,'Category2':l,'Score':score},index=[0])
                            visibility = visibility.append(temp)
                        elif j == '#41 - #50':
                            score = category[(category['Rank'] >= 41) & (category['Rank'] <= 50)].shape[0]
                            temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category1':k,'Category2':l,'Score':score},index=[0])
                            visibility = visibility.append(temp)
            else:
                pass

            for k in category1_list:
                category = device[device['Category1'] == k]

                awr = round(category['WeightedRank'].sum() / category['DailySearchVolume'].sum(),0)
                temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':'Average Weighted Rank','Category1':k,'Category2':'','Score':awr},index=[0])
                visibility = visibility.append(temp)

                vis = category['CtrScore'].sum()
                temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':'Visibility Score','Category1':k,'Category2':'','Score':vis},index=[0])
                visibility = visibility.append(temp)

                for j in rank_brackets:
                    if j == '#1':
                        score = category[category['Rank']==1].shape[0]
                        temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category1':k,'Category2':'','Score':score},index=[0])
                        visibility = visibility.append(temp)
                    elif j == '#2 - #5':
                        score = category[(category['Rank'] >= 2) & (category['Rank'] <= 5)].shape[0]
                        temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category1':k,'Category2':'','Score':score},index=[0])
                        visibility = visibility.append(temp)
                    elif j == '#6 - #10':
                        score = category[(category['Rank'] >= 6) & (category['Rank'] <= 10)].shape[0]
                        temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category1':k,'Category2':'','Score':score},index=[0])
                        visibility = visibility.append(temp)
                    elif j == '#11 - #20':
                        score = category[(category['Rank'] >= 11) & (category['Rank'] <= 20)].shape[0]
                        temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category1':k,'Category2':'','Score':score},index=[0])
                        visibility = visibility.append(temp)
                    elif j == '#21 - #30':
                        score = category[(category['Rank'] >= 21) & (category['Rank'] <= 30)].shape[0]
                        temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category1':k,'Category2':'','Score':score},index=[0])
                        visibility = visibility.append(temp)
                    elif j == '#31 - #40':
                        score = category[(category['Rank'] >= 31) & (category['Rank'] <= 40)].shape[0]
                        temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category1':k,'Category2':'','Score':score},index=[0])
                        visibility = visibility.append(temp)
                    elif j == '#41 - #50':
                        score = category[(category['Rank'] >= 41) & (category['Rank'] <= 50)].shape[0]
                        temp = pd.DataFrame({'Date':date,'Device':i,'Criteria':j,'Category1':k,'Category2':'','Score':score},index=[0])
                        visibility = visibility.append(temp)

        visibility['Score'] = visibility['Score'].astype(int)
        visibility = visibility.reset_index(drop=True)
#        visibility.to_csv(fr'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\{date}.csv',index=False)
        print(f'Adding to database...')
        for i in visibility.index:
            device  = str(visibility['Device'][i])
            criterion  = str(visibility['Criteria'][i])
            category1  = str(visibility['Category1'][i])
            category2  = str(visibility['Category2'][i])
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

    #   category1
            cur.execute('SELECT Id FROM category1 WHERE Category = ? LIMIT 1', (category1,))
            try:
                Category1Id = int(cur.fetchone()[0])
            except:
                cur.execute('INSERT OR IGNORE INTO category1 (Category) VALUES (?)', (category1,))
                cur.execute('SELECT Id FROM category1 WHERE Category = ? LIMIT 1', (category1,))
                Category1Id = int(cur.fetchone()[0])

    #   category2
            if client_name in multiple_categories:
                cur.execute('SELECT Id FROM category2 WHERE Category = ? LIMIT 1', (category2,))
                try:
                    Category2Id = int(cur.fetchone()[0])
                except:
                    cur.execute('INSERT OR IGNORE INTO category2 (Category) VALUES (?)', (category2,))
                    cur.execute('SELECT Id FROM category2 WHERE Category = ? LIMIT 1', (category2,))
                    Category2Id = int(cur.fetchone()[0])

                cur.execute(
                    f'''INSERT OR IGNORE INTO visibility_{save_name}
                    (Date, DeviceId, CriterionId, Category1Id, Category2Id, Score)
                    VALUES (?, ?, ?, ?, ?, ?)''',
                    (date, DeviceId, CriterionId, Category1Id, Category2Id, score,)
                )

            else:
                cur.execute(
                    f'''INSERT OR IGNORE INTO visibility_{save_name}
                    (Date, DeviceId, CriterionId, Category1Id, Score)
                    VALUES (?, ?, ?, ?, ?)''',
                    (date, DeviceId, CriterionId, Category1Id, score,)
                )

            con.commit()

        print(f'Completed {date}!')
    con.close()
    print(f'\nCompleted {client_name}!\n')

print('Data transformation complete!')

# authenticate with gsheets
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
google_auth = r'C:\Users\JLee35\OneDrive - Dentsu Aegis Network\PROJECTS\Python\APIs\keys\iprospectseonorth\google_auth.json'
creds = ServiceAccountCredentials.from_json_keyfile_name(google_auth, scope)

# upload visibility table to gsheets
for i in client_list.index:
    con = sqlite3.connect(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\architect_pace_normalised.db')
    cur = con.cursor()
    client_name = client_list['Client Name'][i]
    save_name = scrub(client_name)
    gspread_id = client_list['GSheet ID'][i]
    print(f'\nRetrieving {client_name} visibility table from database...')

    year_ago = int(dt.datetime.now().strftime('%Y'))-1
    year_ago = end_date.replace(year=year_ago).strftime('%Y-%m-%d')
#TODO insert code to add CTR model to gsheets

    if client_name in multiple_categories:
        sql = f'''
        SELECT visibility_{save_name}.Date, devices.Device as Device, criteria.Criterion as Criteria, category1.Category as Category1, category2.Category as Category2, visibility_{save_name}.Score
        FROM visibility_{save_name}
        JOIN devices ON visibility_{save_name}.DeviceId = devices.Id
        JOIN criteria ON visibility_{save_name}.CriterionId = criteria.Id
        JOIN category1 ON visibility_{save_name}.Category1Id = category1.Id
        JOIN category2 ON visibility_{save_name}.Category2Id = category2.Id
        WHERE date(Date) >= :date;'''

        params={'date':year_ago}

        df = pd.read_sql(sql, params=params, con=con)
        df2 = df.copy()
        df3 = df[~df['Category2'].isin([""])]

        for i in df3.index:
            df2.at[i,'Category1'] = ""

        df2['Category'] = df2['Category1'] + df2['Category2']
        df = df2[['Date','Device','Criteria','Category','Score']]
    else:
        sql = f'''
        SELECT visibility_{save_name}.Date, devices.Device as Device, criteria.Criterion as Criteria, category1.Category as Category, visibility_{save_name}.Score
        FROM visibility_{save_name}
        JOIN devices ON visibility_{save_name}.DeviceId = devices.Id
        JOIN criteria ON visibility_{save_name}.CriterionId = criteria.Id
        JOIN category1 ON visibility_{save_name}.Category1Id = category1.Id
        WHERE date(Date) >= :date;'''

        params={'date':year_ago}
        df = pd.read_sql(sql, params=params, con=con)

    df_rows, df_cols = df.shape

    print(f'Uploading {client_name} visibility table to Google Sheets...')

    client = gspread.authorize(creds)
    sheet = client.open_by_key(gspread_id)

    try:
        worksheet = sheet.worksheet(f'{client_name} - Visibility')
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound as err:
        worksheet = sheet.add_worksheet(title=f'{client_name} - Visibility', rows=1, cols=1)

    set_with_dataframe(worksheet, df)

    print(f'{client_name} complete!')

print('\nDURN!')

#%%

con = sqlite3.connect(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\architect_pace_normalised.db')
cur = con.cursor()

cur.execute(f'''CREATE TABLE IF NOT EXISTS visibility_{save_name}(
    Date TEXT NOT NULL,
    DeviceId INTEGER NOT NULL,
    CriterionId INTEGER NOT NULL,
    CategoryId INTEGER NOT NULL,
    Score INTEGER NOT NULL,
    FOREIGN KEY (DeviceId) REFERENCES keywords (Id),
    FOREIGN KEY (CriterionId) REFERENCES keywords (Id),
    FOREIGN KEY (CategoryId) REFERENCES keywords (Id),
    UNIQUE (Date, DeviceId, CriterionId, CategoryId)
    );''')

con.commit()
con.close()

# %%


