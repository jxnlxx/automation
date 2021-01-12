#%%
import os
import time
import json
import sqlite3
import datetime
import requests
import openpyxl
import pandas as pd
import xlsxwriter as xl

from getstat import stat_subdomain, stat_key, stat_base_url                     # saved locally in C:\Users\USERNAME\AppData\Local\Programs\Python\Python37-32\Lib





date = '2018-12-31' #monday

print('\n'+'Requesting Site List from STAT...')

sites_all_url = f'{stat_base_url}/sites/all?&results=5000&format=json'
response = requests.get(sites_all_url)
request_counter = 1
response = response.json()
site_list = response.get('Response').get('Result')
print('Site List received!')
site_list = pd.DataFrame(site_list)
site_list = site_list.rename(columns={'Id' : 'UrlId'})

con = sqlite3.connect(r'C:\Users\JLee35\Automation\STAT\Bulk Ranks\bulk_ranks_normalised.db')

cur = con.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS sites(
    Id INTEGER PRIMARY KEY,
    SiteId INTEGER,
    ProjectId INTEGER,
    FolderId TEXT,
    FolderName TEXT,
    Title TEXT,
    Url TEXT,
    Synced TEXT,
    TotalKeywords INTEGER,
    Tracking TEXT,
    CreatedAt DATE,
    UpdatedAt DATE,
    RequestUrl TEXT,
    IndustryId INTEGER,
    UNIQUE (SiteID)
    );''')

for i in site_list.index:
    SiteId = site_list['SiteId'][i]
    ProjectId = site_list['ProjectId'][i]
    FolderId = site_list['FolderId'][i]
    FolderName = site_list['FolderName'][i]
    Title = site_list['Title'][i]
    Url = site_list['Url'][i]
    Synced = site_list['Synced'][i]
    TotalKeywords = site_list['TotalKeywords'][i]
    Tracking = site_list['Tracking'][i]
    CreatedAt = site_list['CreatedAt'][i]
    UpdatedAt = site_list['UpdatedAt'][i]
    RequestUrl = site_list['RequestUrl'][i]
    #IndustryId = None

    cur.execute(
        '''INSERT OR IGNORE INTO sites(
            SiteId, ProjectId, FolderId, FolderName, Title, Url, Synced, TotalKeywords, Tracking, CreatedAt, UpdatedAt, RequestUrl)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (SiteId, ProjectId, FolderId, FolderName, Title, Url, Synced, TotalKeywords, Tracking, CreatedAt, UpdatedAt, RequestUrl)
            )
    con.commit()

#%%

site_list = pd.read_sql_query('SELECT * FROM sites', con)
site_list = site_list[site_list['Tracking'] == 'true'].reset_index(drop=True)

for i in site_list.index:
    site_url = site_list['Url'][i]
    keywords_list = site_list['RequestUrl'][i]
    url = stat_base_url + keywords_list
    response = requests.get(url)
    response = response.json()
    print('\n'+f'Requesting data for {site_url}'
          '\n'+f'({i+1} of {len(site_list)})')
    n = 1 # for showing progress through site ids
    p = 1 # for showing progress through pagination

    kw_list = response.get('Response').get('Result')                                # extracts 'result' which is a list of dictionaries - 1 dict for each keyword
    print(p, keywords_list)
    p += 1 # for showing progress through pagination
    while True:                                                                     # Loop through pages to get all data
        try:
            nextpage = response.get('Response').get('nextpage')                     # get 'nextpage' path from response
            url = stat_base_url + nextpage                                          # create URL from 'base_url and 'nextpage'
            print(p, nextpage)                                                           # print n + url to show script is not idle
            response = requests.get(url)                                            # ping the new url to get the data
            response = response.json()                                              # convert the JSON response into dict
            new_kws = response.get('Response').get('Result')                        # extract 'Result' from dict
            kw_list = kw_list + new_kws                                             # append the data to the kw_list
            p += 1                                                                  # add 1 to 'n'
        except TypeError:# TypeError:                                               # if 'nextpage' is blank, this will yield a TypeError when attempting to append
            break                                                                   # this will be the last page, so break the loop
# process keywords for export
    total_kws = len(kw_list)
    kw_df = pd.json_normalize(kw_list)
    break

cur.execute('''CREATE TABLE IF NOT EXISTS phrases(
    Id INTEG ER PRIMARY KEY,
    Phrase TEXT NOT NULL,
    UNIQUE (Phrase)
    );''')

cur.execute('''CREATE TABLE IF NOT EXISTS markets(
    Id INTEGER PRIMARY KEY,
    Market TEXT NOT NULL,
    UNIQUE (Market)
    );''')

cur.execute('''CREATE TABLE IF NOT EXISTS devices(
    Id INTEGER PRIMARY KEY,
    Device TEXT NOT NULL,
    UNIQUE (Device)
    );''')

cur.execute('''CREATE TABLE IF NOT EXISTS industries(
    Id INTEGER PRIMARY KEY,
    Industry TEXT NOT NULL,
    UNIQUE (Industry)
    );''')

cur.execute('''CREATE TABLE IF NOT EXISTS categories(
    Id INTEGER PRIMARY KEY,
    Category TEXT NOT NULL,
    UNIQUE (Category)
    );''')

cur.execute(''' CREATE TABLE IF NOT EXISTS keywords(
    Id INTEGER PRIMARY KEY,
    StatId INTEGER NOT NULL,
    PhraseId INTEGER NOT NULL,
    MarketId INTEGER NOT NULL,
    DeviceId INTEGER NOT NULL,
    SiteId INTEGER NOT NULL,
    ClientId INTEGER NOT NULL
    IndustryId INTEGER,
    CategoryId INTEGER,
    Translation TEXT,
    CreatedAt DATETIME NOT NULL,
    GlobalSearchVolume INTEGER NOT NULL,
    TargetedSearchVolume INTEGER NOT NULL,
    Jan INTEGER,
    Feb INTEGER,
    Mar INTEGER,
    Apr INTEGER,
    May INTEGER,
    Jun INTEGER,
    Jul INTEGER,
    Aug INTEGER,
    Sep INTEGER,
    Oct INTEGER,
    Nov INTEGER,
    Dec INTEGER,
    FOREIGN KEY (PhraseId) REFERENCES phrases (Id),
    FOREIGN KEY (MarketId) REFERENCES markets (Id),
    FOREIGN KEY (DeviceId) REFERENCES devices (Id),
    FOREIGN KEY (IndustryId) REFERENCES industries (Id),
    FOREIGN KEY (CategoryId) REFERENCES categories (Id)
    UNIQUE (StatID)
    );''')


for i in kw_df.index:
    ClientId = kw_df['Id']
    SiteId = kw_df['SiteId'][i]
    Keyword = kw_df['Keyword'][i]
    KeywordMarket = kw_df['KeywordMarket'][i]
    KeywordLocation = kw_df['KeywordLocation'][i]
    KeywordDevice = kw_df['KeywordDevice'][i]
    Translation = kw_df['KeywordTranslation'][i]
    StatTags = kw_df['KeywordTags'][i]
    CreatedAt = kw_df['CreatedAt'][i]
    RequestUrl = kw_df['RequestUrl'][i]
    AdvertiserCompetition = kw_df['KeywordStats.AdvertiserCompetition'][i]
    GlobalSearchVolume = kw_df['KeywordStats.GlobalSearchVolume'][i]
    TargetedSearchVolume = kw_df['KeywordStats.RegionalSearchVolume'][i]
    Jan = kw_df['KeywordStats.LocalSearchTrendsByMonth.Jan'][i]
    Feb = kw_df['KeywordStats.LocalSearchTrendsByMonth.Feb'][i]
    Mar = kw_df['KeywordStats.LocalSearchTrendsByMonth.Mar'][i]
    Apr = kw_df['KeywordStats.LocalSearchTrendsByMonth.Apr'][i]
    May = kw_df['KeywordStats.LocalSearchTrendsByMonth.May'][i]
    Jun = kw_df['KeywordStats.LocalSearchTrendsByMonth.Jun'][i]
    Jul = kw_df['KeywordStats.LocalSearchTrendsByMonth.Jul'][i]
    Aug = kw_df['KeywordStats.LocalSearchTrendsByMonth.Aug'][i]
    Sep = kw_df['KeywordStats.LocalSearchTrendsByMonth.Sep'][i]
    Oct = kw_df['KeywordStats.LocalSearchTrendsByMonth.Oct'][i]
    Nov = kw_df['KeywordStats.LocalSearchTrendsByMonth.Nov'][i]
    Dec = kw_df['KeywordStats.LocalSearchTrendsByMonth.Dec'][i]
    CPC = kw_df['KeywordStats.CPC'][i]

# Phrases
    cur.execute('SELECT Id FROM phrases WHERE Phrase = ? LIMIT 1', (Keyword,))
    try:
        PhraseId = int(cur.fetchone()[0])
    except:
        cur.execute('INSERT OR IGNORE INTO phrases (Phrase) VALUES (?)', (Keyword,))
        cur.execute('SELECT Id FROM phrases WHERE Phrase = ? LIMIT 1', (Keyword,))
        PhraseId = int(cur.fetchone()[0])

# Markets
    cur.execute('SELECT Id FROM markets WHERE Market = ? LIMIT 1', (KeywordMarket,))
    try:
        MarketId = int(cur.fetchone()[0])
    except:
        cur.execute('INSERT OR IGNORE INTO markets (Market) VALUES (?)', (KeywordMarket,))
        cur.execute('SELECT Id FROM markets WHERE Market = ? LIMIT 1', (KeywordMarket,))
        MarketId = int(cur.fetchone()[0])

# Devices
    cur.execute('SELECT Id FROM devices WHERE Device = ? LIMIT 1', (KeywordDevice,))
    try:
        DeviceId = int(cur.fetchone()[0])
    except:
        cur.execute('INSERT OR IGNORE INTO devices (Device) VALUES (?)', (KeywordDevice,))
        cur.execute('SELECT Id FROM devices WHERE Device = ? LIMIT 1', (KeywordDevice,))
        DeviceId = int(cur.fetchone()[0])

# # Industries
#     cur.execute('SELECT Id FROM industries WHERE Industry = ? LIMIT 1', (Industry,))
#     try:
#         IndustryId = int(cur.fetchone()[0])
#     except:
#         cur.execute('INSERT OR IGNORE INTO industries (Industry) VALUES (?)', (Industry,))
#         cur.execute('SELECT Id FROM industries WHERE Industry = ? LIMIT 1', (Industry,))
#         IndustryId = int(cur.fetchone()[0])

# # Categories
#     cur.execute('SELECT Id FROM categories WHERE Category = ? LIMIT 1', (Category,))
#     try:
#         CategoryId = int(cur.fetchone()[0])
#     except:
#         cur.execute('INSERT OR IGNORE INTO categories (Category) VALUES (?)', (Category,))
#         cur.execute('SELECT Id FROM categories WHERE Category = ? LIMIT 1', (Category,))
#         CategoryId  = int(cur.fetchone()[0])

# Keywords
    cur.execute('SELECT Id FROM keywords WHERE PhraseId = ? AND MarketId = ? AND DeviceId = ?', (PhraseId, MarketId, DeviceId))
    try:
        KeywordId = int(cur.execute.fetchone()[0])
    except:
        cur.execute(
        '''INSERT OR IGNORE INTO keywords
        (StatId, PhraseId, MarketId, DeviceId, IndustryId, CreatedAt, GlobalSearchVolume, TargetedSearchVolume, Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (StatId, PhraseId, MarketId, DeviceId, IndustryId, CreatedAt, GlobalSearchVolume, TargetedSearchVolume, Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec)
        )

    'Id',
    'StatId',
    'Keyword',
    'KeywordMarket',
    'KeywordLocation',
    'KeywordDevice',
    'KeywordTranslation', 'KeywordTags',
    'CreatedAt',
    'RequestUrl',
    'AdvertiserCompetition',
    'GlobalSearchVolume',
    'RegionalSearchVolume',
    'Jan',
    'Feb',
    'Mar',
    'Apr',
    'May',
    'Jun',
    'Jul',
    'Aug',
    'Sep',
    'Oct',
    'Nov',
    'Dec',
    'CPC',






# %%
