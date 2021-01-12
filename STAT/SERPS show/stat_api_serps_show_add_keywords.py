#%%

import os
import sqlite3
import pandas as pd
import requests
import datetime as dt
from getstat import stat_subdomain, stat_key, stat_base_url                                  # saved locally in C:\Users\USERNAME\AppData\Local\Programs\Python\Python37-32\Lib


date = dt.datetime.today() - dt.timedelta(days=7)
date = date.strftime('%Y-%m-%d')

clients = pd.read_csv(fr'C:\Users\JLee35\Automation\STAT\SERPS show\serps_show_sites.csv')

for i in clients.index:
    site_url = clients['Url'][i]
    print(site_url)
    site_id = clients['Id'][i]
    Industry = clients['Industry'][i]
    p = 1
    keywords_list = f'/keywords/list?site_id={site_id}&results=5000&format=json'
    url = stat_base_url + keywords_list
    response = requests.get(url)
    response = response.json()
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

# sort and filter keywords

    kw_df = pd.json_normalize(kw_list)
    kw_df = kw_df.drop(kw_df[kw_df['KeywordMarket'] != 'GB-en'].index)
    kw_df['KeywordStats.RegionalSearchVolume'] = kw_df['KeywordStats.RegionalSearchVolume'].astype(int)
    kw_df = kw_df.sort_values(by='KeywordStats.RegionalSearchVolume', axis=0, ascending=False)

    smartphone = kw_df.drop(kw_df[kw_df['KeywordDevice'] == 'desktop'].index)
    desktop = kw_df.drop(kw_df[kw_df['KeywordDevice'] == 'smartphone'].index)

    union = pd.concat([desktop[['Keyword']],smartphone[['Keyword']]], join='inner') # get the union of both desktop and smartphone

    smartphone = smartphone[smartphone['Keyword'].isin(union['Keyword'])] # choose smarphone kws that are in the union
    smartphone = smartphone.head(100) # take top 100 smartphone kws

    desktop = desktop[desktop['Keyword'].isin(smartphone['Keyword'])] # choose desktop kws based on what's in the smartphone list

    all_kws = desktop.append(smartphone, ignore_index=True).reset_index(drop=True) # concat the two

# add data to database

    con = sqlite3.connect(r'C:\Users\JLee35\Automation\STAT\SERPS show\serps_show_normalised.db')

    cur = con.cursor()

    for i in all_kws.index:
        StatId = int(all_kws['Id'][i])
        Keyword = str(all_kws['Keyword'][i])
        KeywordMarket = str(all_kws['KeywordMarket'][i])
        KeywordDevice = str(all_kws['KeywordDevice'][i])
        CreatedAt = str(all_kws['CreatedAt'][i])
        GlobalSearchVolume = int(all_kws['KeywordStats.GlobalSearchVolume'][i])
        TargetedSearchVolume = int(all_kws['KeywordStats.RegionalSearchVolume'][i])
        Jan = int(all_kws['KeywordStats.LocalSearchTrendsByMonth.Jan'][i])
        Feb = int(all_kws['KeywordStats.LocalSearchTrendsByMonth.Feb'][i])
        Mar = int(all_kws['KeywordStats.LocalSearchTrendsByMonth.Mar'][i])
        Apr = int(all_kws['KeywordStats.LocalSearchTrendsByMonth.Apr'][i])
        May = int(all_kws['KeywordStats.LocalSearchTrendsByMonth.May'][i])
        Jun = int(all_kws['KeywordStats.LocalSearchTrendsByMonth.Jun'][i])
        Jul = int(all_kws['KeywordStats.LocalSearchTrendsByMonth.Jul'][i])
        Aug = int(all_kws['KeywordStats.LocalSearchTrendsByMonth.Aug'][i])
        Sep = int(all_kws['KeywordStats.LocalSearchTrendsByMonth.Sep'][i])
        Oct = int(all_kws['KeywordStats.LocalSearchTrendsByMonth.Oct'][i])
        Nov = int(all_kws['KeywordStats.LocalSearchTrendsByMonth.Nov'][i])
        Dec = int(all_kws['KeywordStats.LocalSearchTrendsByMonth.Dec'][i])

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

# Industries
        cur.execute('SELECT Id FROM industries WHERE Industry = ? LIMIT 1', (Industry,))
        try:
            IndustryId = int(cur.fetchone()[0])
        except:
            cur.execute('INSERT OR IGNORE INTO industries (Industry) VALUES (?)', (Industry,))
            cur.execute('SELECT Id FROM industries WHERE Industry = ? LIMIT 1', (Industry,))
            IndustryId = int(cur.fetchone()[0])

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

    con.commit()

con.close()

print('DURN!')

#%%
# while True:
#     acct = input('Enter a Twitter account, or quit: ')
#     if (acct == 'quit'): break
#     if (len(acct) < 1):
#         cur.execute('SELECT id, name FROM People WHERE retrieved = 0 LIMIT 1')
#         try:
#             (id, acct) = cur.fetchone()
#         except:
#             print('No unretrieved Twitter accounts found')
#             continue
#     else:
#         cur.execute('SELECT id FROM People WHERE name = ? LIMIT 1',
#                     (acct, ))
#         try:
#             id = cur.fetchone()[0]
#         except:
#             cur.execute('''INSERT OR IGNORE INTO People
#                         (name, retrieved) VALUES (?, 0)''', (acct, ))
#             conn.commit()
#             if cur.rowcount != 1:
#                 print('Error inserting account:', acct)
#                 continue
#             id = cur.lastrowid
