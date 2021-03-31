#%% stat_ranks_db_update_keyword_categories.py

import os
import time
import json
import sqlite3
import datetime as dt
import requests
import numpy as np
import pandas as pd
import getstat

# load client_list
client_list = pd.read_excel(r'C:\Users\JLee35\dentsu\Automation - Documents\General\STAT Ranks - Client List.xlsx')
client_list['STAT ID'] = client_list['STAT ID'].astype(int) # ensure that STAT ID is not a float
client_list['STAT ID'] = client_list['STAT ID'].astype(str) # ensure that STAT ID is str
print('Done!')

#%% select by folder name

folder_name = 'N Brown'
client_list = client_list[client_list['Folder Name'] == folder_name]

#%% select by client name

client_name = 'musicMagpie Store'
client_list = client_list[client_list['Client Name'] == client_name]

#%% updating categories

start_time = dt.datetime.now().strftime("%H:%M:%S")
root = fr'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\STAT\Clients'

for i in client_list.index:
    folder_name = client_list['Folder Name'][i]
    client_name = client_list['Client Name'][i]
    stat_id = client_list['STAT ID'][i]
    csv_name = getstat.scrub(client_name) + '_keywords_list.csv'
    database_name = getstat.dbize(folder_name)
    setup = "Setup"
    con = sqlite3.connect(os.path.join(root, folder_name, database_name))
    cur = con.cursor()

    df = pd.read_csv(os.path.join(root, folder_name, setup, csv_name))
    df = df.fillna("")

    for j in df.index:
        StatId = int(df['Id'][j])
        Keyword = str(df['Keyword'][j]).strip()
        print(f"Updating '{Keyword}'")
        KeywordCategory1 = str(df['Category1'][j]).strip()
        KeywordCategory2 = str(df['Category2'][j]).strip()
        KeywordCategory3 = str(df['Category3'][j]).strip()
        KeywordCategory4 = str(df['Category4'][j]).strip()
        KeywordCategory5 = str(df['Category5'][j]).strip()

#   Category1
        cur.execute('SELECT Id FROM KeywordCategory1 WHERE Category = ? LIMIT 1', (KeywordCategory1,))
        try:
            Category1Id = int(cur.fetchone()[0])
        except:
            cur.execute('INSERT OR IGNORE INTO KeywordCategory1 (Category) VALUES (?)', (KeywordCategory1,))
            cur.execute('SELECT Id FROM KeywordCategory1 WHERE Category = ? LIMIT 1', (KeywordCategory1,))
            Category1Id = int(cur.fetchone()[0])

#   Category2
        cur.execute('SELECT Id FROM KeywordCategory2 WHERE Category = ? LIMIT 1', (KeywordCategory2,))
        try:
            Category2Id = int(cur.fetchone()[0])
        except:
            cur.execute('INSERT OR IGNORE INTO KeywordCategory2 (Category) VALUES (?)', (KeywordCategory2,))
            cur.execute('SELECT Id FROM KeywordCategory2 WHERE Category = ? LIMIT 1', (KeywordCategory2,))
            Category2Id = int(cur.fetchone()[0])

#   Category3
        cur.execute('SELECT Id FROM KeywordCategory3 WHERE Category = ? LIMIT 1', (KeywordCategory3,))
        try:
            Category3Id = int(cur.fetchone()[0])
        except:
            cur.execute('INSERT OR IGNORE INTO KeywordCategory3 (Category) VALUES (?)', (KeywordCategory3,))
            cur.execute('SELECT Id FROM KeywordCategory3 WHERE Category = ? LIMIT 1', (KeywordCategory3,))
            Category3Id = int(cur.fetchone()[0])

#   Category4
        cur.execute('SELECT Id FROM KeywordCategory4 WHERE Category = ? LIMIT 1', (KeywordCategory4,))
        try:
            Category4Id = int(cur.fetchone()[0])
        except:
            cur.execute('INSERT OR IGNORE INTO KeywordCategory4 (Category) VALUES (?)', (KeywordCategory4,))
            cur.execute('SELECT Id FROM KeywordCategory4 WHERE Category = ? LIMIT 1', (KeywordCategory4,))
            Category4Id = int(cur.fetchone()[0])

#   Category5
        cur.execute('SELECT Id FROM KeywordCategory5 WHERE Category = ? LIMIT 1', (KeywordCategory5,))
        try:
            Category5Id = int(cur.fetchone()[0])
        except:
            cur.execute('INSERT OR IGNORE INTO KeywordCategory5 (Category) VALUES (?)', (KeywordCategory5,))
            cur.execute('SELECT Id FROM KeywordCategory5 WHERE Category = ? LIMIT 1', (KeywordCategory5,))
            Category4Id = int(cur.fetchone()[0])

        cur.execute('UPDATE KeywordsTable SET Category1Id = ?, Category2Id = ?, Category3Id = ?, Category4Id = ?, Category5Id = ? WHERE StatId = ?', (Category1Id, Category2Id, Category3Id, Category4Id, Category5Id, StatId,))

    con.commit()
    con.close()


print("DURN!")

end_time = dt.datetime.now().strftime("%H:%M:%S")
print("\nStart:\t", start_time)
print("End:\t", end_time)

# %%
