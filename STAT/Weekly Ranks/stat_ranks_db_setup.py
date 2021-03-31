#%% stat_weekly_ranks_db_setup.py

import os
import sqlite3
import getstat
import pandas as pd

client_list = pd.read_excel(r'C:\Users\JLee35\dentsu\Automation - Documents\General\STAT Ranks - Client List.xlsx')
client_list['STAT ID'] = client_list['STAT ID'].astype(int) # ensure that STAT ID is not a float
client_list['STAT ID'] = client_list['STAT ID'].astype(str) # ensure that STAT ID is str

print('Done!')
#%%

folder_name = 'The Ivy Restaurants'
client_list = client_list[client_list['Folder Name'] == folder_name].reset_index(drop=True)

#%%

root = fr"C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\STAT\Clients"

for i in client_list.index:
    folder_name = client_list["Folder Name"][i]
    client_name = client_list["Client Name"][i]
    stat_id = client_list["STAT ID"][i]

    database_name = getstat.dbize(folder_name)
    save_name = getstat.scrub(client_name)


    if not os.path.exists(os.path.join(root, folder_name)):
        os.mkdir(os.path.join(root, folder_name))

    con = sqlite3.connect(os.path.join(root, folder_name, database_name))
    cur = con.cursor()

    cur.execute('''CREATE TABLE IF NOT EXISTS KeywordDevice(
        Id INTEGER PRIMARY KEY,
        Device TEXT NOT NULL,
        UNIQUE (Device)
        );''')
    cur.execute('''INSERT OR IGNORE INTO KeywordDevice(Device) VALUES ('')''')

    cur.execute('''CREATE TABLE IF NOT EXISTS KeywordMarket(
        Id INTEGER PRIMARY KEY,
        Market TEXT NOT NULL,
        UNIQUE (Market)
        );''')
    cur.execute('''INSERT OR IGNORE INTO KeywordMarket(Market) VALUES ('')''')

    cur.execute('''CREATE TABLE IF NOT EXISTS KeywordLocation(
        Id INTEGER PRIMARY KEY,
        Location TEXT NOT NULL,
        UNIQUE (Location)
        );''')
    cur.execute('''INSERT OR IGNORE INTO KeywordLocation(Location) VALUES ('')''')

    cur.execute('''CREATE TABLE IF NOT EXISTS KeywordCategory1(
        Id INTEGER PRIMARY KEY,
        Category TEXT NOT NULL,
        UNIQUE (Category)
        );''')
    cur.execute('''INSERT OR IGNORE INTO KeywordCategory1(Category) VALUES ('')''')

    cur.execute('''CREATE TABLE IF NOT EXISTS KeywordCategory2(
        Id INTEGER PRIMARY KEY,
        Category TEXT NOT NULL,
        UNIQUE (Category)
        );''')
    cur.execute('''INSERT OR IGNORE INTO KeywordCategory2(Category) VALUES ('')''')

    cur.execute('''CREATE TABLE IF NOT EXISTS KeywordCategory3(
        Id INTEGER PRIMARY KEY,
        Category TEXT NOT NULL,
        UNIQUE (Category)
        );''')
    cur.execute('''INSERT OR IGNORE INTO KeywordCategory3(Category) VALUES ('')''')

    cur.execute('''CREATE TABLE IF NOT EXISTS KeywordCategory4(
        Id INTEGER PRIMARY KEY,
        Category TEXT NOT NULL,
        UNIQUE (Category)
        );''')
    cur.execute('''INSERT OR IGNORE INTO KeywordCategory4(Category) VALUES ('')''')

    cur.execute('''CREATE TABLE IF NOT EXISTS KeywordCategory5(
        Id INTEGER PRIMARY KEY,
        Category TEXT NOT NULL,
        UNIQUE (Category)
        );''')
    cur.execute('''INSERT OR IGNORE INTO KeywordCategory5(Category) VALUES ('')''')

    cur.execute('''CREATE TABLE IF NOT EXISTS RankingUrl(
        Id INTEGER PRIMARY KEY,
        Url TEXT NOT NULL,
        UNIQUE (Url)
        );''')
    cur.execute('''INSERT OR IGNORE INTO RankingUrl(Url) VALUES ('')''')

    cur.execute('''CREATE TABLE IF NOT EXISTS KeywordsTable(
        Id INTEGER PRIMARY KEY,
        StatId INTEGER NOT NULL,
        Keyword TEXT NOT NULL,
        MarketId INTEGER NOT NULL,
        LocationId INTEGER NOT NULL,
        DeviceId INTEGER NOT NULL,
        CreatedAt TEXT NOT NULL,
        GlobalSearchVolume INTEGER,
        RegionalSearchVolume INTEGER,
        Jan INTEGER NOT NULL,
        Feb INTEGER NOT NULL,
        Mar INTEGER NOT NULL,
        Apr INTEGER NOT NULL,
        May INTEGER NOT NULL,
        Jun INTEGER NOT NULL,
        Jul INTEGER NOT NULL,
        Aug INTEGER NOT NULL,
        Sep INTEGER NOT NULL,
        Oct INTEGER NOT NULL,
        Nov INTEGER NOT NULL,
        Dec INTEGER NOT NULL,
        Category1Id INTEGER,
        Category2Id INTEGER,
        Category3Id INTEGER,
        Category4Id INTEGER,
        Category5Id INTEGER,
        FOREIGN KEY (MarketId) REFERENCES KeywordMarket (Id),
        FOREIGN KEY (DeviceId) REFERENCES KeywordDevice (Id),
        FOREIGN KEY (LocationId) REFERENCES KeywordLocation (Id),
        FOREIGN KEY (Category1Id) REFERENCES KeywordCategory1 (Id),
        FOREIGN KEY (Category2Id) REFERENCES KeywordCategory2 (Id),
        FOREIGN KEY (Category3Id) REFERENCES KeywordCategory3 (Id),
        FOREIGN KEY (Category4Id) REFERENCES KeywordCategory4 (Id),
        FOREIGN KEY (Category5Id) REFERENCES KeywordCategory5 (Id),
        UNIQUE (StatID)
        );''')

    cur.execute(f'''CREATE TABLE IF NOT EXISTS Requests_{save_name}(
        Date TEXT NOT NULL,
        JobId INTEGER NOT NULL,
        Status INTEGER NOT NULL,
        UNIQUE (Date)
        );''')

    cur.execute(f'''CREATE TABLE IF NOT EXISTS RanksDaily_{save_name}(
        Date TEXT NOT NULL,
        KeywordId INTEGER NOT NULL,
        Rank INTEGER NOT NULL,
        BaseRank INTEGER NOT NULL,
        RankingUrlId INTEGER NOT NULL,
        FOREIGN KEY (KeywordId) REFERENCES KeywordsTable(Id),
        FOREIGN KEY (RankingUrlId) REFERENCES RankingUrl (Id),
        UNIQUE (KeywordId, Date)
        );''')

    con.commit()
    con.close()

#%%
