#%% architect_pace_add_keywords.py

import os
import time
import sqlite3
import requests
import numpy as np
import pandas as pd
import datetime as dt
import getstat

#   load client_list
client_list = pd.read_excel(r"C:\Users\JLee35\dentsu\Automation - Documents\General\STAT Ranks - Client List.xlsx")
client_list["STAT ID"] = client_list["STAT ID"].astype(int) # ensure that STAT ID is not a float
client_list["STAT ID"] = client_list["STAT ID"].astype(str) # ensure that STAT ID is str
print("Done!")

#%% filter client_list

folder_name = "Toolstation"
client_list = client_list[client_list["Folder Name"] == folder_name]

#%% add keywords

root = fr"C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\STAT\Clients"

all_categories = []

for i in client_list.index:
    folder_name = client_list["Folder Name"][i]
    client_name = client_list["Client Name"][i]
    stat_id = client_list["STAT ID"][i]

    database_name = getstat.dbize(folder_name)
    save_name = getstat.scrub(client_name)

    con = sqlite3.connect(os.path.join(root, folder_name, database_name))
    cur = con.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS KeywordsTable(
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
        );""")

    df = pd.read_csv(os.path.join(root,folder_name,"Setup",f"{save_name}_keywords_list.csv"))
    df = df.fillna("")

    for j in df.index:
        StatId = int(df["Id"][j])
        Keyword = str(df["Keyword"][j].strip())
        KeywordMarket = str(df["KeywordMarket"][j])
        KeywordLocation = str(df["KeywordLocation"][j])
        KeywordDevice = str(df["KeywordDevice"][j])
        try:
            CreatedAt = str(dt.datetime.strptime(df["CreatedAt"][j], "%d/%m/%Y").strftime("%Y-%m-%d")) # convert df["CreatedAt"][j] DD/MM/YYYY to YYY-MM-DD
        except ValueError:
            CreatedAt = df["CreatedAt"][j]
        GlobalSearchVolume = int(df["KeywordStats.GlobalSearchVolume"][j])
        try:
            RegionalSearchVolume = int(df["KeywordStats.RegionalSearchVolume"][j])
        except KeyError:
            RegionalSearchVolume = int(df["KeywordStats.TargetedSearchVolume"][j])
        Jan = int(df["KeywordStats.LocalSearchTrendsByMonth.Jan"][j])
        Feb = int(df["KeywordStats.LocalSearchTrendsByMonth.Feb"][j])
        Mar = int(df["KeywordStats.LocalSearchTrendsByMonth.Mar"][j])
        Apr = int(df["KeywordStats.LocalSearchTrendsByMonth.Apr"][j])
        May = int(df["KeywordStats.LocalSearchTrendsByMonth.May"][j])
        Jun = int(df["KeywordStats.LocalSearchTrendsByMonth.Jun"][j])
        Jul = int(df["KeywordStats.LocalSearchTrendsByMonth.Jul"][j])
        Aug = int(df["KeywordStats.LocalSearchTrendsByMonth.Aug"][j])
        Sep = int(df["KeywordStats.LocalSearchTrendsByMonth.Sep"][j])
        Oct = int(df["KeywordStats.LocalSearchTrendsByMonth.Oct"][j])
        Nov = int(df["KeywordStats.LocalSearchTrendsByMonth.Nov"][j])
        Dec = int(df["KeywordStats.LocalSearchTrendsByMonth.Dec"][j])
        KeywordCategory1 = str(df["Category1"][j]).strip()
        KeywordCategory2 = str(df["Category2"][j]).strip()
        KeywordCategory3 = str(df["Category3"][j]).strip()
        KeywordCategory4 = str(df["Category4"][j]).strip()
        KeywordCategory5 = str(df["Category5"][j]).strip()

        print(f"Adding {Keyword}...")

    # Market
        cur.execute("SELECT Id FROM KeywordMarket WHERE Market = ? LIMIT 1", (KeywordMarket,))
        try:
            MarketId = int(cur.fetchone()[0])
        except:
            cur.execute("INSERT OR IGNORE INTO KeywordMarket (Market) VALUES (?)", (KeywordMarket,))
            cur.execute("SELECT Id FROM KeywordMarket WHERE Market = ? LIMIT 1", (KeywordMarket,))
            MarketId = int(cur.fetchone()[0])

    # Device
        cur.execute("SELECT Id FROM KeywordDevice WHERE Device = ? LIMIT 1", (KeywordDevice,))
        try:
            DeviceId = int(cur.fetchone()[0])
        except:
            cur.execute("INSERT OR IGNORE INTO KeywordDevice (Device) VALUES (?)", (KeywordDevice,))
            cur.execute("SELECT Id FROM KeywordDevice WHERE Device = ? LIMIT 1", (KeywordDevice,))
            DeviceId = int(cur.fetchone()[0])

    # Location
        cur.execute("SELECT Id FROM KeywordLocation WHERE Location = ? LIMIT 1", (KeywordLocation,))
        try:
            LocationId = int(cur.fetchone()[0])
        except:
            cur.execute("INSERT OR IGNORE INTO KeywordLocation (Location) VALUES (?)", (KeywordLocation,))
            cur.execute("SELECT Id FROM KeywordLocation WHERE Location = ? LIMIT 1", (KeywordLocation,))
            LocationId = int(cur.fetchone()[0])

    # Category1
        cur.execute("SELECT Id FROM KeywordCategory1 WHERE Category = ? LIMIT 1", (KeywordCategory1,))
        try:
            Category1Id = int(cur.fetchone()[0])
        except:
            cur.execute("INSERT OR IGNORE INTO KeywordCategory1 (Category) VALUES (?)", (KeywordCategory1,))
            cur.execute("SELECT Id FROM KeywordCategory1 WHERE Category = ? LIMIT 1", (KeywordCategory1,))
            Category1Id = int(cur.fetchone()[0])

    # Category2
        cur.execute("SELECT Id FROM KeywordCategory2 WHERE Category = ? LIMIT 1", (KeywordCategory2,))
        try:
            Category2Id = int(cur.fetchone()[0])
        except:
            cur.execute("INSERT OR IGNORE INTO KeywordCategory2 (Category) VALUES (?)", (KeywordCategory2,))
            cur.execute("SELECT Id FROM KeywordCategory2 WHERE Category = ? LIMIT 1", (KeywordCategory2,))
            Category2Id = int(cur.fetchone()[0])

    # Category3
        cur.execute("SELECT Id FROM KeywordCategory3 WHERE Category = ? LIMIT 1", (KeywordCategory3,))
        try:
            Category3Id = int(cur.fetchone()[0])
        except:
            cur.execute("INSERT OR IGNORE INTO KeywordCategory3 (Category) VALUES (?)", (KeywordCategory3,))
            cur.execute("SELECT Id FROM KeywordCategory3 WHERE Category = ? LIMIT 1", (KeywordCategory3,))
            Category3Id = int(cur.fetchone()[0])

    # Category4
        cur.execute("SELECT Id FROM KeywordCategory4 WHERE Category = ? LIMIT 1", (KeywordCategory4,))
        try:
            Category4Id = int(cur.fetchone()[0])
        except:
            cur.execute("INSERT OR IGNORE INTO KeywordCategory4 (Category) VALUES (?)", (KeywordCategory4,))
            cur.execute("SELECT Id FROM KeywordCategory4 WHERE Category = ? LIMIT 1", (KeywordCategory4,))
            Category4Id = int(cur.fetchone()[0])

    # Category5
        cur.execute("SELECT Id FROM KeywordCategory5 WHERE Category = ? LIMIT 1", (KeywordCategory5,))
        try:
            Category5Id = int(cur.fetchone()[0])
        except:
            cur.execute("INSERT OR IGNORE INTO KeywordCategory5 (Category) VALUES (?)", (KeywordCategory5,))
            cur.execute("SELECT Id FROM KeywordCategory5 WHERE Category = ? LIMIT 1", (KeywordCategory5,))
            Category5Id = int(cur.fetchone()[0])

        cur.execute(
        """INSERT OR IGNORE INTO KeywordsTable
        (StatId, Keyword, MarketId, LocationId, DeviceId, CreatedAt, GlobalSearchVolume, RegionalSearchVolume, Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec, Category1Id,  Category2Id, Category3Id, Category4Id, Category5Id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?)""",
        (StatId, Keyword, MarketId, LocationId, DeviceId, CreatedAt, GlobalSearchVolume, RegionalSearchVolume, Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec, Category1Id,  Category2Id, Category3Id, Category4Id, Category5Id)
         )

    con.commit()
    con.close()

print("\nDURN!")

#%%
