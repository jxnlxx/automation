#%% architect_pace_export.py

import os
import time
import json
import sqlite3
import getstat
import requests
import openpyxl
import numpy as np
import pandas as pd
import datetime as dt
import xlsxwriter as xl

client_list = pd.read_excel(r"C:\Users\JLee35\dentsu\Automation - Documents\General\STAT Ranks - Client List.xlsx")
client_list["STAT ID"] = client_list["STAT ID"].astype(int) # ensure that STAT ID is not a float
client_list["STAT ID"] = client_list["STAT ID"].astype(str) # ensure that STAT ID is str
print("Done!")

#%%

folder_name = "The Ivy Restaurants"
client_list = client_list[client_list["Folder Name"] == folder_name]

#%%

client_list = client_list.head(20)
#%%

start_time = dt.datetime.now().strftime("%H:%M:%S")

root =fr"C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\STAT\Clients"

incomplete = [] # for list of skipped clients due to incomplete stat jobs

for i in client_list.index:
    folder_name = client_list["Folder Name"][i]
    client_name = client_list["Client Name"][i]
    stat_id = client_list["STAT ID"][i]

    database_name = getstat.dbize(folder_name)
    save_name = getstat.scrub(client_name)

    con = sqlite3.connect(os.path.join(root, folder_name, database_name))
    cur = con.cursor()

# retrieve reports
    print(f"Beginning {client_name}...")

    requests_df = pd.read_sql_query(f"SELECT * FROM Requests_{save_name} WHERE status = 0", con)
    try:
        final_job = requests_df["JobId"].iloc[-1]
    except IndexError:
        print(f"{client_name} is up-to-date!")
        print("Moving onto next client...")
        continue
    print(f"Checking {client_name}'s final job status...")
    status = getstat.job_status(final_job)
    current_time = dt.datetime.now().strftime("%H:%M")
    if status != "Completed":
        print(f"{current_time} - Final job status: {status}")
        print(f"Skipping {client_name}...")
        incomplete += [client_name]
        continue
    else:
        print(f"{current_time} - Final job status: {status}")
        pass

    print (f"Adding {client_name} ranks...")

    for j in requests_df.index:
        con = sqlite3.connect(os.path.join(root, folder_name, database_name))
        cur = con.cursor()

        all_kws = pd.read_sql_query(f"SELECT * FROM KeywordsTable", con)
        all_kws = all_kws["StatId"].astype(int).to_list()

        job_id = int(requests_df["JobId"][j])
        date = requests_df["Date"][j]

        print(f"\nBeginning job {j+1} of {len(requests_df)}")
        print(f"\nFetching {client_name} data for {date}")

        kw_df = getstat.export_ranks(job_id)

        print("Data received!")
        kw_df = pd.json_normalize(kw_df)
        kw_df["Id"] = kw_df["Id"].astype(int) # changes "Id" to int so filter works - StatId is stored in the db as int

        new_kws = kw_df[~kw_df["Id"].isin(all_kws)]
        new_kws = new_kws[~new_kws["KeywordStats.GlobalSearchVolume"].str.contains("N/A")] # if "GlobalSearchVolume is "N/A", the keywords aren"t being tracked

        if len(new_kws) > 0: # if new keywords, add them to database without tags
            print("Adding new keywords to database...")
            for x in new_kws.index:
                StatId = int(new_kws["Id"][x])
                Keyword = str(new_kws["Keyword"][x]).strip()
                KeywordMarket = str(new_kws["KeywordMarket"][x])
                KeywordLocation = str(new_kws["KeywordLocation"][x])
                KeywordDevice = str(new_kws["KeywordDevice"][x])
                try:
                    CreatedAt = str(dt.datetime.strptime(new_kws["CreatedAt"][x], "%d/%m/%Y").strftime("%Y-%m-%d")) # convert df["CreatedAt"][j] DD/MM/YYYY to YYY-MM-DD
                except ValueError:
                    CreatedAt = str(new_kws["CreatedAt"][x])
                GlobalSearchVolume = int(new_kws["KeywordStats.GlobalSearchVolume"][x])
                try:
                    RegionalSearchVolume = int(new_kws["KeywordStats.RegionalSearchVolume"][x])
                except KeyError:
                    RegionalSearchVolume = int(new_kws["KeywordStats.TargetedSearchVolume"][x])
                Jan = int(new_kws["KeywordStats.LocalSearchTrendsByMonth.Jan"][x])
                Feb = int(new_kws["KeywordStats.LocalSearchTrendsByMonth.Feb"][x])
                Mar = int(new_kws["KeywordStats.LocalSearchTrendsByMonth.Mar"][x])
                Apr = int(new_kws["KeywordStats.LocalSearchTrendsByMonth.Apr"][x])
                May = int(new_kws["KeywordStats.LocalSearchTrendsByMonth.May"][x])
                Jun = int(new_kws["KeywordStats.LocalSearchTrendsByMonth.Jun"][x])
                Jul = int(new_kws["KeywordStats.LocalSearchTrendsByMonth.Jul"][x])
                Aug = int(new_kws["KeywordStats.LocalSearchTrendsByMonth.Aug"][x])
                Sep = int(new_kws["KeywordStats.LocalSearchTrendsByMonth.Sep"][x])
                Oct = int(new_kws["KeywordStats.LocalSearchTrendsByMonth.Oct"][x])
                Nov = int(new_kws["KeywordStats.LocalSearchTrendsByMonth.Nov"][x])
                Dec = int(new_kws["KeywordStats.LocalSearchTrendsByMonth.Dec"][x])
                KeywordCategory1 = ""
                KeywordCategory2 = ""
                KeywordCategory3 = ""
                KeywordCategory4 = ""
                KeywordCategory5 = ""

                print(f"Adding {Keyword}...")

            # Market
                cur.execute("SELECT Id FROM KeywordMarket WHERE Market = ? LIMIT 1", (KeywordMarket,))
                try:
                    MarketId = int(cur.fetchone()[0])
                except TypeError:
                    cur.execute("INSERT OR IGNORE INTO KeywordMarket (Market) VALUES (?)", (KeywordMarket,))
                    cur.execute("SELECT Id FROM KeywordMarket WHERE Market = ? LIMIT 1", (KeywordMarket,))
                    MarketId = int(cur.fetchone()[0])

            # Device
                cur.execute("SELECT Id FROM KeywordDevice WHERE Device = ? LIMIT 1", (KeywordDevice,))
                try:
                    DeviceId = int(cur.fetchone()[0])
                except TypeError:
                    cur.execute("INSERT OR IGNORE INTO KeywordDevice (Device) VALUES (?)", (KeywordDevice,))
                    cur.execute("SELECT Id FROM KeywordDevice WHERE Device = ? LIMIT 1", (KeywordDevice,))
                    DeviceId = int(cur.fetchone()[0])

            # Location
                cur.execute("SELECT Id FROM KeywordLocation WHERE Location = ? LIMIT 1", (KeywordLocation,))
                try:
                    LocationId = int(cur.fetchone()[0])
                except TypeError:
                    cur.execute("INSERT OR IGNORE INTO KeywordLocation (Location) VALUES (?)", (KeywordLocation,))
                    cur.execute("SELECT Id FROM KeywordLocation WHERE Location = ? LIMIT 1", (KeywordLocation,))
                    LocationId = int(cur.fetchone()[0])

            # Category1
                cur.execute("SELECT Id FROM KeywordCategory1 WHERE Category = ? LIMIT 1", (KeywordCategory1,))
                try:
                    Category1Id = int(cur.fetchone()[0])
                except TypeError:
                    cur.execute("INSERT OR IGNORE INTO KeywordCategory1 (Category) VALUES (?)", (KeywordCategory1,))
                    cur.execute("SELECT Id FROM KeywordCategory1 WHERE Category = ? LIMIT 1", (KeywordCategory1,))
                    Category1Id = int(cur.fetchone()[0])

            # Category2
                cur.execute("SELECT Id FROM KeywordCategory2 WHERE Category = ? LIMIT 1", (KeywordCategory2,))
                try:
                    Category2Id = int(cur.fetchone()[0])
                except TypeError:
                    cur.execute("INSERT OR IGNORE INTO KeywordCategory2 (Category) VALUES (?)", (KeywordCategory2,))
                    cur.execute("SELECT Id FROM KeywordCategory2 WHERE Category = ? LIMIT 1", (KeywordCategory2,))
                    Category2Id = int(cur.fetchone()[0])

            # Category3
                cur.execute("SELECT Id FROM KeywordCategory3 WHERE Category = ? LIMIT 1", (KeywordCategory3,))
                try:
                    Category3Id = int(cur.fetchone()[0])
                except TypeError:
                    cur.execute("INSERT OR IGNORE INTO KeywordCategory3 (Category) VALUES (?)", (KeywordCategory3,))
                    cur.execute("SELECT Id FROM KeywordCategory3 WHERE Category = ? LIMIT 1", (KeywordCategory3,))
                    Category3Id = int(cur.fetchone()[0])

            # Category4
                cur.execute("SELECT Id FROM KeywordCategory4 WHERE Category = ? LIMIT 1", (KeywordCategory4,))
                try:
                    Category4Id = int(cur.fetchone()[0])
                except TypeError:
                    cur.execute("INSERT OR IGNORE INTO KeywordCategory4 (Category) VALUES (?)", (KeywordCategory4,))
                    cur.execute("SELECT Id FROM KeywordCategory4 WHERE Category = ? LIMIT 1", (KeywordCategory4,))
                    Category4Id = int(cur.fetchone()[0])

            # Category5
                cur.execute("SELECT Id FROM KeywordCategory5 WHERE Category = ? LIMIT 1", (KeywordCategory5,))
                try:
                    Category5Id = int(cur.fetchone()[0])
                except TypeError:
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

        else: # if no new keywords, pass
            pass

        all_kws = pd.read_sql_query(f"SELECT * FROM KeywordsTable", con)
        all_kws = all_kws["StatId"].astype(int).to_list()

        print("Filtering data...")

        kw_df = kw_df[kw_df["Id"].isin(all_kws)]
        kw_df = kw_df[["Id","Ranking.date","Ranking.Google.Rank","Ranking.Google.BaseRank","Ranking.Google.Url"]]
        kw_df["Ranking.Google.Url"] = kw_df["Ranking.Google.Url"].replace("N/A","")
        kw_df["Ranking.Google.Url"] = kw_df["Ranking.Google.Url"].fillna("")
        kw_df[["Ranking.Google.Rank","Ranking.Google.BaseRank"]] = kw_df[["Ranking.Google.Rank","Ranking.Google.BaseRank"]].replace("N/A", int("120"))
        kw_df[["Ranking.Google.Rank","Ranking.Google.BaseRank"]] = kw_df[["Ranking.Google.Rank","Ranking.Google.BaseRank"]].fillna(int("120"))
        kw_df = kw_df.reset_index(drop=True)

        print(f"\nAdding keywords...")

#   add keywords to db
        for k in kw_df.index:
            if (k+1) % 500 == 0:
                print(f"Completed {k+1} of {len(kw_df)}")
            StatId = int(kw_df["Id"][k])
            Date = str(kw_df["Ranking.date"][k])
            Rank = int(kw_df["Ranking.Google.Rank"][k])
            BaseRank = int(kw_df["Ranking.Google.BaseRank"][k])
            RankingUrl = str(kw_df["Ranking.Google.Url"][k])

            cur.execute("SELECT Id FROM KeywordsTable WHERE StatId = ? LIMIT 1", (StatId,))
            KeywordId = int(cur.fetchone()[0])

            cur.execute("SELECT Id FROM RankingUrl WHERE Url = ? LIMIT 1", (RankingUrl,))
            try:
                RankingUrlId = int(cur.fetchone()[0])
            except TypeError:
                cur.execute("INSERT OR IGNORE INTO RankingUrl (Url) VALUES (?)", (RankingUrl,))
                cur.execute("SELECT Id FROM RankingUrl WHERE Url = ? LIMIT 1", (RankingUrl,))
                RankingUrlId = int(cur.fetchone()[0])

            cur.execute(f"""INSERT OR IGNORE INTO Ranks_{save_name}(Date, KeywordId, Rank, BaseRank, RankingUrlId)
                        VALUES (?, ?, ?, ?, ?)""", (Date, KeywordId, Rank, BaseRank, RankingUrlId,))

            cur.execute(f"UPDATE requests_{save_name} SET Status = ? WHERE JobId = ?", (1, job_id,))

        print(f"Completed {len(kw_df)} of {len(kw_df)}")
        print(f"\nSaving {date}...")
        con.commit()
        con.close()
        print(f"Done!")
        print("\n---------------------")

        time.sleep(1)
if len(incomplete) > 0:
    print("These clients were skipped:")
    for i in incomplete:
        print(i)
else:
    print("DURN!")
end_time = dt.datetime.now().strftime("%H:%M:%S")
print("\nStart:\t", start_time)
print("End:\t", end_time)

#%%
