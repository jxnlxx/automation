#%% stat_RanksDaily_weekly.py

import os
import time
import json
import sqlite3
import datetime as dt
import requests
import openpyxl
import numpy as np
import pandas as pd
import xlsxwriter as xl
import getstat

import gspread
import gspread_formatting
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery

# load client_list

client_list = pd.read_excel(r"C:\Users\JLee35\dentsu\Automation - Documents\General\STAT Ranks - Client List.xlsx")
client_list["STAT ID"] = client_list["STAT ID"].astype(int) # ensure that STAT ID is not a float
client_list["STAT ID"] = client_list["STAT ID"].astype(str) # ensure that STAT ID is str

client_list = client_list[client_list["Weekly Report"] != "Inactive"]

print("Client list loaded!")

#%% filter by client name

client_name = "musicMagpie Store"
client_list = client_list[client_list["Client Name"] == client_name]

#%% filter by client folder

folder_name = "Entertainment Magpie"
client_list = client_list[client_list["Folder Name"] == folder_name]

#%% amalgamate ranks by week and set date as 1st day of week

root = fr"C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\STAT\Clients"

for i in client_list.index:
    folder_name = client_list["Folder Name"][i]
    client_name = client_list["Client Name"][i]
    stat_id = client_list["STAT ID"][i]
    week_start = client_list["Weekly Report"][i]

    database_name = getstat.dbize(folder_name)
    save_name = getstat.scrub(client_name)

    con = sqlite3.connect(os.path.join(root, folder_name, database_name))
    cur = con.cursor()

    print(f"Starting {client_name}...")

    cur.execute(f'''CREATE TABLE IF NOT EXISTS RanksWeekly_{save_name}(
        Date TEXT NOT NULL,
        KeywordId INTEGER NOT NULL,
        Rank INTEGER NOT NULL,
        BaseRank INTEGER NOT NULL,
        RankingUrlId INTEGER NOT NULL,
        FOREIGN KEY (KeywordId) REFERENCES KeywordsTable(Id),
        FOREIGN KEY (RankingUrlId) REFERENCES RankingUrl (Id),
        UNIQUE (KeywordId, Date)
        );''')

    try:
#   get start_date from visibility table
        cur.execute(f"SELECT date FROM RanksWeekly_{save_name} ORDER BY date DESC LIMIT 1;")
        start_date = str(cur.fetchone()[0])
        start_date = dt.datetime.strptime(start_date, "%Y-%m-%d").date() #  convert start_date string to date object
        start_date = start_date + dt.timedelta(days=7) # add 1 day to start_date
#   get end_date from requests table
        cur.execute(f"SELECT date FROM Requests_{save_name} ORDER BY date DESC LIMIT 1;")
        end_date = str(cur.fetchone()[0])
        end_date = dt.datetime.strptime(end_date, "%Y-%m-%d").date() #  convert start_date string to date object
    except TypeError:
#   get start_date from requests table
        cur.execute(f"SELECT date FROM Requests_{save_name} ORDER BY date ASC LIMIT 1;")
        start_date = str(cur.fetchone()[0])
        start_date = dt.datetime.strptime(start_date, "%Y-%m-%d").date() #  convert start_date string to date object
#   get end_date from requests table
        cur.execute(f"SELECT date FROM Requests_{save_name} ORDER BY date DESC LIMIT 1;")
        end_date = str(cur.fetchone()[0])
        end_date = dt.datetime.strptime(end_date, "%Y-%m-%d").date() #  convert start_date string to date object

    con.commit()
    con.close()

    if (end_date - start_date).days >= 6:
        print(f"Beginning RanksWeekly_{save_name}...")
        pass
    else:
        print(f"{client_name} up-to-date!\n")
        continue

#   aggregate ranks by week
    counter =0
    num_weeks = int(np.floor((end_date - start_date).days /7))
    if week_start == "Monday":
        for j in getstat.mon_weekspan(start_date,end_date):
            con = sqlite3.connect(os.path.join(root, folder_name, database_name))
            cur = con.cursor()

            sDate = str(j.strftime("%Y-%m-%d"))
            eDate = str((j + dt.timedelta(days=6)).strftime("%Y-%m-%d"))

            counter += 1
            print(f"{client_name} - {counter} of {num_weeks}")
            print(f'Aggregating {sDate} - {eDate}')
            sql = f"""
                SELECT RanksDaily_{save_name}.Date, KeywordsTable.StatId, RanksDaily_{save_name}.Rank as Rank, RanksDaily_{save_name}.BaseRank as BaseRank, RankingUrl.Url as RankingUrl
                FROM RanksDaily_{save_name}
                JOIN KeywordsTable ON RanksDaily_{save_name}.KeywordId = KeywordsTable.Id
                JOIN RankingUrl ON RanksDaily_{save_name}.RankingUrlId = RankingUrl.Id
                WHERE date BETWEEN :sDate AND :eDate;
                """
            params = {"sDate":sDate, "eDate":eDate}
            df = pd.read_sql(sql, params=params, con=con)

            week_df = df.drop_duplicates(subset=["StatId"], keep="first")
            week_df = week_df.drop(["Rank","BaseRank","RankingUrl"], axis=1)
#   get average Rank
            temp =  df.groupby("StatId", as_index=False)["Rank"].mean().round().astype(int)
            week_df = week_df.merge(temp,on="StatId",how="left")
#   get average BaseRank
            temp =  df.groupby("StatId", as_index=False)["BaseRank"].mean().round().astype(int)
            week_df = week_df.merge(temp,on="StatId",how="left")
#   get top RankingUrl
            temp = df.sort_values("BaseRank", ascending=True).drop_duplicates("StatId").sort_index().reset_index(drop=True)
            week_df = pd.merge(week_df, temp[["StatId","RankingUrl"]], on="StatId", how="left")
# insert ranksweekly_df into RanksDaily_{save_name}

            print(f"Adding keywords...")

            for k in week_df.index:
                Date = str(week_df["Date"][k])
                StatId = int(week_df["StatId"][k])
                Rank = int(week_df["Rank"][k])
                BaseRank = int(week_df["BaseRank"][k])
                RankingUrl = str(week_df["RankingUrl"][k])

                cur.execute("SELECT Id FROM KeywordsTable WHERE StatId = ? LIMIT 1", (StatId,))
                KeywordId = int(cur.fetchone()[0])

                cur.execute("SELECT Id FROM RankingUrl WHERE Url = ? LIMIT 1", (RankingUrl,))
                RankingUrlId = int(cur.fetchone()[0])

                cur.execute(f"""INSERT OR IGNORE INTO RanksWeekly_{save_name}(Date, KeywordId, Rank, BaseRank, RankingUrlId)
                    VALUES (?, ?, ?, ?, ?)""", (Date, KeywordId, Rank, BaseRank, RankingUrlId,))

                if k % 500 == 0:
                    print(f"Completed {k+1} of {len(week_df)}")
            print(f"Completed {len(week_df)} of {len(week_df)}")
            print(f"Saving {sDate} - {eDate}...")
            con.commit()
            con.close()
            print(f"Done!")
            print("\n---------------------\n")
            time.sleep(2)

    if week_start == "Sunday":
        for j in getstat.sun_weekspan(start_date,end_date):
            con = sqlite3.connect(os.path.join(root, folder_name, database_name))
            cur = con.cursor()

            sDate = str(j.strftime("%Y-%m-%d"))
            eDate = str((j + dt.timedelta(days=6)).strftime("%Y-%m-%d"))

            counter += 1
            print(f"{client_name} - {counter} of {num_weeks}")
            print(f'Aggregating {sDate} - {eDate}')
            sql = f"""
                SELECT RanksDaily_{save_name}.Date, KeywordsTable.StatId, RanksDaily_{save_name}.Rank as Rank, RanksDaily_{save_name}.BaseRank as BaseRank, RankingUrl.Url as RankingUrl
                FROM RanksDaily_{save_name}
                JOIN KeywordsTable ON RanksDaily_{save_name}.KeywordId = KeywordsTable.Id
                JOIN RankingUrl ON RanksDaily_{save_name}.RankingUrlId = RankingUrl.Id
                WHERE date BETWEEN :sDate AND :eDate;
                """
            params = {"sDate":sDate, "eDate":eDate}
            df = pd.read_sql(sql, params=params, con=con)

            week_df = df.drop_duplicates(subset=["StatId"], keep="first")
            week_df = week_df.drop(["Rank","BaseRank","RankingUrl"], axis=1)
#   get average Rank
            temp =  df.groupby("StatId", as_index=False)["Rank"].mean().round().astype(int)
            week_df = week_df.merge(temp,on="StatId",how="left")
#   get average BaseRank
            temp =  df.groupby("StatId", as_index=False)["BaseRank"].mean().round().astype(int)
            week_df = week_df.merge(temp,on="StatId",how="left")
#   get top RankingUrl
            temp = df.sort_values("BaseRank", ascending=True).drop_duplicates("StatId").sort_index().reset_index(drop=True)
            week_df = pd.merge(week_df, temp[["StatId","RankingUrl"]], on="StatId", how="left")
# insert ranksweekly_df into RanksDaily_{save_name}

            print(f"Adding keywords...")

            for k in week_df.index:
                Date = str(week_df["Date"][k])
                StatId = int(week_df["StatId"][k])
                Rank = int(week_df["Rank"][k])
                BaseRank = int(week_df["BaseRank"][k])
                RankingUrl = str(week_df["RankingUrl"][k])

                cur.execute("SELECT Id FROM KeywordsTable WHERE StatId = ? LIMIT 1", (StatId,))
                KeywordId = int(cur.fetchone()[0])

                cur.execute("SELECT Id FROM RankingUrl WHERE Url = ? LIMIT 1", (RankingUrl,))
                RankingUrlId = int(cur.fetchone()[0])

                cur.execute(f"""INSERT OR IGNORE INTO RanksWeekly_{save_name}(Date, KeywordId, Rank, BaseRank, RankingUrlId)
                    VALUES (?, ?, ?, ?, ?)""", (Date, KeywordId, Rank, BaseRank, RankingUrlId,))

                if k % 500 == 0:
                    print(f"Completed {k+1} of {len(week_df)}")

            print(f"Completed {len(week_df)} of {len(week_df)}")
            print(f"Saving {sDate} - {eDate}...")
            con.commit()
            con.close()
            print(f"Done!")
            print("\n---------------------\n")
            time.sleep(2)

        con.close()

#%% upload RanksWeekly to gsheets

for i in client_list.index:
    folder_name = client_list["Folder Name"][i]
    client_name = client_list["Client Name"][i]
    stat_id = client_list["STAT ID"][i]
    gspread_id = client_list["GSheet ID"][i]

    database_name = getstat.dbize(folder_name)
    save_name = getstat.scrub(client_name)

    con = sqlite3.connect(os.path.join(root, folder_name, database_name))
    cur = con.cursor()

    cutoff = (dt.date.today() - dt.timedelta(days=40)).isoformat()
    sql = f"""
        SELECT RanksWeekly_{save_name}.Date,
        KeywordsTable.Keyword,
        KeywordsTable.RegionalSearchVolume as "Search Volume",
        KeywordDevice.Device,
        RanksWeekly_{save_name}.BaseRank as Rank,
        RankingUrl.Url as "Ranking Url",
        KeywordCategory1.Category as Category1,
        KeywordCategory2.Category as Category2
        FROM RanksWeekly_{save_name}
        JOIN KeywordsTable ON RanksWeekly_{save_name}.KeywordId = KeywordsTable.Id
        JOIN KeywordDevice ON KeywordsTable.DeviceId = KeywordDevice.Id
        JOIN RankingUrl ON RanksWeekly_{save_name}.RankingUrlId = RankingUrl.Id
        JOIN KeywordCategory1 ON KeywordsTable.Category1Id = KeywordCategory1.Id
        JOIN KeywordCategory2 ON KeywordsTable.Category2Id = KeywordCategory2.Id
        WHERE date(Date) >= :date;"""

    params={"date":cutoff}
    df = pd.read_sql(sql, params=params, con=con)

    df2 = df.copy()
    df3 = df[~df["Category2"].isin([""])]

    for j in df3.index:
        df2.at[j,"Category1"] = ""

    df2["Category"] = df2["Category1"] + df2["Category2"]
    df = df2.drop(columns=["Category1","Category2"])
    df = df[df["Category"] != ""]

#   format ranksweekly_df
    df["Product"] = df["Rank"] * df["Search Volume"]

#   upload ranksweekly_df to gsheets
    gspread_id = client_list["GSheet ID"][i]
    gsheet_name = f"{client_name} - Weekly Ranks"
    print(f"\nRetrieving '{gsheet_name}' table from Google Sheets...")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    google_auth = r"C:\Users\JLee35\OneDrive - Dentsu Aegis Network\PROJECTS\Python\APIs\keys\iprospectseonorth\google_auth.json"
    creds = ServiceAccountCredentials.from_json_keyfile_name(google_auth, scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_key(gspread_id)
    try:
        worksheet = sheet.worksheet(gsheet_name)
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound as err:
        worksheet = sheet.add_worksheet(title=gsheet_name, rows=1, cols=1)

    set_with_dataframe(worksheet, df)

    print(f"{client_name} complete!")
    con.close()

print("\nDURN!")

# %%
