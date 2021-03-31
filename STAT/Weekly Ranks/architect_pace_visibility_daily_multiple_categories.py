#%% architect_pace_VisibilityDaily_daily_multiple_categories.py

import os
import time
import sqlite3
import getstat
import requests
import numpy as np
import pandas as pd
import datetime as dt

import gspread
import gspread_formatting
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery

client_list = pd.read_excel(r"C:\Users\JLee35\dentsu\Automation - Documents\General\STAT Ranks - Client List.xlsx")
client_list["STAT ID"] = client_list["STAT ID"].astype(int) # ensure that STAT ID is not a float
client_list["STAT ID"] = client_list["STAT ID"].astype(str) # ensure that STAT ID is str

#   filter for only sites with databases set as "Complete"
client_list = client_list[client_list["Architect Pace"] == "Active"]

print("Client list loaded!")

#%% transform data

multiple_categories = ["JD Williams"]

root = fr"C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\STAT\Clients"

for i in client_list.index:
    folder_name = client_list["Folder Name"][i]
    client_name = client_list["Client Name"][i]
    stat_id = client_list["STAT ID"][i]

    database_name = getstat.dbize(folder_name)
    save_name = getstat.scrub(client_name)

    con = sqlite3.connect(os.path.join(root, folder_name, database_name))
    cur = con.cursor()

    print(f"Starting {client_name}...")

    cur.execute(f"""CREATE TABLE IF NOT EXISTS Criteria(
        Id INTEGER PRIMARY KEY,
        Criterion TEXT,
        UNIQUE (Criterion)
        );""")
    cur.execute("INSERT OR IGNORE INTO Criteria (Criterion) VALUES (?)", ("",))

    cur.execute(f"""CREATE TABLE IF NOT EXISTS VisibilityDaily_{save_name}(
        Date TEXT NOT NULL,
        DeviceId INTEGER NOT NULL,
        CriterionId INTEGER NOT NULL,
        KeywordCategory1Id INTEGER,
        KeywordCategory2Id INTEGER,
        KeywordCategory3Id INTEGER,
        KeywordCategory4Id INTEGER,
        KeywordCategory5Id INTEGER,
        Score INTEGER NOT NULL,
        FOREIGN KEY (DeviceId) REFERENCES keywords (Id),
        FOREIGN KEY (CriterionId) REFERENCES Criterion (Id),
        FOREIGN KEY (KeywordCategory1Id) REFERENCES KeywordCategory1 (Id),
        FOREIGN KEY (KeywordCategory2Id) REFERENCES KeywordCategory2 (Id),
        FOREIGN KEY (KeywordCategory3Id) REFERENCES KeywordCategory3 (Id),
        FOREIGN KEY (KeywordCategory4Id) REFERENCES KeywordCategory4 (Id),
        FOREIGN KEY (KeywordCategory5Id) REFERENCES KeywordCategory5 (Id),
        UNIQUE (Date, DeviceId, CriterionId, KeywordCategory1Id, KeywordCategory2Id)
        );""")

    try:
#   get start_date from visibility table
        cur.execute(f"SELECT date FROM VisibilityDaily_{save_name} ORDER BY date DESC LIMIT 1;")
        start_date = str(cur.fetchone()[0])
        start_date = dt.datetime.strptime(start_date, "%Y-%m-%d").date() #  convert start_date string to date object
        start_date = start_date + dt.timedelta(days=1) # add 1 day to start_date
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

    print(f"Beginning from {start_date}")
    counter = 1
    for j in pd.date_range(start_date, end_date):
        con = sqlite3.connect(os.path.join(root, folder_name, database_name))
        cur = con.cursor()

        month = j.strftime("%b")
        date = str(j.strftime("%Y-%m-%d"))

        print(f"\n{client_name} - job {counter} of {len(pd.date_range(start_date, end_date))}")
        counter += 1

        print(f"Starting {date}...")

        sql = f"""
            SELECT Ranks_{save_name}.Date, KeywordsTable.Keyword, KeywordsTable.RegionalSearchVolume, Ranks_{save_name}.BaseRank as Rank, KeywordDevice.Device, KeywordCategory1.Category as Category1, KeywordCategory2.Category as Category2, Ctr_{save_name}.CTR
            FROM Ranks_{save_name}
            JOIN KeywordsTable ON Ranks_{save_name}.KeywordId = KeywordsTable.Id
            JOIN KeywordDevice ON KeywordsTable.DeviceId = KeywordDevice.Id
            JOIN KeywordCategory1 ON KeywordsTable.Category1Id = KeywordCategory1.Id
            JOIN KeywordCategory2 ON KeywordsTable.Category2Id = KeywordCategory2.Id
            JOIN Ctr_{save_name} ON Ranks_{save_name}.BaseRank = Ctr_{save_name}.Position
            WHERE date = :date;
            """
        params = {"date":date}
        df = pd.read_sql(sql, params=params, con=con)

        print("Data retrieved!")

#   aggreagate
        print("Aggregating data...")
        df["DailySearchVolume"] = (df["RegionalSearchVolume"].astype(int)/30).astype(int)
        df["WeightedRank"] = (df["DailySearchVolume"].astype(int) * df["Rank"].astype(int)).apply(np.floor).astype(int)
        df["CtrScore"] = round(df["CTR"] * df["DailySearchVolume"],0).apply(np.floor).astype(int)

        visibility = pd.DataFrame(columns=["Date", "Device", "Criteria", "Category1", "Category2", "Score"])

        device_list = list(dict.fromkeys(df["Device"].to_list()))
        category1_list = list(dict.fromkeys(df["Category1"].to_list()))
        category2_list = list(dict.fromkeys(df["Category2"].to_list()))

        try:
            category1_list.remove("")
        except ValueError:
            pass
        try:
            category2_list.remove("")
        except ValueError:
            pass

        rank_brackets = ["#1", "#2 - #5", "#6 - #10", "#11 - #20", "#21 - #30", "#31 - #40", "#41 - #50"]

        for i in device_list:
            device = df[df["Device"] == i]
            k = "All Keywords"

            awr = round(device["WeightedRank"].sum() / device["DailySearchVolume"].sum(),0)
            temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":"Average Weighted Rank","Category1":k,"Category2":"","Score":awr},index=[0])
            visibility = visibility.append(temp)

            vis = device["CtrScore"].sum()
            temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":"Visibility Score","Category1":k,"Category2":"","Score":vis},index=[0])
            visibility = visibility.append(temp)

            for j in rank_brackets:
                if j == "#1":
                    score = device[device["Rank"] == 1].shape[0]
                    temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":j,"Category1":k,"Category2":"","Score":score},index=[0])
                    visibility = visibility.append(temp)
                elif j == "#2 - #5":
                    score = device[(device["Rank"] >= 2) & (device["Rank"] <= 5)].shape[0]
                    temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":j,"Category1":k,"Category2":"","Score":score},index=[0])
                    visibility = visibility.append(temp)
                elif j == "#6 - #10":
                    score = device[(device["Rank"] >= 6) & (device["Rank"] <= 10)].shape[0]
                    temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":j,"Category1":k,"Category2":"","Score":score},index=[0])
                    visibility = visibility.append(temp)
                elif j == "#11 - #20":
                    score = device[(device["Rank"] >= 11) & (device["Rank"] <= 20)].shape[0]
                    temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":j,"Category1":k,"Category2":"","Score":score},index=[0])
                    visibility = visibility.append(temp)
                elif j == "#21 - #30":
                    score = device[(device["Rank"] >= 21) & (device["Rank"] <= 30)].shape[0]
                    temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":j,"Category1":k,"Category2":"","Score":score},index=[0])
                    visibility = visibility.append(temp)
                elif j == "#31 - #40":
                    score = device[(device["Rank"] >= 31) & (device["Rank"] <= 40)].shape[0]
                    temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":j,"Category1":k,"Category2":"","Score":score},index=[0])
                    visibility = visibility.append(temp)
                elif j == "#41 - #50":
                    score = device[(device["Rank"] >= 41) & (device["Rank"] <= 50)].shape[0]
                    temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":j,"Category1":k,"Category2":"","Score":score},index=[0])
                    visibility = visibility.append(temp)

            for k in category1_list:
                category = device[device["Category1"] == k]

#                category.to_csv(fr"C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\{k}.csv", index=False)

                awr = round(category["WeightedRank"].sum() / category["DailySearchVolume"].sum(),0)
                temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":"Average Weighted Rank","Category1":k,"Category2":"","Score":awr},index=[0])
                visibility = visibility.append(temp)

                vis = category["CtrScore"].sum()
                temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":"Visibility Score","Category1":k,"Category2":"","Score":vis},index=[0])
                visibility = visibility.append(temp)

                for j in rank_brackets:
                    if j == "#1":
                        score = category[category["Rank"]==1].shape[0]
                        temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":j,"Category1":k,"Category2":"","Score":score},index=[0])
                        visibility = visibility.append(temp)
                    elif j == "#2 - #5":
                        score = category[(category["Rank"] >= 2) & (category["Rank"] <= 5)].shape[0]
                        temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":j,"Category1":k,"Category2":"","Score":score},index=[0])
                        visibility = visibility.append(temp)
                    elif j == "#6 - #10":
                        score = category[(category["Rank"] >= 6) & (category["Rank"] <= 10)].shape[0]
                        temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":j,"Category1":k,"Category2":"","Score":score},index=[0])
                        visibility = visibility.append(temp)
                    elif j == "#11 - #20":
                        score = category[(category["Rank"] >= 11) & (category["Rank"] <= 20)].shape[0]
                        temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":j,"Category1":k,"Category2":"","Score":score},index=[0])
                        visibility = visibility.append(temp)
                    elif j == "#21 - #30":
                        score = category[(category["Rank"] >= 21) & (category["Rank"] <= 30)].shape[0]
                        temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":j,"Category1":k,"Category2":"","Score":score},index=[0])
                        visibility = visibility.append(temp)
                    elif j == "#31 - #40":
                        score = category[(category["Rank"] >= 31) & (category["Rank"] <= 40)].shape[0]
                        temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":j,"Category1":k,"Category2":"","Score":score},index=[0])
                        visibility = visibility.append(temp)
                    elif j == "#41 - #50":
                        score = category[(category["Rank"] >= 41) & (category["Rank"] <= 50)].shape[0]
                        temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":j,"Category1":k,"Category2":"","Score":score},index=[0])
                        visibility = visibility.append(temp)

            if len(category2_list) > 0:
                k = ""
                for l in category2_list:
                    category = device[device["Category2"] == l]
#                    category.to_csv(fr"C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\{k}.csv", index=False)

                    awr = round(category["WeightedRank"].sum() / category["DailySearchVolume"].sum(),0)
                    temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":"Average Weighted Rank","Category1":k,"Category2":l,"Score":awr},index=[0])
                    visibility = visibility.append(temp)

                    vis = category["CtrScore"].sum()
                    temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":"Visibility Score","Category1":k,"Category2":l,"Score":vis},index=[0])
                    visibility = visibility.append(temp)

                    for j in rank_brackets:
                        if j == "#1":
                            score = category[category["Rank"]==1].shape[0]
                            temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":j,"Category1":k,"Category2":l,"Score":score},index=[0])
                            visibility = visibility.append(temp)
                        elif j == "#2 - #5":
                            score = category[(category["Rank"] >= 2) & (category["Rank"] <= 5)].shape[0]
                            temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":j,"Category1":k,"Category2":l,"Score":score},index=[0])
                            visibility = visibility.append(temp)
                        elif j == "#6 - #10":
                            score = category[(category["Rank"] >= 6) & (category["Rank"] <= 10)].shape[0]
                            temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":j,"Category1":k,"Category2":l,"Score":score},index=[0])
                            visibility = visibility.append(temp)
                        elif j == "#11 - #20":
                            score = category[(category["Rank"] >= 11) & (category["Rank"] <= 20)].shape[0]
                            temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":j,"Category1":k,"Category2":l,"Score":score},index=[0])
                            visibility = visibility.append(temp)
                        elif j == "#21 - #30":
                            score = category[(category["Rank"] >= 21) & (category["Rank"] <= 30)].shape[0]
                            temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":j,"Category1":k,"Category2":l,"Score":score},index=[0])
                            visibility = visibility.append(temp)
                        elif j == "#31 - #40":
                            score = category[(category["Rank"] >= 31) & (category["Rank"] <= 40)].shape[0]
                            temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":j,"Category1":k,"Category2":l,"Score":score},index=[0])
                            visibility = visibility.append(temp)
                        elif j == "#41 - #50":
                            score = category[(category["Rank"] >= 41) & (category["Rank"] <= 50)].shape[0]
                            temp = pd.DataFrame({"Date":date,"Device":i,"Criteria":j,"Category1":k,"Category2":l,"Score":score},index=[0])
                            visibility = visibility.append(temp)
            else:
                pass

#       change visibility["Score"] to integer
        visibility["Score"] = visibility["Score"].astype(int)
        visibility = visibility.reset_index(drop=True)

        print(f"Adding to database...")
        for i in visibility.index:
            KeywordDevice  = str(visibility["Device"][i])
            Criterion  = str(visibility["Criteria"][i])
            KeywordCategory1  = str(visibility["Category1"][i])
            KeywordCategory2  = str(visibility["Category2"][i])
            Score  = int(visibility["Score"][i])

    #   device
            cur.execute("SELECT Id FROM KeywordDevice WHERE Device = ? LIMIT 1", (KeywordDevice,))
            try:
                DeviceId = int(cur.fetchone()[0])
            except:
                cur.execute("INSERT OR IGNORE INTO KeywordDevice (Device) VALUES (?)", (KeywordDevice,))
                cur.execute("SELECT Id FROM KeywordDevice WHERE Device = ? LIMIT 1", (KeywordDevice,))
                DeviceId = int(cur.fetchone()[0])

    #   criterion
            cur.execute("SELECT Id FROM Criteria WHERE Criterion = ? LIMIT 1", (Criterion,))
            try:
                CriterionId = int(cur.fetchone()[0])
            except:
                cur.execute("INSERT OR IGNORE INTO Criteria (Criterion) VALUES (?)", (Criterion,))
                cur.execute("SELECT Id FROM Criteria WHERE Criterion = ? LIMIT 1", (Criterion,))
                CriterionId = int(cur.fetchone()[0])

    #   category1
            cur.execute("SELECT Id FROM KeywordCategory1 WHERE Category = ? LIMIT 1", (KeywordCategory1,))
            try:
                KeywordCategory1Id = int(cur.fetchone()[0])
            except:
                cur.execute("INSERT OR IGNORE INTO KeywordCategory1 (Category) VALUES (?)", (KeywordCategory1,))
                cur.execute("SELECT Id FROM KeywordCategory1 WHERE Category = ? LIMIT 1", (KeywordCategory1,))
                KeywordCategory1Id = int(cur.fetchone()[0])

    #   category2
            if client_name in multiple_categories:
                cur.execute("SELECT Id FROM KeywordCategory2 WHERE Category = ? LIMIT 1", (KeywordCategory2,))
                try:
                    KeywordCategory2Id = int(cur.fetchone()[0])
                except:
                    cur.execute("INSERT OR IGNORE INTO KeywordCategory2 (Category) VALUES (?)", (KeywordCategory2,))
                    cur.execute("SELECT Id FROM KeywordCategory2 WHERE Category = ? LIMIT 1", (KeywordCategory2,))
                    KeywordCategory2Id = int(cur.fetchone()[0])

                cur.execute(
                    f"""INSERT OR IGNORE INTO VisibilityDaily_{save_name}
                    (Date, DeviceId, CriterionId, KeywordCategory1Id, KeywordCategory2Id, Score)
                    VALUES (?, ?, ?, ?, ?, ?)""",
                    (date, DeviceId, CriterionId, KeywordCategory1Id, KeywordCategory2Id, Score,)
                )

            else:
                cur.execute(
                    f"""INSERT OR IGNORE INTO VisibilityDaily_{save_name}
                    (Date, DeviceId, CriterionId, KeywordCategory1Id, Score)
                    VALUES (?, ?, ?, ?, ?)""",
                    (date, DeviceId, CriterionId, KeywordCategory1Id, Score,)
                )

        con.commit()
        con.close()
        print(f"Completed {date}!")
        print(f"\n---------------------")
        time.sleep(2)

    print(f"\nCompleted {client_name}!\n")

print("Data transformation complete!")

# authenticate with gsheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
google_auth = r"C:\Users\JLee35\OneDrive - Dentsu Aegis Network\PROJECTS\Python\APIs\keys\iprospectseonorth\google_auth.json"
creds = ServiceAccountCredentials.from_json_keyfile_name(google_auth, scope)

# upload visibility table to gsheets
for i in client_list.index:
    folder_name = client_list["Folder Name"][i]
    client_name = client_list["Client Name"][i]
    stat_id = client_list["STAT ID"][i]
    gspread_id = client_list["GSheet ID"][i]

    database_name = getstat.dbize(folder_name)
    save_name = getstat.scrub(client_name)

    con = sqlite3.connect(os.path.join(root, folder_name, database_name))
    cur = con.cursor()
    print(f"\nRetrieving {client_name} visibility table from database...")

    today = dt.date.today().strftime("%m-%d")
    year_ago = int(dt.date.today().strftime("%Y"))-1
    if today == '02-29':
        year_ago = end_date.replace(day=28,year=year_ago).strftime("%Y-%m-%d")
    else:
        year_ago = end_date.replace(year=year_ago).strftime("%Y-%m-%d")

    if client_name in multiple_categories:
        sql = f"""
        SELECT VisibilityDaily_{save_name}.Date, KeywordDevice.Device as Device, Criteria.Criterion as Criteria, KeywordCategory1.Category as Category1, KeywordCategory2.Category as Category2, VisibilityDaily_{save_name}.Score
        FROM VisibilityDaily_{save_name}
        JOIN KeywordDevice ON VisibilityDaily_{save_name}.DeviceId = KeywordDevice.Id
        JOIN Criteria ON VisibilityDaily_{save_name}.CriterionId = Criteria.Id
        JOIN KeywordCategory1 ON VisibilityDaily_{save_name}.KeywordCategory1Id = KeywordCategory1.Id
        JOIN KeywordCategory2 ON VisibilityDaily_{save_name}.KeywordCategory2Id = KeywordCategory2.Id
        WHERE date(Date) >= :date;"""

        params={"date":year_ago}

        df = pd.read_sql(sql, params=params, con=con)
        df2 = df.copy()
        df3 = df[~df["Category2"].isin([""])]

        for i in df3.index:
            df2.at[i,"Category1"] = ""

        df2["Category"] = df2["Category1"] + df2["Category2"]
        df = df2[["Date","Device","Criteria","Category","Score"]]
    else:
        sql = f"""
        SELECT VisibilityDaily_{save_name}.Date, KeywordDevice.Device as Device, Criteria.Criterion as Criteria, KeywordCategory1.Category as Category, VisibilityDaily_{save_name}.Score
        FROM VisibilityDaily_{save_name}
        JOIN KeywordDevice ON VisibilityDaily_{save_name}.DeviceId = KeywordDevice.Id
        JOIN Criteria ON VisibilityDaily_{save_name}.CriterionId = Criteria.Id
        JOIN KeywordCategory1 ON VisibilityDaily_{save_name}.KeywordCategory1Id = KeywordCategory1.Id
        WHERE date(Date) >= :date;"""

        params={"date":year_ago}
        df = pd.read_sql(sql, params=params, con=con)

    df_rows, df_cols = df.shape

    print(f"Uploading {client_name} visibility table to Google Sheets...")

    client = gspread.authorize(creds)
    sheet = client.open_by_key(gspread_id)

    try:
        worksheet = sheet.worksheet(f"{client_name} - Visibility")
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound as err:
        worksheet = sheet.add_worksheet(title=f"{client_name} - Visibility", rows=1, cols=1)

    set_with_dataframe(worksheet, df)

    print(f"{client_name} complete!")

print("\nDURN!")

#%%
