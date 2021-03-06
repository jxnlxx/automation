#%% architect_pace_VisibilityDaily.py

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

print("Client list loaded!\n")

# transform data

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

#   get dates from VisibilityDaily
    cur.execute(f"SELECT date FROM VisibilityDaily_{save_name} ORDER BY date ASC")
    VisibilityDaily_dates = cur.fetchall() # fetches list of tuples
    VisibilityDaily_dates = [x[0] for x in VisibilityDaily_dates] # convert to list of strings
    VisibilityDaily_dates = list(dict.fromkeys(VisibilityDaily_dates)) # remove duplicates

#   get dates from RanksDaily
    cur.execute(f"SELECT date FROM RanksDaily_{save_name} ORDER BY date ASC")
    RanksDaily_dates = cur.fetchall()
    RanksDaily_dates = [x[0] for x in RanksDaily_dates]
    RanksDaily_dates = list(dict.fromkeys(RanksDaily_dates))

#   get dates in RanksWeekly not present in VisibilityWeekly

    dates_list = [x for x in RanksDaily_dates if x not in VisibilityDaily_dates]

    if len(dates_list) > 0:
        print(f"Beginning VisibilityWeekly_{save_name}")
        pass
    else:
        print(f"{client_name} up-to-date!")
        print(f"\n----------------------\n")
        continue

    con.commit()
    con.close()

    counter = 0
    for date in dates_list:
        con = sqlite3.connect(os.path.join(root, folder_name, database_name))
        cur = con.cursor()

        counter += 1
        print(f"\n----------------------\n")
        print(f"{client_name} - job {counter} of {len(dates_list)}")

        print(f"Starting {date}...")

        sql = f"""
            SELECT RanksDaily_{save_name}.Date, KeywordsTable.Keyword, KeywordsTable.RegionalSearchVolume, RanksDaily_{save_name}.BaseRank as Rank, KeywordDevice.Device, KeywordCategory1.Category as Category1, KeywordCategory2.Category as Category2, Ctr_{save_name}.CTR
            FROM RanksDaily_{save_name}
            JOIN KeywordsTable ON RanksDaily_{save_name}.KeywordId = KeywordsTable.Id
            JOIN KeywordDevice ON KeywordsTable.DeviceId = KeywordDevice.Id
            JOIN KeywordCategory1 ON KeywordsTable.Category1Id = KeywordCategory1.Id
            JOIN KeywordCategory2 ON KeywordsTable.Category2Id = KeywordCategory2.Id
            JOIN Ctr_{save_name} ON RanksDaily_{save_name}.BaseRank = Ctr_{save_name}.Position
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

#   create list of devices
        device_list = list(dict.fromkeys(df["Device"].to_list()))

#   create list of criteria
        cur.execute("""SELECT Criterion from Criteria""")
        criteria_list = cur.fetchall() # fetches list of tuples
        criteria_list = [x[0] for x in criteria_list] # convert to list of strings
        criteria_list = list(dict.fromkeys(criteria_list)) # remove duplicates
        try:
            criteria_list.remove("")
        except ValueError:
            pass

        for device in device_list:
            device_df = df[df["Device"] == device]
            device_df = device_df[device_df["Category1"] != ""] # remove any untagged category 1

#   create category1_list
            category1_list = list(dict.fromkeys(device_df["Category1"].to_list()))
            try:
                category1_list.remove("")
            except (ValueError, NameError) as e:
                pass

#   create category2_list
            category2_list = list(dict.fromkeys(df["Category2"].to_list()))
            try:
                category2_list.remove("")
            except (ValueError, NameError) as e:
                pass

            category1 = "All Keywords"
            category2 = ""
            for criteria in criteria_list:
                if criteria == "Average Weighted Rank":
                    score = round(device_df["WeightedRank"].sum() / device_df["DailySearchVolume"].sum(),0)
                    temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                    visibility = visibility.append(temp)
                if criteria == "Visibility Score":
                    score = device_df["CtrScore"].sum()
                    temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                    visibility = visibility.append(temp)
                if criteria == "#1":
                    score = device_df[device_df["Rank"] == 1].shape[0]
                    temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                    visibility = visibility.append(temp)
                if criteria == "#2 - #5":
                    score = device_df[(device_df["Rank"] >= 2) & (device_df["Rank"] <= 5)].shape[0]
                    temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                    visibility = visibility.append(temp)
                if criteria == "#6 - #10":
                    score = device_df[(device_df["Rank"] >= 6) & (device_df["Rank"] <= 10)].shape[0]
                    temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                    visibility = visibility.append(temp)
                if criteria == "#11 - #20":
                    score = device_df[(device_df["Rank"] >= 11) & (device_df["Rank"] <= 20)].shape[0]
                    temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                    visibility = visibility.append(temp)
                if criteria == "#21 - #30":
                    score = device_df[(device_df["Rank"] >= 21) & (device_df["Rank"] <= 30)].shape[0]
                    temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                    visibility = visibility.append(temp)
                if criteria == "#31 - #40":
                    score = device_df[(device_df["Rank"] >= 31) & (device_df["Rank"] <= 40)].shape[0]
                    temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                    visibility = visibility.append(temp)
                if criteria == "#41 - #50":
                    score = device_df[(device_df["Rank"] >= 41) & (device_df["Rank"] <= 50)].shape[0]
                    temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                    visibility = visibility.append(temp)
                elif criteria == "#51 - #100":
                    score = device_df[(device_df["Rank"] >= 51) & (device_df["Rank"] <= 100)].shape[0]
                    temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                    visibility = visibility.append(temp)

            device_df = df[df["Device"] == device] #reset device_df

            for category1 in category1_list:
                category1_df = device_df[device_df["Category1"] == category1]
                category2 = ""
                for criteria in criteria_list:
                    if criteria == "Average Weighted Rank":
                        score = round(category1_df["WeightedRank"].sum() / category1_df["DailySearchVolume"].sum(),0)
                        temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                        visibility = visibility.append(temp)
                    if criteria == "Visibility Score":
                        score = category1_df["CtrScore"].sum()
                        temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                        visibility = visibility.append(temp)
                    if criteria == "#1":
                        score = category1_df[category1_df["Rank"] == 1].shape[0]
                        temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                        visibility = visibility.append(temp)
                    if criteria == "#2 - #5":
                        score = category1_df[(category1_df["Rank"] >= 2) & (category1_df["Rank"] <= 5)].shape[0]
                        temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                        visibility = visibility.append(temp)
                    if criteria == "#6 - #10":
                        score = category1_df[(category1_df["Rank"] >= 6) & (category1_df["Rank"] <= 10)].shape[0]
                        temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                        visibility = visibility.append(temp)
                    if criteria == "#11 - #20":
                        score = category1_df[(category1_df["Rank"] >= 11) & (category1_df["Rank"] <= 20)].shape[0]
                        temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                        visibility = visibility.append(temp)
                    if criteria == "#21 - #30":
                        score = category1_df[(category1_df["Rank"] >= 21) & (category1_df["Rank"] <= 30)].shape[0]
                        temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                        visibility = visibility.append(temp)
                    if criteria == "#31 - #40":
                        score = category1_df[(category1_df["Rank"] >= 31) & (category1_df["Rank"] <= 40)].shape[0]
                        temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                        visibility = visibility.append(temp)
                    if criteria == "#41 - #50":
                        score = category1_df[(category1_df["Rank"] >= 41) & (category1_df["Rank"] <= 50)].shape[0]
                        temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                        visibility = visibility.append(temp)
                    elif criteria == "#51 - #100":
                        score = category1_df[(category1_df["Rank"] >= 51) & (category1_df["Rank"] <= 100)].shape[0]
                        temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                        visibility = visibility.append(temp)

            for category2 in category2_list:
                category2_df = device_df[device_df["Category2"] == category2] #remove any untagged category 2
                category1 = ""
                for criteria in criteria_list:
                    if criteria == "Average Weighted Rank":
                        score = round(category2_df["WeightedRank"].sum() / category2_df["DailySearchVolume"].sum(),0)
                        temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                        visibility = visibility.append(temp)
                    if criteria == "Visibility Score":
                        score = category2_df["CtrScore"].sum()
                        temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                        visibility = visibility.append(temp)
                    if criteria == "#1":
                        score = category2_df[category2_df["Rank"] == 1].shape[0]
                        temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                        visibility = visibility.append(temp)
                    if criteria == "#2 - #5":
                        score = category2_df[(category2_df["Rank"] >= 2) & (category2_df["Rank"] <= 5)].shape[0]
                        temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                        visibility = visibility.append(temp)
                    if criteria == "#6 - #10":
                        score = category2_df[(category2_df["Rank"] >= 6) & (category2_df["Rank"] <= 10)].shape[0]
                        temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                        visibility = visibility.append(temp)
                    if criteria == "#11 - #20":
                        score = category2_df[(category2_df["Rank"] >= 11) & (category2_df["Rank"] <= 20)].shape[0]
                        temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                        visibility = visibility.append(temp)
                    if criteria == "#21 - #30":
                        score = category2_df[(category2_df["Rank"] >= 21) & (category2_df["Rank"] <= 30)].shape[0]
                        temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                        visibility = visibility.append(temp)
                    if criteria == "#31 - #40":
                        score = category2_df[(category2_df["Rank"] >= 31) & (category2_df["Rank"] <= 40)].shape[0]
                        temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                        visibility = visibility.append(temp)
                    if criteria == "#41 - #50":
                        score = category2_df[(category2_df["Rank"] >= 41) & (category2_df["Rank"] <= 50)].shape[0]
                        temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                        visibility = visibility.append(temp)
                    elif criteria == "#51 - #100":
                        score = category2_df[(category2_df["Rank"] >= 51) & (category2_df["Rank"] <= 100)].shape[0]
                        temp = pd.DataFrame({"Date":date,"Device":device,"Criteria":criteria,"Category1":category1,"Category2":category2,"Score":score},index=[0])
                        visibility = visibility.append(temp)

#   change visibility["Score"] to integer
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
            cur.execute("SELECT Id FROM KeywordCategory2 WHERE Category = ? LIMIT 1", (KeywordCategory2,))
            try:
                KeywordCategory2Id = int(cur.fetchone()[0])
            except:
                cur.execute("INSERT OR IGNORE INTO KeywordCategory2 (Category) VALUES (?)", (KeywordCategory2,))
                cur.execute("SELECT Id FROM KeywordCategory2 WHERE Category = ? LIMIT 1", (KeywordCategory2,))
                KeywordCategory2Id = int(cur.fetchone()[0])

#   insert into database
            cur.execute(
                f"""INSERT OR IGNORE INTO VisibilityDaily_{save_name}
                (Date, DeviceId, CriterionId, KeywordCategory1Id, KeywordCategory2Id, Score)
                VALUES (?, ?, ?, ?, ?, ?)""",
                (date, DeviceId, CriterionId, KeywordCategory1Id, KeywordCategory2Id, Score,))

            con.commit()
        con.close()
        print(f"Completed {date}!")
        time.sleep(2)

    print(f"\nCompleted {client_name}!")
    print(f"\n----------------------\n")
print("Data transformation complete!")
print(f"\n----------------------\n")

# upload last year of VisibilityDaily table to gsheets

# authenticate with gsheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
google_auth = r"C:\Users\JLee35\OneDrive - Dentsu Aegis Network\PROJECTS\Python\APIs\keys\iprospectseonorth\google_auth.json"
creds = ServiceAccountCredentials.from_json_keyfile_name(google_auth, scope)

# upload visibility table to gsheets
for i in client_list.index:
    folder_name = client_list["Folder Name"][i]
    client_name = client_list["Client Name"][i]
    stat_id = client_list["STAT ID"][i]

    database_name = getstat.dbize(folder_name)
    save_name = getstat.scrub(client_name)

    con = sqlite3.connect(os.path.join(root, folder_name, database_name))
    cur = con.cursor()

    today = dt.date.today()
    if today.strftime("%m-%d") == "02-29":
        year_ago = today.replace(day=28,year=today.year - 1).strftime("%Y-%m-%d")
    else:
        year_ago = today.replace(year=today.year - 1).strftime("%Y-%m-%d")

    sql = f"""
    SELECT VisibilityDaily_{save_name}.Date,
    KeywordDevice.Device as Device,
    Criteria.Criterion as Criteria,
    KeywordCategory1.Category as Category1,
    KeywordCategory2.Category as Category2,
    VisibilityDaily_{save_name}.Score
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

    for j in df3.index:
        df2.at[j,"Category1"] = ""

    df2["Category"] = df2["Category1"] + df2["Category2"]
    df = df2[["Date","Device","Criteria","Category","Score"]]

#   upload df to gsheets
    gsheet_name = f"{client_name} - Visibility"
    gspread_id = client_list["GSheet ID"][i]
    print(f"Uploading '{gsheet_name}' to Google Sheets...")
    client = gspread.authorize(creds)
    sheet = client.open_by_key(gspread_id)

    try:
        worksheet = sheet.worksheet(gsheet_name)
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound as err:
        worksheet = sheet.add_worksheet(title=gsheet_name, rows=1, cols=1)

    set_with_dataframe(worksheet, df)
    con.close()

    print(f"{client_name} complete!")
    print(f"\n----------------------\n")
print("DURN!")

#%%

client_list = client_list[client_list["Client Name"] == "Simply Be"]

# %%
