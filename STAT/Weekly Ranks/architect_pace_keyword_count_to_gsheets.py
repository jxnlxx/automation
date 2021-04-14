#%% architect_pace_keyword_count_to_gsheets.py

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

#%% upload category list to gsheets

root = fr"C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\STAT\Clients"

# authenticate with gsheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
google_auth = r"C:\Users\JLee35\OneDrive - Dentsu Aegis Network\PROJECTS\Python\APIs\keys\iprospectseonorth\google_auth.json"
creds = ServiceAccountCredentials.from_json_keyfile_name(google_auth, scope)



for i in client_list.index:
    folder_name = client_list["Folder Name"][i]
    client_name = client_list["Client Name"][i]
    stat_id = client_list["STAT ID"][i]

    database_name = getstat.dbize(folder_name)
    save_name = getstat.scrub(client_name)

    con = sqlite3.connect(os.path.join(root, folder_name, database_name))
    cur = con.cursor()
    print(f"Retrieving 'RanksDaily_{save_name}' from database...")

    cur.execute(f"SELECT date FROM RanksDaily_{save_name} ORDER BY date DESC LIMIT 1;")
    date = str(cur.fetchone()[0])
    date = dt.datetime.strptime(date, "%Y-%m-%d").date() #  convert start_date string to date object

    sql = f"""
        SELECT RanksDaily_{save_name}.Date, KeywordsTable.StatId, KeywordsTable.Keyword, KeywordDevice.Device, KeywordCategory1.Category as 'Category1', KeywordCategory2.Category as 'Category2'
        FROM RanksDaily_{save_name}
        JOIN KeywordsTable ON RanksDaily_{save_name}.KeywordId = KeywordsTable.Id
        JOIN KeywordDevice ON KeywordsTable.DeviceId = KeywordDevice.Id
        JOIN KeywordCategory1 ON KeywordsTable.Category1Id = KeywordCategory1.Id
        JOIN KeywordCategory2 ON KeywordsTable.Category2Id = KeywordCategory2.Id
        WHERE date(Date) = :date;"""
    params={"date":date}
    df = pd.read_sql(sql, params=params, con=con)

#   create device_list
    device_list = list(dict.fromkeys(df["Device"].to_list()))
    categories_df = pd.DataFrame(columns=["Device","Category","Count"])

    for j in device_list:
        print("\n"+ j +"\n")
        device_df = df[df["Device"] == j]
#   create category1_list
        category1_list = list(dict.fromkeys(device_df["Category1"].to_list()))
        try:
            category1_list.remove("")
        except (ValueError, NameError) as e:
            pass

#   create category2_list
        category2_list = list(dict.fromkeys(device_df["Category2"].to_list()))
        try:
            category2_list.remove("")
        except (ValueError, NameError) as e:
            pass
        temp = {"Device": j, "Category": "All Keywords", "Count": device_df[device_df[["Category1","Category2"]].astype(bool)].shape[0]}
        categories_df = categories_df.append(temp,ignore_index=True)
        for k in category1_list:
            print(k)
            temp = {"Device": j, "Category": k, "Count": device_df[device_df["Category1"] == k].shape[0]}
            categories_df = categories_df.append(temp,ignore_index=True)
        for k in category2_list:
            print(k)
            temp = {"Device": j, "Category": k, "Count": device_df[device_df["Category2"] == k].shape[0]}
            categories_df = categories_df.append(temp,ignore_index=True)

    print(f"\n---------------------\n")

# upload df to gsheets
    gsheet_name = f"{client_name} - Keyword Counts"
    gspread_id = client_list["GSheet ID"][i]
    print(f"Uploading '{gsheet_name}' to Google Sheets...")
    client = gspread.authorize(creds)
    sheet = client.open_by_key(gspread_id)

    try:
        worksheet = sheet.worksheet(gsheet_name)
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound as err:
        worksheet = sheet.add_worksheet(title=gsheet_name, rows=1, cols=1)

    set_with_dataframe(worksheet, categories_df)

    print(f"{client_name} complete!")
    print(f"\n---------------------\n")

print("\nDURN!")

#%%
