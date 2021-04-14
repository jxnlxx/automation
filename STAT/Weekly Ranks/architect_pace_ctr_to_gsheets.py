#%% architect_pace_ctr_to_gsheets.py

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

root = fr"C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\STAT\Clients"

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
google_auth = r"C:\Users\JLee35\OneDrive - Dentsu Aegis Network\PROJECTS\Python\APIs\keys\iprospectseonorth\google_auth.json"
creds = ServiceAccountCredentials.from_json_keyfile_name(google_auth, scope)

for i in client_list.index:
    folder_name = client_list["Folder Name"][i]
    client_name = client_list["Client Name"][i]
    stat_id = client_list["STAT ID"][i]
    gspread_id = client_list["GSheet ID"][i]

    database_name = getstat.dbize(folder_name)
    save_name = getstat.scrub(client_name)

    con = sqlite3.connect(os.path.join(root, folder_name, database_name))
    cur = con.cursor()

    print(f"Starting {client_name}...")

    sql = f"SELECT * FROM Ctr_{save_name} LIMIT 20"
    df = pd.read_sql(sql, con=con)

    client = gspread.authorize(creds)
    sheet = client.open_by_key(gspread_id)

    try:
        worksheet = sheet.worksheet(f"{client_name} - CTR Analysis")
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound as err:
        worksheet = sheet.add_worksheet(title=f"{client_name} - CTR Analysis", rows=1, cols=1)

    set_with_dataframe(worksheet, df)

    print(f"{client_name} complete!")
    print(f"\n----------------------\n")

print('DURN!')

# %%
