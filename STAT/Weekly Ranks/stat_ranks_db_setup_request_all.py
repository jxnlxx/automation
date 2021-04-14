#%% architect_pace_request.py

# google sheets bit
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

#   load client_list
client_list = pd.read_excel(r"C:\Users\JLee35\dentsu\Automation - Documents\General\STAT Ranks - Client List.xlsx")
client_list["STAT ID"] = client_list["STAT ID"].astype(int) # ensure that STAT ID is not a float
client_list["STAT ID"] = client_list["STAT ID"].astype(str) # ensure that STAT ID is str
print("Client list loaded!")

#%% filter client_list

folder_name = "Toolstation"
client_list = client_list[client_list["Folder Name"] == folder_name]
print('Client list filtered!')

#%% request exports

start_time = dt.datetime.now().strftime("%H:%M:%S")
root =fr"C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\STAT\Clients"

count=0
for i in client_list.index:
    folder_name = client_list["Folder Name"][i]
    client_name = client_list["Client Name"][i]
    stat_id = client_list["STAT ID"][i]

    database_name = getstat.dbize(folder_name)
    save_name = getstat.scrub(client_name)

    con = sqlite3.connect(os.path.join(root, folder_name, database_name))
    cur = con.cursor()

#   get date of next request
    print(f"\nDetermining start date for {client_name}...")
    requests_df = pd.read_sql_query(f"SELECT * FROM Requests_{save_name}", con)
    try:
        start_date = requests_df["Date"].iloc[-1] # get last value in requests_df["date"] as start_date
        start_date = dt.datetime.strptime(start_date, "%Y-%m-%d").date() #  convert start_date string to date object
        start_date = start_date + dt.timedelta(days=1) # add 1 day to start_date
    except (KeyError, IndexError) as e:
        start_date = getstat.get_createdat(stat_id)
        start_date = dt.datetime.strptime(start_date, "%Y-%m-%d").date()
        start_date = start_date + dt.timedelta(days=5)

    #       see if start date is before 1 year ago
        cutoff = (dt.date.today() - dt.timedelta(days = 365)).replace(day=1)
        if start_date < cutoff:
            start_date = cutoff
        else:
            pass

    #   ensure that start date is before end date
    end_date = dt.date.today() - dt.timedelta(days = 1)
    if start_date < end_date:
        pass
    else:
        print(f"{client_name} up-to-date! \nMoving onto next client...")
        continue

# #   start_date and end_date override
#     start_date = dt.datetime(2021, 3, 1).date()
#     end_date = dt.datetime(2021, 3, 18).date()

#   request reports for between "start_date" and "end_date"
    counter = 0
    print(f"\nRequesting {client_name} ranks...")
    for j in pd.date_range(start_date, end_date):
        date = str(j.strftime("%Y-%m-%d"))
        counter += 1
        print(f"{counter:03d} Requesting rank report for {date}")
        job_id = getstat.request_ranks(stat_id, date)
        status = 0
        cur.execute(f"""INSERT OR IGNORE INTO Requests_{save_name}(
            Date, JobId, Status)
        VALUES (?, ?, ?)""",
        (date, job_id, status))
    con.commit()
    con.close()
print(f"\n{counter} reports requested\n")

print("DURN!")
end_time = dt.datetime.now().strftime("%H:%M:%S")
print("\nStart:\t", start_time)
print("End:\t", end_time)

#%%
