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

# dates

date = dt.date.today().replace(day = 1) - dt.timedelta(days = 1)
year = int(date.strftime('%Y'))
month = int(date.strftime('%m'))
days = int(date.strftime('%d'))
start_date = f'{year}-{month:02d}-01'
end_date = f'{year}-{month:02d}-{days:02d}'

# load client_list

client_list = pd.read_excel(r"C:\Users\JLee35\dentsu\Automation - Documents\General\STAT Ranks - Client List.xlsx")
client_list["STAT ID"] = client_list["STAT ID"].astype(int) # ensure that STAT ID is not a float
client_list["STAT ID"] = client_list["STAT ID"].astype(str) # ensure that STAT ID is str

client_list = client_list[client_list["Monthly Report"] == "Active"]

print("Client list loaded!")

# amalgamate ranks by month
root = fr"C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\STAT\Clients"
teams_dir = r"C:\Users\JLee35\dentsu\IVY Restaurants - Documents\General\STAT Rank Reports"

ranks_all = pd.DataFrame(columns=["Restaurant","Keyword","Device","BaseRank"])

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
#   aggregate ranks by month
    sql = f"""
        SELECT RanksDaily_{save_name}.Date, KeywordsTable.StatId, KeywordsTable.Keyword, KeywordDevice.Device, RanksDaily_{save_name}.BaseRank as BaseRank
        FROM RanksDaily_{save_name}
        JOIN KeywordsTable ON RanksDaily_{save_name}.KeywordId = KeywordsTable.Id
        JOIN KeywordDevice ON KeywordsTable.DeviceId = KeywordDevice.Id
        WHERE date BETWEEN :sDate AND :eDate;
        """
    params = {"sDate":start_date, "eDate":end_date}
    df = pd.read_sql(sql, params=params, con=con)

    month_df = df.drop_duplicates(subset=["StatId"], keep="first")

    month_df = month_df.drop(["BaseRank",], axis=1)
#   get average BaseRank
    temp =  df.groupby("StatId", as_index=False)["BaseRank"].mean().round().astype(int)
    month_df = month_df.merge(temp,on="StatId",how="left")
#   get top RankingUrl
    temp = df.sort_values("BaseRank", ascending=True).drop_duplicates("StatId").sort_index().reset_index(drop=True)
    month_df = pd.merge(month_df, temp[["StatId",]], on="StatId", how="left")
# insert RanksMonthly_df into RanksDaily_{save_name}
    month_df.insert(loc=0, column="Restaurant", value=client_name)
    month_df = month_df.drop(["Date","StatId",], axis=1)
    ranks_all = ranks_all.append(month_df,)

#%%
ranks_all.to_csv(os.path.join(teams_dir,f"{year}_{month:02d}_ivy_all.csv"), index=False)

print("\nDURN!")

# %%

month_df.to_csv(os.path.join(root,folder_name,f"{year}_{month:02d}_ivy_all.csv"))