#%% stat_ranks_create_keyword_csv.py

import os
import time
import json
import getstat
import datetime as dt
import requests
import numpy as np
import pandas as pd


client_list = pd.read_excel(r"C:\Users\JLee35\dentsu\Automation - Documents\General\STAT Ranks - Client List.xlsx")
client_list["STAT ID"] = client_list["STAT ID"].astype(int) # ensure that STAT ID is not a float
client_list["STAT ID"] = client_list["STAT ID"].astype(str) # ensure that STAT ID is str
print("Done!")

#%%

folder_name = "Entertainment Magpie"
client_list = client_list[client_list["Folder Name"] == folder_name]

#%% request exports for all clients

root = fr"C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\STAT\Clients"

for i in client_list.index:
    folder_name = client_list["Folder Name"][i]
    client_name = client_list["Client Name"][i]
    stat_id = client_list["STAT ID"][i]
    csv_name = getstat.scrub(client_name) + "_keywords_list.csv"
    setup = "Setup"

#   create folders for client
    if not os.path.exists(os.path.join(root, folder_name)):
        os.mkdir(os.path.join(root, folder_name))
    if not os.path.exists(os.path.join(root, folder_name, setup)):
        os.mkdir(os.path.join(root, folder_name, setup))

    print("\n"+f"Requesting data for {client_name}")
    keywords_list = getstat.keywords_list(stat_id)
#   process keywords for export
    keywords_list = pd.json_normalize(keywords_list)
    categories = pd.DataFrame(columns=["Category1","Category2","Category3","Category4","Category5"])
    keywords_list = keywords_list.append(categories)
    keywords_list.to_csv(os.path.join(root, folder_name, setup, csv_name), index=False)

print("\nDURN!")

# %%
