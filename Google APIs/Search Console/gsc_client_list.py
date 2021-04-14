#%% gsc_client_list.py
import re
import os
import time
import json
import requests
import openpyxl
import pandas as pd
import tkinter as tk
import datetime as dt
import xlsxwriter as xl

from uuid import uuid4
from calendar import monthrange
from xlsxwriter.utility import xl_range
from urllib.parse import urlparse

import httplib2
from googleapiclient.discovery import build
from oauth2client.client import OAuth2WebServerFlow
from collections import defaultdict
from dateutil import relativedelta
import argparse
from oauth2client import client
from oauth2client import file
from oauth2client import tools
from urllib.parse import urlparse


start_time = dt.datetime.now().replace(microsecond=0)

# API SECRETS

CLIENT_SECRETS_PATH = r"C:\Users\JLee35\OneDrive - Dentsu Aegis Network\PROJECTS\Python\APIs\keys\iprospectseonorth\OAuth2\credentials.json"
WEBMASTER_CREDS_DAT = r"C:\Users\JLee35\OneDrive - Dentsu Aegis Network\PROJECTS\Python\APIs\keys\iprospectseonorth\OAuth2\webmaster_credentials.dat"
SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]

# AUTHENTICATE

# Create a parser to be able to open browser for Authorization
parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    parents=[tools.argparser])
flags = parser.parse_args([])

flow = client.flow_from_clientsecrets(
    CLIENT_SECRETS_PATH, scope = SCOPES,
    message=tools.message_if_missing(CLIENT_SECRETS_PATH))

# Prepare credentials and authorize HTTP
# If they exist, get them from the storage object
# credentials will get written back to a file.
storage = file.Storage(WEBMASTER_CREDS_DAT)
credentials = storage.get()

# If authenticated credentials don't exist, open Browser to authenticate
if credentials is None or credentials.invalid:
    credentials = tools.run_flow(flow, storage, flags)
http = credentials.authorize(http=httplib2.Http())
service = build("webmasters", "v3", http=http)

# REQUEST SITE LIST FROM SEARCH CONSOLE

site_list = service.sites().list().execute() # pylint: disable=maybe-no-member
site_list = site_list.get("siteEntry")
site_list = pd.DataFrame.from_records(site_list)

# sort dataframe by siteUrl alphabetically, ignoring case
site_list = site_list.iloc[site_list.siteUrl.str.lower().argsort()]
site_list = site_list.reset_index(drop=True)

#site_list.to_csv( r"L:\Commercial\Operations\Technical SEO\Automation\Google API\Search Console\Data\Setup\search_console_site_list.csv", index=False )


#%%
import tkinter as tk
import math

options = site_list["siteUrl"].tolist()

rows = math.ceil(len(options)/6)+1
if len(options) >=6:
    cols = 6
else:
    cols = len(options)

class CheckBox(tk.Checkbutton):
    boxes = []  # Storage for all buttons

    def __init__(self, master=None, **options):
        tk.Checkbutton.__init__(self, master, options)  # Subclass checkbutton to keep other methods
        self.boxes.append(self)
        self.var = tk.BooleanVar()  # var used to store checkbox state (on/off)
        self.text = self.cget("text")  # store the text for later
        self.configure(variable=self.var)  # set the checkbox to use our var

root = tk.Tk()
root.title("SEO Technical Healthchecks")
title = tk.Label(root, text="Please select client(s) for which you'd like to generate healthchecks:")
title.grid(row=0, column=0, columnspan=6, padx=400, pady=10)

for index, item in enumerate(options):
    x = math.floor(index/6)+1
    y = index-((math.floor(index/6))*6)
    cb = CheckBox(root, text=item)
    cb.grid(row=x, column=y, sticky=tk.W)

selection = []
def selected():
    for box in CheckBox.boxes:
        if box.var.get():  # Checks if the button is ticked
            selection.append(box.text)
    root.destroy()

rows = math.ceil(len(options)/5)+1
button = tk.Button(root, text="Select", command=selected, padx=20)
button.grid(row=rows, column=2, pady=20)

root.mainloop()
#%%

for i in selection:
    today = dt.date.today() - dt.timedelta(days=7)
    if today.strftime("%m-%d") == "02-29":
        start_date = today.replace(day=28,year=today.year - 1).strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")
    else:
        start_date = today.replace(year=today.year - 1).strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")

        # populate the request with details
        # available "dimensions" are - "date", "country", "device", "page", "query", "searchAppearance"
        max_row = 10000
        start_row = 0
        status = ""
        request = {
              "startDate": start_date,
              "endDate": end_date,
              "dimensions": ["date", "query","country", "device"], # uneditable to enforce a nice clean dataframe at the end!
              "rowLimit": 25000, # valid range is 1 - 25000; default is 1000
              "startRow": 0, # "0" starts it at the beginning
               }
        print(f"Requesting data for {i}...")
        response = service.searchanalytics().query(siteUrl=i, body=request).execute() # pylint: disable=maybe-no-member
        print(f"Data Received!")

        try:
            sc_dict = defaultdict(list)
            print("Processing results...")
            #Process the response
            for row in response["rows"]:
                sc_dict["date"].append(row["keys"][0] or 0)
                sc_dict["query"].append(row["keys"][1] or 0)
                sc_dict["country"].append(row["keys"][2] or 0)
                sc_dict["device"].append(row["keys"][3] or 0)
                sc_dict["clicks"].append(row["clicks"] or 0)
                sc_dict["impressions"].append(row["impressions"] or 0)
                sc_dict["ctr"].append(row["ctr"] or 0)
                sc_dict["position"].append(row["position"] or 0)
            print("successful at %i" % max_row)

        except KeyError:
            print("error occurred at %i" % max_row)
            continue
        #Add response to dataframe response

        df = pd.DataFrame(data = sc_dict)
        print("Filtering results...")
        df["clicks"] = df["clicks"].astype("int")
        df["ctr"] = df["ctr"]*100
        df["impressions"] = df["impressions"].astype("int")
        df["position"] = df["position"].round(2)
        df = df.sort_values("impressions", ascending = False)


# END TIMER

end_time = dt.datetime.now().replace(microsecond=0)
time_elapsed = end_time - start_time

print("\n"+"DURN!")
print("\n"+f"Time elapsed: {time_elapsed}")

# %%
