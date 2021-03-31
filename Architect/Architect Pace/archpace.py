#%% architect_pace_definitions.py

import datetime as dt


def scrub(string):
    return string.lower().replace(' ', '_').replace('(','').replace(')','').replace(',','')

# check datetime, if it's before one year ago, return oneyear, if not, return datetime
def cutoff(datetime):
    oneyear = (dt.date.today() - dt.timedelta(days = 365)).replace(day=1)
    if datetime <= oneyear:
        return oneyear
    else:
        return datetime


# %%


import sqlite3
import pandas as pd

con = sqlite3.connect(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\architect_pace_normalised.db')
cur = con.cursor()

df = pd.read_sql_query('''SELECT keywords.keyword, devices.Device, categories.Category
    FROM keywords
    JOIN devices ON keywords.DeviceId = devices.Id
    JOIN categories ON keywords.CategoryId = categories.Id
    ''' ,con)

df.to_csv(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\kw_list.csv',index=False)


# %%

df = pd.read_csv(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\kw_list.csv')

cur.execute('''CREATE TABLE IF NOT EXISTS facet_tags(
    Id INTEGER PRIMARY KEY,
    Category TEXT NOT NULL,
    UNIQUE (Category)
    );''')

facets = 
# %%
