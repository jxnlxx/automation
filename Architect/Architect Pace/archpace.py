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
