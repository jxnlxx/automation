# %% stat_bulk_list.py


import requests
import pandas as pd
import getstat

# this takes ages if there are loads of jobs!

bulk_list = getstat.bulk_list()
bulk_list = pd.json_normalize(bulk_list)

#%%

