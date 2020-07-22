# -*- coding: utf-8 -*-
"""
Created on Wed Jul 15 07:51:48 2020

@author: JLee35
"""

import os
import sqlite3
import requests
import pandas as pd

from getstat import stat_subdomain, stat_key, stat_base_url                     # saved locally in C:\Users\USERNAME\AppData\Local\Programs\Python\Python37-32\Lib

request_counter = 0
# =============================================================================
# Load full site list from STAT API
# =============================================================================

# =============================================================================
# print('\n'+'Requesting Site List from STAT...')
# 
# sites_all_url = f'{stat_base_url}/sites/all?&results=5000&format=json'
# response = requests.get(sites_all_url)
# request_counter += 1
# response = response.json()
# 
# total_results = response.get('Response').get('totalresults')
# 
# site_list = response.get('Response').get('Result')
# print('Site List received!')
# site_list = pd.DataFrame(site_list)
#  
# # Filter site_list so it shows only tracked sites with >0 keywords
# print('\n'+'Filtering site list...')
# 
# ## Remove False values from 'Tracking' column to leave only tracked sites
# site_list = site_list[site_list['Tracking'].str.contains('^true')]
# 
# ## Ensure 'TotalKeywords' column is set to 'int' (for next line)
# site_list['TotalKeywords'] = site_list['TotalKeywords'].astype(int)
# 
# ## Remove sites that have 0 keywords
# site_list = site_list.drop(site_list[site_list['TotalKeywords'] == 0].index)
# print('Removed untracked sites!')
# 
# site_list = site_list.reset_index(drop=True)
# 
# =============================================================================
# =============================================================================
# Create table for each site
# =============================================================================

conn = sqlite3.connect('stat_ranks_relational.db')

with conn:
    
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS keyword (id INTEGER PRIMARY KEY AUTOINCREMENT, keyword TEXT)')
    c.execute('CREATE TABLE IF NOT EXISTS device (id INTEGER PRIMARY KEY, device TEXT)')
    c.execute('CREATE TABLE IF NOT EXISTS market (id INTEGER PRIMARY KEY, market TEXT)')
    c.execute('CREATE TABLE IF NOT EXISTS site (id INTEGER PRIMARY KEY, site TEXT)')
    
# =============================================================================
#     for site in site_list['Url']:
#         save_name = site.replace('/','_') # Remove slashes so file will load properly
#         save_name = save_name.replace('.','_')
#         save_name = save_name.replace('-','_')
#         print(save_name)
#         c.execute('CREATE TABLE IF NOT EXISTS {} (date DATATIME, keyword_id INTEGER, google_rank INTEGER, google_base_rank INTEGER, google_ranking_url TEXT, FOREIGN KEY(keyword_id) REFERENCES keywords(keyword_id));'.format(save_name) )
#         
#         
# =============================================================================
