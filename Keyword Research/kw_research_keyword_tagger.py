#%% load file
"""
Created on Tue Mar 24 13:44:47 2020

@author: JLee35
"""

import re
#import nltk
import collections
import numpy as np
import pandas as pd
import tkinter as tk
import datetime as dt

start_time = dt.datetime.now()

# select keyword research file
root = tk.Tk()
filename  = tk.filedialog.askopenfilename(title='Select keyword research (not tagging) file', filetypes=(('All Files','*.*'),('Excel files','*.xlsx')))
root.destroy()
if filename.endswith('.csv'):
    kw_research = pd.read_csv(fr'{filename}')
else:
    if filename.endswith(('.xls','.xlsx')):
        kw_research = pd.read_excel(fr'{filename}')
    else:
        print('File type not supported. \nPlease re-run script and select a different file.')
        raise TypeError

# select keyword tagging file
root = tk.Tk()
filename = tk.filedialog.askopenfilename(title='Select ngram tagging file', filetypes=(('All Files','*.*'),('Excel files','*.xlsx')))
root.destroy()
if filename.endswith('.csv'):
    kw_tagging = pd.read_csv(fr'{filename}')
else:
    if filename.endswith(('.xls','.xlsx')):
        kw_tagging = pd.read_excel(fr'{filename}')
    else:
        print('File type not supported. \nPlease re-run script and select a different file.')
        raise TypeError

# add category columns to df

df1 = kw_research.copy()
df1.columns = df1.columns.str.lower()
df2 = kw_tagging.copy()
df2.columns = df2.columns.str.lower()

tags = df2.loc[:,df2.columns.str.contains('tag')].columns # create list of new columns (allows # of columns to vary)

# format tags for potential errors
df2 = df2.replace(r'^\s+$', np.nan, regex=True) # adjust cells containing only whitespace to NaN
df2 = df2.replace(r'^\s+|\s+$', '', regex=True) # remove leading and trailing whitespace
df2 = df2.dropna(subset=tags, how='all').reset_index(drop=True)  # drop ngrams from df2 that have no tags

# add tag columns from d2 to df1
for i in tags:
    df1[i] = [[] for x in range(len(df1))] # create new columns in df1, adding empty list to each cell

# add tags from df2 to df1
cols = len(tags)
for i in tags:
    print(f'\nStarting column "{i}"...') # progress tracker
    groups = df2.groupby(i, as_index=False).aggregate(lambda x : list(x)) # groupby 'tag' column and create list of corresponding keywords (to limit the iterations)
    groups = groups[['keyword',i]]
    groups = groups.dropna(axis=0, how='any') # drop rows with no tags
    rows = len(groups)
    for j in groups.index:
        tag = groups[i][j]
        if tag not in [np.nan, 'nan', '']: # skip empty tags
            print (f'{j+1} of {rows} - {tag}') # progress tracker
            substrings = groups['keyword'][j]
            if len(substrings) > 1:
                substrings = '|'.join(substrings) # create string from list for regex later
            else:
                substrings = substrings[0] # else get element from list as str
            match = df1['keyword'].str.contains(substrings, regex=True, case=False)
            df1.loc[match, i] = df1[i].apply(lambda x: list(dict.fromkeys(x + [tag]))) # 'list(dict.fromkeys())' ensures the resulting list contains only unique values

print('\nFormatting tags...')
for i in tags:
    df1[i] = df1[i].str.join(',') # makes lists into strings separated by ','

print('\nTagging complete')

# save file dialog

root = tk.Tk()
savename = tk.filedialog.asksaveasfilename(title='Save File',  defaultextension='.xlsx', filetypes =(('Excel Workbook','*.xlsx'),))
root.destroy()

df1.to_excel(fr'{savename}', index = False)

print('DURN!')
# %%
