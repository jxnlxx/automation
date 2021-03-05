#%% load file
"""
Created on Tue Mar 24 13:44:47 2020

@author: JLee35
"""

import re
import nltk
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
filename = tk.filedialog.askopenfilename(title='Select keyword tagging file', filetypes=(('All Files','*.*'),('Excel files','*.xlsx')))
root.destroy()
if filename.endswith('.csv'):
    kw_tagging = pd.read_csv(fr'{filename}')
else:
    if filename.endswith(('.xls','.xlsx')):
        kw_tagging = pd.read_excel(fr'{filename}')
    else:
        print('File type not supported. \nPlease re-run script and select a different file.')
        raise TypeError

#%%

df = kw_research.copy()
df['Category 1'] = [val for idx, val in enumerate(kw_tagging['keyword'])]

#%%

df = kw_research.merge(kw_tagging, on='Keyword',how='left')
df.to_excel(r'C:\Users\JLee35\Automation\Keyword Research\tester.xlsx')

#%% assign tags

df = kw_research.copy()
df2 = kw_tagging.groupby('Category 1').apply(lambda x: x['Keyword'].unique())



for keyword in df2.index:
    df[keyword] = ''

for col in df2.index:
    df2[col] = pd.np.where(df.Keyword.str.contains('|'.join(df2[col])),1,0)

# %% save

df.to_excel(fr'C:\Users\JLee35\Automation\Keyword Research\keyword_tagging_output.xlsx')


# %%
