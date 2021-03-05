# kw_phrase_extraction.py
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 13:44:47 2020
@author: JLee35

"""
# %%

import pandas as pd
import tkinter as tk
import inflect
from tkinter import filedialog

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

p = inflect.engine()

# open file
root = tk.Tk()
filename = tk.filedialog.askopenfilename(title='Select File', filetypes=(('All Files','*.*'),('Excel files','*.xlsx'),('CSV files','*.csv')))
root.destroy()

print('Opening file...')
if filename.endswith('.csv'):
    df = pd.read_csv(fr'{filename}')
elif filename.endswith(('.xls','.xlsx')):
    df = pd.read_excel(fr'{filename}')
else:
    print('File type not supported. \nPlease re-run script and select a different file.')
    raise TypeError

df.columns = df.columns.str.lower()

def singularise(keyword):
    s = ''
    keyword = keyword.split()
    for i in keyword:
        if p.singular_noun(i):
            s += f' {p.singular_noun(i)}'
        else:
            s += ' '+ i
    s = s.strip()
    return s

print('Generating singular variants..')
df['singular'] = [singularise(x) for x in df['keyword']]

print('Generating isomers...')
df['Isomers'] = [' '.join(sorted(x.split(), key=str.lower)) for x in df['keyword']]

root = tk.Tk()

savename = tk.filedialog.asksaveasfilename(title='Save File',  defaultextension='.xlsx', filetypes =(('Excel Workbook','*.xlsx'),))
root.destroy()
df.to_excel(fr'{savename}', index=False)

print('DURN!')

# %%
