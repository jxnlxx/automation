#%% kw_research_remove_isomers.py

import pandas as pd
import re
import nltk
import collections
import datetime
import tkinter as tk
from tkinter import filedialog


# open file
root = tk.Tk()
filename = tk.filedialog.askopenfilename(title='Select File', filetypes=(('All Files','*.*'),('Excel files','*.xlsx'),('CSV files','*.csv')))
root.destroy()

print('Opening file...')
if filename.endswith('.csv'):
    kw_list = pd.read_csv(fr'{filename}')
elif filename.endswith(('.xls','.xlsx')):
    kw_list = pd.read_excel(fr'{filename}')
else:
    print('File type not supported. \nPlease re-run script and select a different file.')
    raise TypeError

# remove n-gram isomers
print('Removing isomers...')
kw_list['Isomers'] = [', '.join(sorted(row.split(), key=str.lower)) for row in kw_list['Keyword']]
kw_list = kw_list.drop_duplicates(subset = ['Search Volume', 'Isomers'], keep='first')
kw_list = kw_list.drop(labels='Isomers', axis=1)

# save file

root = tk.Tk()
savename = tk.filedialog.asksaveasfilename(title='Save File',  defaultextension='.xlsx', filetypes =(('Excel Workbook','*.xlsx'),))
root.destroy()
print('Saving file...')
kw_list.to_excel(fr'{savename}', index=False)

print('DURN!')

# %%
