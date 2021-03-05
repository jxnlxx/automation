#%% remove_non_ascii_chars.py

import pandas as pd
from tkinter import filedialog
import tkinter as tk

data_dir = r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Keyword Research\Data'

root = tk.Tk()
filename = filedialog.askopenfilename(title='Select File', initialdir=data_dir, filetypes=(('All Files','*.*'),('Excel files','*.xlsx')))
root.destroy()

print(f'Opening {filename}')

if filename.endswith('.csv'):
    df = pd.read_csv(fr'{filename}')
elif filename.endswith(('.xls','.xlsx')):
    df = pd.read_excel(fr'{filename}')
else:
    print('File type not supported. \nPlease re-run script and select a different file.')
    raise TypeError

# convert column names to lower case
df.columns = df.columns.str.lower()
df['keyword'] = df['keyword'].astype(str)
df['search volume'] = df['search volume'].astype(int)

df['keyword_new'] = df['keyword'].replace({r'[^ -\x7F]+':''}, regex=True)

root = tk.Tk()
savename = tk.filedialog.asksaveasfilename(title='Save File', initialdir=data_dir, defaultextension='.xlsx', filetypes =(('Excel Workbook','*.xlsx'),))
root.destroy()

df.to_excel(fr'{savename}', index=False)

# %%
