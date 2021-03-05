# excel_dropna_col_0_multiple_files.py

# %% remove completed tabs from the list (wb)

import os
import pandas as pd

path = r'C:\Users\JLee35\OneDrive - Dentsu Aegis Network\Python\Trainline\Decontaminated\Verticle Tables\Retry'

for filename in os.listdir(path):
#   clear variables from previous iteration
    df = ''
    if filename.endswith(".csv"):
        if filename.startswith('~$'): # skips hidden temp files
            continue
        else:

            print(f'\n{filename} loading...')
            df = pd.read_csv(fr'{path}\{filename}', encoding='utf-8-sig')

            print(f'{filename} loaded!')

            df = df.dropna(subset=[df.columns[0]])

            print(f'\n{filename} saving...')
            df.to_csv(fr'C:\Users\JLee35\OneDrive - Dentsu Aegis Network\Python\Trainline\Decontaminated\Verticle Tables\Retry\{filename}', encoding='utf-8-sig', index=False)
            print(f'{filename} saved!')

print('\nDURN!')

# %%
