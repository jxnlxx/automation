# excel_horizontal_to_vertical_table_multiple_files.py

# %% remove completed tabs from the list (wb)

import os
import pandas as pd

path = r'C:\Users\JLee35\OneDrive - Dentsu Aegis Network\Python\Trainline\Decontaminated'

for filename in os.listdir(path):
#   clear variables from previous iteration
    df = ''
    df2 = ''
    dates = ''
    df_all = ''
    if filename.endswith(".xlsx"):
        if filename.startswith('~$'): # skips hidden temp files
            continue
        else:
            print(f'\n{filename} loading...')
            df = pd.read_excel(fr'{path}\{filename}', encoding='utf-8-sig')
            print(f'{filename} loaded!')
            site = filename.replace('.xlsx', '')
            n = 6
            df2 = df.iloc[:, :n] # this selects the first n columns
            dates = df.iloc[:, n:].columns.to_list() # this selects all columns after (not including) n

            df_all = pd.DataFrame()
            n=0
            for date in dates:
                ranks = df[date]
                temp = df2
                temp['Rank'] = ranks
                temp['Date'] = date
                temp['Site'] = site
                df_all = df_all.append(temp,ignore_index=True)
                print(f'Completed {n+1} of {len(dates)}')
                n += 1

            for year in ['2018', '2019', '2020', '2021']:
                df_all = df_all[df_all.columns.drop(list(df_all.filter(regex=year)))] # drop columns containing 'year'

            df_all = df_all.dropna(subset=[df_all.columns[0]])
            df_all = df_all[df_all['Keyword'].str.strip().astype(bool)]
            print(f'\n{filename} saving...')
            df_all.to_csv(fr'C:\Users\JLee35\OneDrive - Dentsu Aegis Network\Python\Trainline\Decontaminated\Verticle Tables\Retry\{site}.csv', encoding='utf-8-sig', index=False)
            print(f'{filename} saved!')
print('\nDURN!')

# %%
