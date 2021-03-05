# excel_horizontal_to_vertical_table.py

#%%

import pandas as pd

df = pd.read_csv(fr'C:\Users\JLee35\Automation\Excel\export_rankings_IT_for_script.csv')
df2 = df[['Keyword', 'Project','Market','Engine','Device','Tags']]
dates = df.drop(columns=['Keyword', 'Project','Market','Engine','Device','Tags']).columns.to_list()

site = 'thetrainline.com/it'

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

print('\nSaving...')

df_all.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\Data\STAT\Trainline\export_rankings_IT_amalgamated.csv', index=False)

print('\nDURN!')

# %%
