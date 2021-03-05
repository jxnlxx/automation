#%% os_iterate_through_folders_subfolders.py

import os
import pandas as pd

rootdir = fr"C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Digital PR & Outreach\Research Team\2021\Research\Van Monster\vehicle crime\Input"
output = fr"C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Digital PR & Outreach\Research Team\2021\Research\Van Monster\vehicle crime\Output"

big_df = pd.DataFrame()

for root, dirs, files in os.walk(rootdir):
    for name in files:
        if name.endswith(".csv"):
            print(name)
            df = pd.read_csv(os.path.join(root,name))
            df.columns = df.columns.str.lower()
        if name.endswith(".xlsx"):
            print(name)
            df = pd.read_excel(os.path.join(root,name))
            df.columns = df.columns.str.lower()
        df = df[df["crime type"] == "Vehicle crime"]
        big_df = big_df.append(df, ignore_index=True)

big_df.to_csv(os.path.join(output,'all-vehicle-crime-data.csv'),index=False)

print('DURN!')
# %%
