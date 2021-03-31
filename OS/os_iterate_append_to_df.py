# -*- coding: utf-8 -*-

import os
import pandas as pd

big_df = pd.DataFrame()

for filename in os.listdir(r'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\SERP Trends\Keywords List'):
    if filename.endswith(".csv"):
        df = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\SERP Trends\Keywords List\{filename}')

        clientName = filename.replace('_keywords_list_after_2019-06-01.csv', '')
        clientName = clientName.replace('_', '/')
        df = df.assign(client=clientName)
#        jobs_all = jobs_all.assign(Status='').astype(str)
        all_kws = all_kws.append(df)
        print(clientName,'appended!')
        continue
    else:
        continue