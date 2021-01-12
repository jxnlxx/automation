# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 10:31:18 2020

@author: JLee35
"""

import os
import pandas as pd

from calendar import monthrange

for filename in os.listdir(r'L:\Commercial\Operations\Technical SEO\Automation\STAT\SQL\Requests'):
    if filename.endswith(".csv"):
        print(filename)
        df = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\SQL\Requests\{filename}')
        try:
            df = df.drop(columns=['Status'])
        except KeyError:
            pass
        df.to_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\SQL\Requests\Backup\{filename}',index=False)
