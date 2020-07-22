# -*- coding: utf-8 -*-
"""
Created on Wed Jul 15 09:09:40 2020

@author: JLee35
"""

create_keywords_table =  '''CREATE TABLE IF NOT EXISTS keywords (
                                        keyword_id integer PRIMARY KEY,
                                        keyword text NOT NULL,
                                    );'''

c.execute(create_keywords_table)


for filename in os.listdir(r'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\Client Ranks\Keywords List'):
    if filename.endswith(".csv"):
        df = pd.read_csv(fr'L:\Commercial\Operations\Technical SEO\Automation\STAT\Data\Client Ranks\Keywords List\{filename}')
        df = df['keyword']
        df.to_sql('keywords', con, if_exists='append', index=False) 

