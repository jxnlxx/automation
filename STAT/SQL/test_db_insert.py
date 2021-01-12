# -*- coding: utf-8 -*-
"""
Created on Wed Jul 15 07:51:48 2020

@author: JLee35
"""

#%%

import os
import sqlite3

conn = sqlite3.connect(r'L:\Commercial\Operations\Technical SEO\Automation\Data\STAT\SQL\test_relational.db')

with conn:

    c = conn.cursor()

    # c.execute('''CREATED TABLE IF NOT EXISTS test_keywords INTO KeywordMarket (KeywordMarket)
    # VALUES ('desktop','smartphone')
    #    );''')

    kw_id = c.execute('''SELECT SearchVolume (KeywordId, MarketId, DeviceId)
    VALUES ('dresses', 'GB-en','smartphone');''')

conn.close()
print('DURN!')

# %% example code for finding id
while True:
    acct = input('Enter a Twitter account, or quit: ')
    if (acct == 'quit'): break
    if (len(acct) < 1):
        cur.execute('SELECT id, name FROM People WHERE retrieved = 0 LIMIT 1')
        try:
            (id, acct) = cur.fetchone()
        except:
            print('No unretrieved Twitter accounts found')
            continue
    else:
        cur.execute('SELECT id FROM People WHERE name = ? LIMIT 1',
                    (acct, ))
        try:
            id = cur.fetchone()[0]
        except:
            cur.execute('''INSERT OR IGNORE INTO People
                        (name, retrieved) VALUES (?, 0)''', (acct, ))
            conn.commit()
            if cur.rowcount != 1:
                print('Error inserting account:', acct)
                continue
            id = cur.lastrowid