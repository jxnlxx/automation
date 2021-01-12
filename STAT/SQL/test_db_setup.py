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

    c.execute('''CREATE TABLE Keyword(
        Id INTEGER PRIMARY KEY,
        Keyword TEXT NOT NULL,
        UNIQUE (Keyword)
        );''')

    c.execute('''CREATE TABLE KeywordMarket(
        Id INTEGER PRIMARY KEY,
        KeywordMarket TEXT NOT NULL,
        UNIQUE (KeywordMarket)
       );''')

    c.execute('''CREATE TABLE KeywordDevice(
        Id INTEGER PRIMARY KEY,
        KeywordDevice TEXT NOT NULL,
        UNIQUE (KeywordDevice)
        );''')

conn.close()
print('DURN!')

# %%
