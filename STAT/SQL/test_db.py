# -*- coding: utf-8 -*-
"""
Created on Wed Jul 15 07:51:48 2020

@author: JLee35
"""

#%%

import os
import sqlite3

conn = sqlite3.connect(r'L:\Commercial\Operations\Technical SEO\Automation\Data\STAT\SQL\serps_show_relational.db')

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

    c.execute('''CREATE TABLE SearchVolume(
        KeywordId INTEGER NOT NULL,
        MarketId INTEGER NOT NULL,
        DeviceId INTEGER NOT NULL,
        GlobalSearchVolume INTEGER,
        TargetedSearchVolume INTEGER,
        Jan INTEGER,
        Feb INTEGER,
        Mar INTEGER,
        Apr INTEGER,
        May INTEGER,
        Jun INTEGER,
        Jul INTEGER,
        Aug INTEGER,
        Sep INTEGER,
        Oct INTEGER,
        Nov INTEGER,
        Dec INTEGER,
        PRIMARY KEY (KeywordId, MarketId, DeviceId),
        FOREIGN KEY (KeywordId) REFERENCES Keyword (Id),
        FOREIGN KEY (MarketId) REFERENCES KeywordMarket (Id),
        FOREIGN KEY (DeviceId) REFERENCES KeywordDevice (Id)
        );''')

conn.close()
print('DURN!')
