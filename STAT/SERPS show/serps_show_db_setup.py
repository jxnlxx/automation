# -*- coding: utf-8 -*-
"""
Created on Wed Jul 15 07:51:48 2020

@author: JLee35
"""

#%%

import os
import sqlite3

con = sqlite3.connect(r'C:\Users\JLee35\Automation\STAT\SERPS show\serps_show_normalised.db')

with con:

    cur = con.cursor()

    cur.execute('''CREATE TABLE IF NOT EXISTS phrases(
        Id INTEGER PRIMARY KEY,
        Phrase TEXT NOT NULL,
        UNIQUE (Phrase)
        );''')

    cur.execute('''CREATE TABLE IF NOT EXISTS markets(
        Id INTEGER PRIMARY KEY,
        Market TEXT NOT NULL,
        UNIQUE (Market)
       );''')

    cur.execute('''CREATE TABLE IF NOT EXISTS devices(
        Id INTEGER PRIMARY KEY,
        Device TEXT NOT NULL,
        UNIQUE (Device)
        );''')

    cur.execute('''CREATE TABLE IF NOT EXISTS industries(
        Id INTEGER PRIMARY KEY,
        Industry TEXT NOT NULL,
        UNIQUE (Industry)
        );''')

    cur.execute('''CREATE TABLE IF NOT EXISTS result_types(
        Id INTEGER PRIMARY KEY,
        ResultType TEXT NOT NULL,
        Unique (ResultType)
        );''')

    cur.execute(''' CREATE TABLE IF NOT EXISTS keywords(
        Id INTEGER PRIMARY KEY,
        StatId INTEGER NOT NULL,
        PhraseId INTEGER NOT NULL,
        MarketId INTEGER NOT NULL,
        DeviceId INTEGER NOT NULL,
        IndustryId INTEGER NOT NULL,
        CreatedAt DATETIME NOT NULL,
        GlobalSearchVolume INTEGER NOT NULL,
        TargetedSearchVolume INTEGER NOT NULL,
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
        FOREIGN KEY (PhraseId) REFERENCES phrases (Id),
        FOREIGN KEY (MarketId) REFERENCES markets (Id),
        FOREIGN KEY (DeviceId) REFERENCES devices (Id),
        FOREIGN KEY (IndustryId) REFERENCES industries (Id),
        UNIQUE (StatID)
        );''')

    cur.execute('''CREATE TABLE IF NOT EXISTS serps_show(
        KeywordId INTEGER NOT NULL,
        Date DATETIME NOT NULL,
        ProtocolId TEXT NOT NULL,
        DomainId TEXT NOT NULL,
        PathId TEXT NOT NULL,
        Rank INTEGER NOT NULL,
        BaseRank INTEGER NOT NULL,
        ResultTypeId TEXT NOT NULL,
        UNIQUE (KeywordId, Date, Rank),
        FOREIGN KEY (KeywordId) REFERENCES keywords (Id)
        FOREIGN KEY (ProtocolId) REFERENCES protocols (Id)
        FOREIGN KEY (DomainId) REFERENCES domains (Id)
        FOREIGN KEY (PathId) REFERENCES paths (Id)
        FOREIGN KEY (ResultTypeId) REFERENCES result_types (Id)
    );''')

    cur.execute('''CREATE TABLE IF NOT EXISTS protocols(
        Id INTEGER PRIMARY KEY,
        Protocol TEXT NOT NULL,
        UNIQUE (Protocol)
    );''')

    cur.execute('''CREATE TABLE IF NOT EXISTS domains(
        Id INTEGER PRIMARY KEY,
        Domain TEXT NOT NULL,
        UNIQUE (Domain)
    );''')

    cur.execute('''CREATE TABLE IF NOT EXISTS paths(
        Id INTEGER PRIMARY KEY,
        Path TEXT NOT NULL,
        UNIQUE (Path)
    );''')



con.close()
print('DURN!')



# %%
