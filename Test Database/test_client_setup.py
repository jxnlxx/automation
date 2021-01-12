# -*- coding: utf-8 -*-
#
# @author: Jon Lee
# @email: jon.lee@iprospect.com
# @create date: 2020-08-03 14:27:54
#

# test_client_setup.py

import sqlite3 as lite
import sys

con = lite.connect(r'L:\Commercial\Operations\Technical SEO\Automation\Data\test.db')

with con:

    cur = con.cursor()

    cur.execute('''CREATE TABLE test_client (
        Date DATETIME,
        Id VARCHAR PRIMARY KEY,
        KeywordId INTEGER,
        Keyword INTEGER,
        FOREIGN KEY (Keyword) REFERENCES Keyword (KeywordId),
        KeywordMarket INTEGER,
        FOREIGN KEY (KeywordMarket) REFERENCES Market (MarketId),
        KeywordLocation TEXT,
        KeywordDevice TEXT,
        FOREIGN KEY (Device) REFERENCES Device (DeviceId),
        KeywordTranslation TEXT,
        KeywordCategories TEXT,
        KeywordTags TEXT,
        CreatedAt DATETIME,
        AdvertiserCompetition REAL,
        GlobalSearchVolume INTEGER,
        TargetedSearchVolume INTEGER,
        Apr INTEGER,
        Mar INTEGER,
        Feb INTEGER,
        Jan INTEGER,
        Dec INTEGER,
        Nov INTEGER,
        Oct INTEGER,
        Sep INTEGER,
        Aug INTEGER,
        Jul INTEGER,
        Jun INTEGER,
        May INTEGER,
        CPC REAL,
        Type INTEGER,
        FOREIGN KEY (Type) REFERENCES Type (TypeId),
        GoogleRank INTEGER,
        GoogleBaseRank INTEGER,
        GoogleUrl INTEGER,FOREIGN KEY (GoogleUrl) REFERENCES Url (UrlId),
    );''')