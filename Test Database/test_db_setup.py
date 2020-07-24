"""
@author: jxnlxx
"""
#test_dp_setup.py

import sqlite3 as lite
import sys

con = lite.connect(r'L:\Commercial\Operations\Technical SEO\Automation\Data\test.db')

with con:

    cur = con.cursor()

    cur.execute("CREATE TABLE Keyword(KeywordId INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);")
    cur.execute("CREATE TABLE Device(DeviceId INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);")
    cur.execute("CREATE TABLE Market(MarketId INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);")
    cur.execute("CREATE TABLE Url(UrlId INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);")

#CREATE TABLE Bank(No INTEGER PRIMARY KEY, MatchId INT, Balance INT);
#SELECT Balance FROM Bank WHERE No = (SELECT MAX(No) FROM Bank);