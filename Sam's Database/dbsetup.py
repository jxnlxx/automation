#dbsetup.py
import sqlite3 as lite
import sys

con = lite.connect('salty.db')

with con:
    
    cur = con.cursor()
    
    cur.execute("CREATE TABLE Chars(CharId INTEGER PRIMARY KEY, Name TEXT, Tier TEXT, WinRate INT)")
    cur.execute("CREATE TABLE Fight(MatchId INTEGER PRIMARY KEY, Date TEXT, Red INT, Blue INT, Winner INT, RedMoney INT, BlueMoney INT, Odds TEXT, Bet INT, Win INT)")

CREATE TABLE Bank(No INTEGER PRIMARY KEY, MatchId INT, Balance INT);
SELECT Balance FROM Bank WHERE No = (SELECT MAX(No) FROM Bank);