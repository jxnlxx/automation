#db.py
import sqlite3 as lite
import sys
from datetime import datetime
import cfg


def addChar(name):
    con = lite.connect(cfg.dbName)

    with con:
    
        cur = con.cursor()
        cur.execute("INSERT INTO Chars(Name) VALUES (?);",(name,))
        lid = cur.lastrowid
        cur.execute("INSERT INTO stats(charid,ELO,FightTotal,WinTotal,WinRate,BetTotal,RSW,BSW,Losses) VALUES (?,?,?,?,?,?,?,?,?);",(lid,1000,0,0,0,0,0,0,0))
    print("Action: Adding character to DB: "+str(name))
    return lid


def getCharID(name):
    con = lite.connect(cfg.dbName)
    
    
    with con:
        
        cur = con.cursor()
        cur.execute("SELECT CharId FROM Chars WHERE Name=?",(name,))
        try:
            ID = cur.fetchone()[0]
        except:
            ID = None
    return ID

def getCharName(ID):
	con = lite.connect(cfg.dbName)
		
	with con:
		cur = con.cursor()
		cur.execute("SELECT Name FROM Chars WHERE CharId=?",(ID,))
		try:
			Name = cur.fetchone()[0]
		except:
			Name = None
		return Name

def getLastFight():
    con = lite.connect(cfg.dbName)
    with con:
        cur = con.cursor()
        cur.execute("SELECT * FROM fight WHERE MatchId=(SELECT MAX(MatchId) FROM fight)")
        try:
            data = cur.fetchall()
        except:
            data = None
        if data != None:
            fight = data[0]
    return fight

def getBetTotals(MatchId):
    con = lite.connect(cfg.dbName)
    with con:
        cur = con.cursor()
        cur.execute("SELECT RedMoney, BlueMoney FROM fight WHERE MatchId=?",(MatchId,))
        try:
            data = cur.fetchall()
        except:
            data = None
        if data != None:
            money = data[0]
    return money

    
def getCurrentBalance():
	con = lite.connect(cfg.bank)
	
	with con:
		
		cur = con.cursor()
		
		cur.execute("SELECT Balance FROM Bank WHERE No = (SELECT MAX(No) FROM Bank);")
		try:
			data = cur.fetchall()
		except:
			data = None
		if data != None:
			bal = data[0][0]

		print("getting balance: "+str(bal))
	return bal

def setNewBalance(MatchId,bal):
	con = lite.connect(cfg.bank)
	
	with con:
		
		cur = con.cursor()
		cur.execute("INSERT INTO Bank(MatchId,Balance) VALUES (?,?);",(MatchId,bal))
		print("New balance is: "+ str(bal))

def getCurrentTournamentBalance():
	con = lite.connect(cfg.bank)
		
	with con:
		print("getting tournment balance")
		cur = con.cursor()
		cur.execute("SELECT balance FROM tbank Where No = (select max(No) from tbank)")
		try:
			data = cur.fetchall()
		except:
			data = None
		if data != None:
			bal = data[0][0]
		return bal

def setNewTournamentBalance(MatchId,bal):
	con = lite.connect(cfg.bank)
		
	with con:

		cur = con.cursor()
		print("Setting new balance for tournament: "+ str(bal))
		cur.execute("INSERT INTO tbank(MatchId,Balance) VALUES (?,?);",(MatchId,bal))
		print("New balance is: "+ str(bal))

def resetTournamentBalance(tBailout):
	con = lite.connect(cfg.bank)
		
	with con:

		cur = con.cursor()
		print("Resetting balance for next tournament: ")
		cur.execute("INSERT INTO tbank(MatchId,Balance) VALUES (?,?);",(0,tBailout))

def syncBalance():
	newBal = web.getBank()
	cb = getCurrentBalance()
	if cb != newBal:
		print("updating balance from website")
		setNewBalance(0,newBal)
	else:
		print("Balance is up to date")

def createNewBet(MatchId,stake,choice):
	con = lite.connect(cfg.bank)
	if choice == 1:
		choice = "Red"
	else:
		choice = "Blue"
	with con:
		cur = con.cursor()
		cur.execute("INSERT INTO Bets(MatchId,stake,choice) VALUES (?,?,?)",(MatchId,stake,choice))

def updateBet(MatchId,winAmt):
	con = lite.connect(cfg.bank)
	with con:
		cur = con.cursor()
		cur.execute("UPDATE bets SET winamt=? WHERE matchid=?",(winAmt,MatchId))








