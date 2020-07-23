#betting.py
import sqlite3 as lite
import sys
import re
import db
import math
import web
import cfg
import stats

def chooseFighter(info,RID,BID):
	#pull stats of fighters
    choice = 0
    ELOs=[]
    WR=[]
    con = lite.connect(cfg.dbName)
    with con:
        cur = con.cursor()
        cur.execute("SELECT ELO,WinRate FROM Stats WHERE CharId=?",(RID,))
        try:
            data = cur.fetchall()
        except:
            data = None
        if data != None:
            ELOs.append(data[0][0])
            WR.append(data[0][1])
        cur.execute("SELECT ELO,WinRate FROM Stats Where CharId=?",(BID,))
        try:
            data = cur.fetchall()
        except:
            data = None
        if data != None:
            ELOs.append(data[0][0])
            WR.append(data[0][1])
        p=int(100*round(stats.expectedScore(ELOs[0],ELOs[1]),2))
        print("***********************************")
        print( info["p1name"]+" ELO: " + str(ELOs[0])+ "   " + info["p2name"]+" ELO: "+str(ELOs[1]))
        print(	info["p1name"]+" WR: " + str(WR[0])+ "   " + info["p2name"]+"  WR: "+str(WR[1]))
        print("***********************************")
        print("Probability of "+info["p1name"]+" winning is: " +str(p)+"%")
        if ELOs[0]>ELOs[1]:
            print("Recommend betting RED")
            choice = 1
        else:
            print("Recommend betting BLUE")
            choice = 2
        print("***********************************")
    return choice

def calcBet(RID,BID):
	stake = 0
	bal = web.getBank()
	ELOs=[]
	WR=[]
	con = lite.connect(cfg.dbName)
	with con:
		cur = con.cursor()
		cur.execute("SELECT ELO,WinRate FROM Stats WHERE CharId=?",(RID,))
		try:
			data = cur.fetchall()
		except:
			data = None
		if data != None:
			ELOs.append(data[0][0])
			WR.append(data[0][1])
		cur.execute("SELECT ELO,WinRate FROM Stats Where CharId=?",(BID,))
		try:
			data = cur.fetchall()
		except:
			data = None
		if data != None:
			ELOs.append(data[0][0])
			WR.append(data[0][1])
		p = stats.expectedScore(ELOs[0],ELOs[1])
		if p < 0.5:
# p returned is prob of red winning based on the ELO
# if the p < 0.5 then we bet on blue and need the prob of blue winning
			p = 1 - p
		stake = stats.probBasedBet(p)
		if stake > bal:
			stake = bal
	return stake

def TcalcBet(RID,BID):
	stake = 0
	stake = web.getBank()
	return stake



def confirmBet(choice,stake,MatchId):
	db.createNewBet(MatchId,stake,choice)
    

def settleBet(MatchId,choice,stake,info):
	winAmt = 0
	winner = info["status"]
	bal = db.getCurrentBalance()
	totals = db.getBetTotals(MatchId)
	R = totals[0]
	B = totals[1]
	
	if choice == 1:
		winAmt = float(stake*B)/float(R)
	else:
		winAmt = float(stake*R)/float(B)
	winAmt = math.ceil(winAmt)
	winAmt = int(winAmt)
	print("possible win amount is: "+str(winAmt))
	print("***********************")
	if choice==1 and winner =="1":
		print("Won $" +str(winAmt))
		#red bet and win
	elif choice==1 and winner =="2":
		winAmt = -1*stake
		print("Lost $" + str(stake))
		#red bet and lose
	elif choice==2 and winner =="2":
		print("Won $" +str(winAmt)) 
	elif choice==2 and winner =="1":
		winAmt = -1*stake
		print("Lost $" + str(stake))
		#blue bet and lose
	print("***********************")
	if info["remaining"] != "Tournament mode will be activated after the next match!":
		bal = web.getBank()
	else:
		bal = bal + winAmt

	db.setNewBalance(MatchId,bal)
	db.updateBet(MatchId,winAmt)



def tSettleBet(MatchId,choice,stake,info):
	winAmt = 0
	winner = info["status"]
	bal = db.getCurrentTournamentBalance()
	totals = db.getBetTotals(MatchId)
	R = totals[0]
	B = totals[1]
	if choice == 1:
		winAmt = float(stake*B)/float(R)
	else:
		winAmt = float(stake*R)/float(B)
	winAmt = math.ceil(winAmt)
	winAmt = int(winAmt)
	print("possible win amount is: "+str(winAmt))
	print("***********************")

	if choice==1 and winner =="1":
		print("Won $" +str(winAmt))
	#red bet and win
	elif choice==1 and winner =="2":
		winAmt =-1*stake
		print("Lost $" + str(stake))
	#red bet and lose
	elif choice==2 and winner =="2":
		print("Won $" +str(winAmt)) 
	elif choice==2 and winner =="1":
		winAmt =-1*stake
		print("Lost $" + str(stake))
	#blue bet and lose
	
	print("***********************")
	if info["remaining"] != "25 exhibition matches left!":
		bal = web.getBank()
	else:
		bal = bal + winAmt


	db.setNewTournamentBalance(MatchId,bal)
	db.updateBet(MatchId,winAmt)






