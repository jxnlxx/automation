#stats.py
import sqlite3 as lite
import sys
import numpy as np
import cfg
import db
import math

def expectedScore(ELO1,ELO2):
	xp = (0,20,40,60,80,100,120,140,160,180,200,300,400,800)
	fp = (0.5,0.53,0.58,0.62,0.66,0.69,0.73,0.76,0.79,0.82,0.84,0.93,0.97,0.99)
	diff = abs(ELO1 - ELO2)
	if diff > 800:
		return 0.99
	prob = np.interp(diff, xp, fp)
	if ELO1 < ELO2 and ELO1 != ELO2:
		prob = 1 - prob
	return prob

def probBasedBet(p):
	y = 0
	y = math.exp(2*math.log(cfg.maxbet)*(p-0.5))
	return int(round(y))

def batchStats():
    #pull all CharIDs from DB
    con = lite.connect(cfg.dbName)

    with con:
        
        cur = con.cursor()
        cur.execute("SELECT CharId FROM Chars")
        try:
            IDs = cur.fetchall()
        except:
            IDs = None

        if IDs != None:
            CharIDs=[]
            for i in range(0, len(IDs)):
                CharIDs.append(IDs[i][0])
        

        del IDs
        try:
            cur.execute("DROP TABLE Stats")
            cur.execute("CREATE TABLE Stats(CharId INT, ELO INT, FightTotal INT, WinTotal INT, WinRate INT,BetTotal BIGINT, RSW INT, BSW INT, Losses INT)")
        except:
            cur.execute("CREATE TABLE Stats(CharId INT, ELO INT, FightTotal INT, WinTotal INT, WinRate INT,BetTotal BIGINT, RSW INT, BSW INT, Losses INT)")

        for j in CharIDs:
            cur.execute("SELECT * FROM Fight WHERE Red=?",(j,))
            try:
                redJ = cur.fetchall()
            except:
                redJ = None
            cur.execute("SELECT * FROM Fight WHERE Blue=?",(j,))
            try:
                blueJ = cur.fetchall()
            except:
                blueJ = None
            fightTotal = len(redJ)+len(blueJ)
            rMoney= 0
            bMoney = 0
            if len(redJ)>=0:
                for k in range(0,len(redJ)):
                    rMoney=rMoney+redJ[k][5]
            if len(blueJ)>0:
                for k in range(0,len(blueJ)):
                    bMoney=bMoney+blueJ[k][6]
            TMoney = rMoney +bMoney 

            cur.execute("SELECT Winner FROM Fight WHERE Winner=?",(j,))
            try:
                data = cur.fetchall()
            except:
                data = None
            if data != None:
                wins = len(data)
            else:
                wins = 0
            try:    
                winRate = 100*float(wins)/float(fightTotal)
                winRate = int(winRate)
            except:
                winRate=0
            #red side wins
            cur.execute("SELECT Winner FROM Fight WHERE Winner=? AND Red =?",(j,j))
            try:
                data = cur.fetchall()
            except:
                data = None
            if data != None:
                RSW = len(data)
            else:
                RSW = 0
            #blue side wins    
            cur.execute("SELECT Winner FROM Fight WHERE Winner=? AND Blue =?",(j,j))
            try:
                data = cur.fetchall()
            except:
                data = None
            if data != None:
                BSW = len(data)
            else:
                BSW = 0

            losses = fightTotal - wins
            
            cur.execute("INSERT INTO Stats(CharId, ELO, FightTotal, WinTotal, WinRate, BetTotal,RSW,BSW,Losses) VALUES (?,?,?,?,?,?,?,?,?);",(j, 1000,fightTotal,wins,winRate,TMoney,RSW,BSW,losses))
		            
def batchELO():
#calculate the ELO of characters
    #pull all CharIDs from DB
    K=100
    con = lite.connect(cfg.dbName)
    with con:
        cur = con.cursor()
        cur.execute("SELECT * FROM Fight")
        try:
            fights = cur.fetchall()
        except:
            fights = None
            
        if fights != None:
            #grab ELOs of R&B
            for i in range(0,len(fights)):
                winner = fights[i][4]
                if winner == fights[i][2]:
                    loser = fights[i][3]
                else:
                    loser = fights[i][2]
                cur.execute("SELECT ELO FROM Stats WHERE CharID=?",(winner,))
                try:
                    data = cur.fetchall()
                except:
                    data = None
                if data != None:
                    winnerELO = data[0][0]
                cur.execute("SELECT ELO FROM Stats WHERE CharID=?",(loser,))
                try:
                    data = cur.fetchall()
                except:
                    data = None
                if data != None:
                    loserELO = data[0][0]
                p = expectedScore(winnerELO,loserELO)
                #calculate winner's ELO
                newWinnerELO = int(round(winnerELO + K*(1-p)))
                #calculate loser's new ELO
                newLoserELO = int(round(loserELO + K*(0-(1-p))))
                
                cur.execute("UPDATE Stats SET ELO=? WHERE CharId=?", (newWinnerELO,winner ))
                cur.execute("UPDATE Stats SET ELO=? WHERE CharId=?", (newLoserELO,loser ))
                

def ELO(info):
    K=100
    if info["status"] == "1":
        winner = db.getCharID(info["p1name"])
        winnerName = info["p1name"]
        loser = db.getCharID(info["p2name"])
        loserName = info["p2name"]
    elif info["status"] == "2":
        winner = db.getCharID(info["p2name"])
        winnerName = info["p2name"]
        loser = db.getCharID(info["p1name"])
        loserName = info["p1name"]
    con = lite.connect(cfg.dbName)
    with con:
        cur = con.cursor()
        cur.execute("SELECT ELO FROM Stats WHERE CharID=?",(winner,))
        try:
            data = cur.fetchall()
        except:
            data = None
        if data != None:
            winnerELO = data[0][0]
        cur.execute("SELECT ELO FROM Stats WHERE CharID=?",(loser,))
        try:
            data = cur.fetchall()
        except:
            data = None
        if data != None:
            loserELO = data[0][0]
        #get prob of winner winning
        p = expectedScore(winnerELO,loserELO)
        #calculate winner's ELO
        newWinnerELO = int(round(winnerELO + K*(1-p)))
        #calculate loser's new ELO
        newLoserELO = int(round(loserELO + K*(0-(1-p))))
                        
        cur.execute("UPDATE Stats SET ELO=? WHERE CharId=?", (newWinnerELO,winner ))
        cur.execute("UPDATE Stats SET ELO=? WHERE CharId=?", (newLoserELO,loser ))
        print("****************************************")
        print("New ELO for " +winnerName +" is: " + str(newWinnerELO))
        print("****************************************")
        print("New ELO for " +loserName +" is: " + str(newLoserELO))
        print("****************************************")
















