#main.py

from socketIO_client import SocketIO, LoggingNamespace
import json
import urllib2
import check
import stats
import db
import re
import time
import betting
import web
import mode

gameMode = 1
info =    {'status': 'open', 'alert': '', 'p2name': '', 'p1total': '', 'x': '', 'remaining': '', 'p1name': '', 'p2total': ''}
newInfo = {'status': 'open', 'alert': '', 'p2name': '', 'p1total': '', 'x': '', 'remaining': '', 'p1name': '', 'p2total': ''}
MatchID = 0
stake = 0
choice = 0
jsonurl = "http://www.saltybet.com/state.json"
hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
	'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
		'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
			'Accept-Encoding': 'none',
				'Accept-Language': 'en-US,en;q=0.8',
					'Connection': 'keep-alive'}


def on_message(*data):
	global gameMode
	global info
	global newInfo
	global MatchID
	global stake
	global choice
	info = newInfo

	if len(data)>0:
		if data[0] == '2["message"]':
			print "***************************"
			print "Socket ALERT! Getting JSON"
			u = urllib2.urlopen(urllib2.Request(jsonurl,headers=hdr))
			newInfo = json.loads(u.read())
			newInfo["p1total"] = re.sub(",","",newInfo["p1total"])# take out the commas in the numbers
			newInfo["p2total"] = re.sub(",","",newInfo["p2total"])
			gameMode = mode.checkMode(gameMode,newInfo) # get the game mode; matchmaking, tournament, exhibitions
			if gameMode != 3:
				if check.isNewMessage(newInfo,info) == "True":# check to see if a duplicate message was received
					if newInfo["status"] == "open":
						print "Bets are open!"
						time.sleep(2)
						if check.isSameMatch(newInfo) == "True":
							fight = db.getLastFight()
							MatchID = fight[0]
							RID = fight[2]
							BID = fight[3]
							choice = betting.chooseFighter(newInfo,RID,BID)# choose who to bet on
							if gameMode ==1:#matchmaking
								stake = betting.calcBet(RID,BID)
							elif gameMode == 2: #tournament
								stake = betting.TcalcBet(RID,BID)
							if choice == 1:
								print "Betting $" +str(stake) + " on " + newInfo["p1name"]
							elif choice == 2:
								print "Betting $" +str(stake) + " on " + newInfo["p2name"]
							web.placeBet(choice,stake)
								
					
					elif newInfo["status"] == "locked":
						print "Bets are locked"
						if MatchID != 0:
							betting.confirmBet(choice,stake,MatchID)
						else:
							print "missed betting"
					
					elif newInfo["status"] == "1" or newInfo["status"] == "2":
						print "Winner Announced"
						if MatchID != 0:
							if gameMode ==1:#matchmaking
								betting.settleBet(MatchID,choice,stake,newInfo)
							elif gameMode == 2: #tournament
								betting.tSettleBet(MatchID,choice,stake,newInfo)
							MatchID = 0
						else:
							print "missed betting"

				else:
					print("Duplicate Message Received")
			else:
				print("Currently Exhibition Mode, waiting for Matchmaking")
			print "***************************"


while True:
	try:
		socketIO = SocketIO('www-cdn-twitch.saltybet.com',1337, LoggingNamespace)
		socketIO.on('message', on_message)
		socketIO.wait()
	except:
		print("No connection, trying again in 10s")
		time.sleep(10)