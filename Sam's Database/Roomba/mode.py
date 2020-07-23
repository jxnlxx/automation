#mode.py
import re
import db
import stats
import cfg

def checkMode(currentMode,info):
	text = info["remaining"]
	mode = None
	try:#check for exhibition match to change mode stop weird data
		exhibtest = re.search(r"\d* (exhibition matches left!)", text)
		if exhibtest.group(1) == "exhibition matches left!":
			if currentMode ==2:
				if info["status"] == "1" or info["status"] == "2":
					print("********************************")
					print("tournament over, adding winnings")
					print("********************************")
					db.syncBalance()
				else:
					mode = 3
			else:
				mode = 3
	except:
		error = 1
	try:#check for exhibition match to change mode stop weird data
		exhibtest = re.search(r"(Matchmaking mode will be activated after the next exhibition match!)", text)
		if exhibtest.group(1) == "Matchmaking mode will be activated after the next exhibition match!":
			mode =3
	except:
		error = 1.5
	try:#check for matchmaking to change mode
		exhibtest = re.search(r"\d* (more matches until the next tournament!)", text)
		if exhibtest.group(1) == "more matches until the next tournament!":
			mode = 1
	except:
		error = 2
	try:#check for matchmaking to change mode
		exhibtest = re.search(r"(Tournament mode will be activated after the next match!)", text)
		if exhibtest.group(1) == "Tournament mode will be activated after the next match!":
			mode = 1
			db.syncBalance()
	except:
		error = 2
	try:#check for tournament to change mode
		exhibtest = re.search(r"\d* (characters are left in the bracket!)", text)
		if exhibtest.group(1) == "characters are left in the bracket!":
			if currentMode ==1:
				mode = 2
				bal = web.getBank()
				db.setNewTournamentBalance(0,bal)
			else:
				mode =2
	except:
		error = 3
	try:#check for tournament to change mode
		exhibtest = re.search(r"(16 characters are left in the bracket!)", text)
		if exhibtest.group(1) == "16 characters are left in the bracket!":
			if currentMode ==1:
				mode = 2
			
			else:
				mode =2
	except:
		error = 3.5
	try:#check for tournament to change mode
		exhibtest = re.search(r"(FINAL ROUND! Stay tuned for exhibitions after the tournament!)", text)
		if exhibtest.group(1) == "FINAL ROUND! Stay tuned for exhibitions after the tournament!":
			mode = 2
	except:
		error = 4
	if mode == None:
		mode = currentMode
		print "default mode acquired"
	return mode
