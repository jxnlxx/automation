#check.py
import db

def isNewMessage(newMessage,oldMessage):
    x = "False"
    if newMessage != oldMessage:
        if oldMessage != "":
            x = "True"
    return x
    
def isNewMatch(info):
	x = "False"
	currentIDs = [None, None]
	currentIDs[0] = db.getCharID(info["p1name"])
	currentIDs[1] = db.getCharID(info["p2name"])
	if currentIDs[0] == None or currentIDs[1]==None:
		x = "True"
		return x
	fight = db.getLastFight()
	lastIDs = [None,None]
	lastIDs[0] = fight[2]
	lastIDs[1] = fight[3]
	if currentIDs[0] != lastIDs[0] or currentIDs[1] != lastIDs[1]:
		x = "True"
	return x

def isSameMatch(info):
	x = "False"
	currentIDs = [None, None]
	currentIDs[0] = db.getCharID(info["p1name"])
	currentIDs[1] = db.getCharID(info["p2name"])
	fight = db.getLastFight()
	lastIDs = [None,None]
	lastIDs[0] = fight[2]
	lastIDs[1] = fight[3]
	if currentIDs[0] == lastIDs[0] and currentIDs[1] == lastIDs[1]:
		x = "True"
	return x