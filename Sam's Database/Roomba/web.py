#web.py
import requests
import re
import cfg

def placeBet(choice,stake):
    print("Sending bet to server")
    

    with requests.Session() as r:
        r.get(cfg.url+cfg.signin)
        cfduid = r.cookies['__cfduid']
        phpsessid = r.cookies['PHPSESSID']
        saltycookies = dict(__cfduid=cfduid,PHPSESSID=phpsessid)
        login_data = dict(email=cfg.USERNAME, pword=cfg.PASSWORD,authenticate='signin')
        r.post(cfg.url+cfg.signin,login_data, cookies=saltycookies, headers={"Referer":cfg.url})
        
        payload = {'selectedplayer': 'player'+ str(choice), 'wager': str(stake)}
        r = requests.post(cfg.url+cfg.placebet, data=payload, cookies=saltycookies)
    print("Bet sent to server")
        
def getBank():
    
    bal =0
    
    with requests.Session() as r:
        page = r.get(cfg.url+cfg.signin)
        cfduid = r.cookies['__cfduid']
        phpsessid = r.cookies['PHPSESSID']
        saltycookies = dict(__cfduid=cfduid,PHPSESSID=phpsessid)
        login_data = dict(email=cfg.USERNAME, pword=cfg.PASSWORD,authenticate='signin')
        r.post(cfg.url+cfg.signin,login_data, cookies=saltycookies, headers={"Referer":cfg.url})
        
        page = r.get(cfg.url, cookies=saltycookies, headers={"Referer":cfg.url})
        #print page.content
        bal1 = re.findall(r'<span class="dollar" id="balance">(\d{1,3}(,\d{3})*)<', str(page.content))[0][0]
        bal = int(re.sub(",","",bal1))
    return bal
        
def getWin():
    
    win =0
    
    with requests.Session() as r:
        r.get(cfg.url+cfg.signin)
        cfduid = r.cookies['__cfduid']
        phpsessid = r.cookies['PHPSESSID']
        saltycookies = dict(__cfduid=cfduid,PHPSESSID=phpsessid)
        login_data = dict(email=cfg.USERNAME, pword=cfg.PASSWORD,authenticate='cfg.signin')
        r.post(cfg.url+cfg.signin,login_data, cookies=saltycookies, headers={"Referer":cfg.url})
        
        page = r.get(cfg.url, cookies=saltycookies, headers={"Referer":cfg.url})
        print page.content
        win = re.findall(r'\<span class="greentext">\+\$(\d*)<', page.content)[0]
        print win
        win = int(win)
    return win
