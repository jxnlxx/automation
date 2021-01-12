# sharepoint_login.py
# %%

import requests
from requests.auth import HTTPDigestAuth
from requests_oauthlib import OAuth1

username = 'jon.lee@iprospect.com'
password = 'I0psinen42'
#s = sharepy.connect('https://globalappsportal.sharepoint.com/sites/MonthlyHealthchecks/', username=username, password=password)
response = requests.get('https://globalappsportal.sharepoint.com/sites/MonthlyHealthchecks/', auth=HTTPDigestAuth(username, password))
#response = requests.get('https://globalappsportal.sharepoint.com/sites/MonthlyHealthchecks/')

# %%

# %%
