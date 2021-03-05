# getstat.py

import requests

stat_subdomain = 'iprospectman'
stat_key = '6a3cbf9fc980e2695714c5022312bd9ba509d6aa'
stat_base_url = f'https://{stat_subdomain}.getstat.com/api/v2/{stat_key}'

def bulkranks(site_id, iso_date):
        url = f'{stat_base_url}/bulk/ranks?date={iso_date}&site_id={site_id}&engines=google&format=json'
        response = requests.get(url)
        response = response.json()
        job_id = response.get('Response').get('Result').get('Id')
        return job_id