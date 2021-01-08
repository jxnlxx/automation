#brandwatch.py


#%%
from bcr_api.bwproject import BWProject, BWUser
from bcr_api.bwresources import BWQueries
import datetime as dt
import logging
import pandas as pd
import time

logger = logging.getLogger("bcr_api")

project = BWProject(username="Richard.greenwood@iprospect.com", project="iProspect Manchester")

queries = BWQueries(project)

name = queries.get(name='2000369279')
name = name.get('name')

date = dt.date.today()
date = date - dt.timedelta(days=date.weekday(), weeks=1)
end = date + dt.timedelta(days=6)
start = f'{date}T00:00:01'
end = f'{end}T23:59:59'
date = date.isoformat()

#%%

try:
    queries_list = pd.read_csv(fr'C:\Users\JLee35\Automation\Brandwatch\BetVictor\{date}_betvictor_mta_manager_queries.csv')
except FileNotFoundError:
    queries_list = pd.read_csv(fr'C:\Users\JLee35\Automation\Brandwatch\BetVictor\betvictor_mta_manager_queries_template.csv')

queries_index = queries_list[~queries_list['Status'].isin(['Done'])]

for i in queries_index.index:
    query = queries_list['Query'][i]

    print(f'\nUploading query {i+1} of {len(queries_list)}: {query}')
    queries.upload(name=name,
                    language="en",
                    pageType="twitter",
                    booleanQuery=query,
                    startDate=start,
                    modify_only=True
                    )

    print('\nWaiting 5 mins for query to backfill...')
    n=0
    while n < 11:
        print(n*30, 'secs')
        time.sleep(30)
        n+=1

    print(f'\nRequesting query {i+1} of {len(queries_list)}: {query}\n')
    try:
        filtered = queries.get_mentions(name=name, startDate=start, endDate=end, pageSize=5000, pageType="twitter", language="en")
    except KeyError:
        queries_list['Status'][i] = 'Done'
        queries_list.to_csv(fr'C:\Users\JLee35\Automation\Brandwatch\BetVictor\{date}_betvictor_mta_manager_queries.csv', index=False)
        print('Too many requests. \nSleeping for 10 mins...')
        while n <= 20:
            print(n*30, 'secs')
            time.sleep(30)
            n+=1
        continue

    df = pd.json_normalize(filtered)

    #df_to.csv(fr'C:\Users\JLee35\Automation\Brandwatch\BetVictor\filtered.csv') # for checking received data

    df = df[df['countryCode'].isin(["GBR"])]
    queries_list['Total Mentions'][i] = len(df)

    if len(df) > 0:
        queries_list['Positive Sentiment'][i] = round(len(df[df['sentiment'].str.contains('positive')])/len(df)*100,1)
        queries_list['Negative Sentiment'][i] = round(len(df[df['sentiment'].str.contains('negative')])/len(df)*100,1)
    else:
        queries_list['Positive Sentiment'][i] = 0
        queries_list['Negative Sentiment'][i] = 0

    try:
        df['classifications'] = df['classifications'].astype(str)
        emotions = df[df['classifications'].str.contains("'classifierId': 'emotions'")]
        queries_list['Joy'][i] = round(len(emotions[emotions['classifications'].str.contains("'name': 'Joy'")])/len(emotions)*100,1)
        queries_list['Sadness'][i] = round(len(emotions[emotions['classifications'].str.contains("'name': 'Sadness'")])/len(emotions)*100,1)
        queries_list['Anger'][i] = round(len(emotions[emotions['classifications'].str.contains("'name': 'Anger'")])/len(emotions)*100,1)
        queries_list['Disgust'][i] = round(len(emotions[emotions['classifications'].str.contains("'name': 'Disgust'")])/len(emotions)*100,1)
        queries_list['Fear'][i] = round(len(emotions[emotions['classifications'].str.contains("'name': 'Fear'")])/len(emotions)*100,1)
        queries_list['Surprise'][i] = round(len(emotions[emotions['classifications'].str.contains("'name': 'Surprise'")])/len(emotions)*100,1)
    except KeyError:
        queries_list['Joy'][i] = 0
        queries_list['Sadness'][i] = 0
        queries_list['Anger'][i] = 0
        queries_list['Disgust'][i] = 0
        queries_list['Fear'][i] = fear = 0
        queries_list['Surprise'][i] = 0
        pass

    queries_list['Status'][i] = 'Done'

    queries_list.to_csv(fr'C:\Users\JLee35\Automation\Brandwatch\BetVictor\{date}_betvictor_mta_manager_queries.csv', index=False)
    print(f'\nFinished {i+1} of {len(queries_list)}')

    if len(df) >= 80000:
        print('\nRequests over threshold. \nSleeping for 10 mins...')
        while n <= 20:
            print(n*30, 'secs')
            time.sleep(30)
            n+=1
    elif 50000 <= len(df) <= 79999:
        print('\nRequests over threshold. \nSleeping for 5 mins...')
        while n <= 10:
            print(n*30, 'secs')
            time.sleep(30)
            n+=1
    else:
        pass
#    break
print('\nDURN!')

#%%

# %%
