#%% brandwatch.py

import os
import time
import logging
import pandas as pd
import datetime as dt
from bcr_api.bwproject import BWProject, BWUser
from bcr_api.bwresources import BWQueries

# set date

date = dt.date.today()
last_year = date - dt.timedelta(weeks=52)
last_year = last_year.isoformat()
start_date = date - dt.timedelta(days=date.weekday(), weeks=1)
date = start_date.isoformat()
end_date = start_date + dt.timedelta(days=6)
start_date = f'{date}T00:00:01'
end_date = f'{end_date}T23:59:59'

#%% brandwatch authentication

start_date = '2020-08-12'
end_date = '2021-03-20'

logger = logging.getLogger("bcr_api")

project = BWProject(username="Richard.greenwood@iprospect.com", project="iProspect Manchester")

queries = BWQueries(project)

name = queries.get(name='2000369279')
name = name.get('name')

# %% retrieve brandwatch queries

root = fr"C:\Users\JLee35\dentsu\BetVictor - Documents\Brandwatch"

brandwatch_queries = (os.path.join(root,"Betvictor - Brandwatch Queries.xlsx"))

try:
    queries_list = pd.read_csv(os.path.join(root,"Data",f"{start_date}_{end_date}_betvictor_brandwatch.csv"))
except FileNotFoundError:
    queries_list = pd.read_excel(brandwatch_queries)


queries_index = queries_list[~queries_list['Status'].isin(['Done'])]

for i in queries_index.index:
    query = queries_list['Query'][i]

    print(f'\nUploading query {i+1} of {len(queries_list)}: {query}')
    queries.upload(name=name,
                    language="en",
                    pageType="twitter",
                    booleanQuery=query,
                    startDate=last_year,
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
        filtered = queries.get_mentions(name=name, startDate=start_date, endDate=end_date, pageSize=5000, pageType="twitter", language="en")
    except KeyError:
#   if no results, add done
        queries_list.at[i, 'Status'] = 'Done'
        queries_list.to_csv(os.path.join(root,"Data",f"{start_date}_{end_date}_betvictor_brandwatch.csv"), index=False)
        print('Too many requests. \nSleeping for 10 mins...')
        while n <= 20:
            print(n*30, 'secs')
            time.sleep(30)
            n+=1
        continue

    df = pd.json_normalize(filtered)

    df = df[df['countryCode'].isin(["GBR"])]

    queries_list.at[i, 'Total Mentions'] = len(df)

    if len(df) > 0:
        df['sentiment'] = df['sentiment'].astype(str)
        sentiment = df[df['sentiment'].isin(['positive','negative'])]
        try:    # remove neutral sentiment, and then calculate percentage of positive and negative as if, when combined, they're 100% of the total.
            queries_list.at[i, 'Positive Sentiment'] = round(len(sentiment[sentiment['sentiment'].str.contains('positive')])/len(sentiment)*100,1)
        except (KeyError, ZeroDivisionError) as e:
            queries_list.at[i, 'Positive Sentiment'] = 0
        try:    # remove neutral sentiment, and then calculate percentage of positive and negative as if, when combined, they're 100% of the total.
            queries_list.at[i, 'Negative Sentiment'] = round(len(sentiment[sentiment['sentiment'].str.contains('negative')])/len(sentiment)*100,1)
        except (KeyError, ZeroDivisionError) as e:
            queries_list.at[i, 'Negative Sentiment'] = 0

        try:
            df['classifications'] = df['classifications'].astype(str)
            emotions = df[df['classifications'].str.contains("'classifierId': 'emotions'")]
            queries_list.at[i, 'Joy'] = round(len(emotions[emotions['classifications'].str.contains("'name': 'Joy'")])/len(emotions)*100,1)
            queries_list.at[i, 'Sadness'] = round(len(emotions[emotions['classifications'].str.contains("'name': 'Sadness'")])/len(emotions)*100,1)
            queries_list.at[i, 'Anger'] = round(len(emotions[emotions['classifications'].str.contains("'name': 'Anger'")])/len(emotions)*100,1)
            queries_list.at[i, 'Disgust'] = round(len(emotions[emotions['classifications'].str.contains("'name': 'Disgust'")])/len(emotions)*100,1)
            queries_list.at[i, 'Fear'] = round(len(emotions[emotions['classifications'].str.contains("'name': 'Fear'")])/len(emotions)*100,1)
            queries_list.at[i, 'Surprise'] = round(len(emotions[emotions['classifications'].str.contains("'name': 'Surprise'")])/len(emotions)*100,1)
        except (KeyError, ZeroDivisionError) as e:
            queries_list.at[i, 'Joy'] = 0
            queries_list.at[i, 'Sadness'] = 0
            queries_list.at[i, 'Anger'] = 0
            queries_list.at[i, 'Disgust'] = 0
            queries_list.at[i, 'Fear'] = fear = 0
            queries_list.at[i, 'Surprise'] = 0

    queries_list.at[i, "Status"] = "Done"

    queries_list.to_csv(os.path.join(root,"Data",f"{start_date}_{end_date}_betvictor_brandwatch.csv"), index=False)
    print(f"\nFinished {i+1} of {len(queries_list)}")

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

print('\nDURN!')

# %%
