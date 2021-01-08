#brandwatch.py


#%%
from bcr_api.bwproject import BWProject, BWUser
from bcr_api.bwresources import BWQueries, BWGroups, BWAuthorLists, BWSiteLists, BWLocationLists, BWTags, BWCategories, BWRules, BWMentions, BWSignals
import datetime
import logging
import pandas as pd
import time

logger = logging.getLogger("bcr_api")

project = BWProject(username="Richard.greenwood@iprospect.com", project="iProspect Manchester")
queries = BWQueries(project)

start = "2018-12-01" + "T05:00:00"
today = (datetime.date.today() + datetime.timedelta(days=1)).isoformat() + "T05:00:00"

#%%

queries_list = pd.read_csv(r'C:\Users\JLee35\Automation\Brandwatch\musicMagpie - Thirstiest Tweets All Queries.csv')
queries_index = queries_list[~queries_list['Status'].isin(['Done'])]

for i in queries_index.index:
    query = queries_list['Query'][i]

    print(f'\nUploading query {i+1} of {len(queries_list)}: {query}')
    queries.upload(name="ENGAGE - Thirsty Tweets",
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
        filtered = queries.get_mentions(name = "ENGAGE - Thirsty Tweets", startDate=start, endDate=today, pageSize=5000, pageType="twitter", language="en")
    except KeyError:
        queries_list['Status'][i] = 'Done'
        queries_list.to_csv(r'C:\Users\JLee35\Automation\Brandwatch\musicMagpie - Thirstiest Tweets All Queries.csv', index=False)
        print('\nToo many requests. \nSleeping for 10 mins...')
        while n <= 20:
            print(n*30, 'secs')
            time.sleep(30)
            n+=1
        continue

    df = pd.json_normalize(filtered)

    total_mentions = len(df)

    if total_mentions > 0:
        positive_sentiment = round(len(df[df['sentiment'].str.contains('positive')])/total_mentions*100)
        negative_sentiment = round(len(df[df['sentiment'].str.contains('negative')])/total_mentions*100)
    else:
        positive_sentiment = 0
        negative_sentiment = 0

    try:
        df['classifications'] = df['classifications'].astype(str)
        emotions = df[df['classifications'].str.contains("'classifierId': 'emotions'")]
        joy = round(len(emotions[emotions['classifications'].str.contains("'name': 'Joy'")])/len(emotions)*100)
        sadness = round(len(emotions[emotions['classifications'].str.contains("'name': 'Sadness'")])/len(emotions)*100)
        anger = round(len(emotions[emotions['classifications'].str.contains("'name': 'Anger'")])/len(emotions)*100)
        disgust = round(len(emotions[emotions['classifications'].str.contains("'name': 'Disgust'")])/len(emotions)*100)
        fear = round(len(emotions[emotions['classifications'].str.contains("'name': 'Fear'")])/len(emotions)*100)
        surprise = round(len(emotions[emotions['classifications'].str.contains("'name': 'Surprise'")])/len(emotions)*100)
    except KeyError:
        joy = 0
        sadness = 0
        anger = 0
        disgust = 0
        fear = 0
        surprise = 0
        pass

    queries_list['Total Mentions'][i] = total_mentions
    queries_list['Positive Sentiment'][i] = positive_sentiment
    queries_list['Negative Sentiment'][i] = negative_sentiment
    queries_list['Joy'][i] = joy
    queries_list['Sadness'][i] = sadness
    queries_list['Anger'][i] = anger
    queries_list['Disgust'][i] = disgust
    queries_list['Fear'][i] = fear
    queries_list['Surprise'][i] = surprise
    queries_list['Status'][i] = 'Done'

    queries_list.to_csv(r'C:\Users\JLee35\Automation\Brandwatch\musicMagpie - Thirstiest Tweets All Queries.csv', index=False)
    print(f'\nFinished {i+1} of {len(queries_list)}')


    if total_mentions >= 80000:
        print('Requests over threshold. \nSleeping for 10 mins...')
        while n <= 20:
            print(n*30, 'secs')
            time.sleep(30)
            n+=1
    elif 50000 <= total_mentions <= 79999:
        print('Requests over threshold. \nSleeping for 5 mins...')
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
