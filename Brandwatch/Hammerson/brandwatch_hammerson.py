#brandwatch.py


#%%
import os
import time
import logging
import calendar
import pandas as pd
import datetime as dt
from bcr_api.bwproject import BWProject
from bcr_api.bwresources import BWQueries
from operator import itemgetter, attrgetter

# file paths

brandwatch_path = fr"K:\Investment\iProspect\Social Media\15. Brandwatch Reporting"

# dates

date = dt.date.today().replace(day=1) - dt.timedelta(days=1)

year = int(date.strftime("%Y"))
month = int(date.strftime("%m")) - 1

filter_start = date.replace(day=1).strftime("%Y-%m-%d")+"T00:00:00"
filter_end = date.strftime("%Y-%m-%d")+"T23:59:59"

query_start = date.replace(day=1) - dt.timedelta(days=1) # sets it 1 month before
query_start = query_start.replace(day=1) - dt.timedelta(days=1) # again to set it 2 months before
query_start = query_start.replace(day=1).strftime("%Y-%m-%d")+"T00:00:00"

save_date = date.strftime("%Y-%m")

project = BWProject(username="Richard.greenwood@iprospect.com", project="iProspect Manchester")
queries = BWQueries(project)
logger = logging.getLogger("bcr_api")

#%%

root = fr"C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Social Media\Organic Social & Influencers\Product\Organic Social\Brandwatch Automation\Hammerson"
save_path = fr"{year}\Brandwatch Report {save_date}.csv"

try:
    queries_list = pd.read_csv(os.path.join(root,save_path))
except FileNotFoundError:
    queries_list = pd.read_csv(os.path.join(root, "Brandwatch Report Template.csv"))
    if not os.path.exists(os.path.join(root,f"{year}")):
        os.mkdir(os.path.join(root,f"{year}"))
    queries_list.to_csv(os.path.join(root,save_path), index=False)

queries_index = queries_list[~queries_list["Status"].isin(["Done"])]

#%%

for i in queries_index.index:
    query = queries_list["Query"][i]

    print(f"\nUploading query {i+1} of {len(queries_list)}: {query}")
    queries.upload(name="DO NOT DELETE: AUTOMATION",
                    language="en",
                    pageType=["blog","forum","news","twitter","tumblr"],
                    booleanQuery=query,
                    startDate=query_start,
                    modify_only=True
                    )

    print("\nWaiting 5 mins for query to backfill...")
    n=0
    while n < 11:
        print(n*30, "secs")
        time.sleep(30)
        n+=1

    print(f"\nRequesting query {i+1} of {len(queries_list)}: {query}\n")
    try:
        filtered = queries.get_mentions(name="DO NOT DELETE: AUTOMATION",
                    language="en",
                    pageType=["blog","forum","news","twitter","tumblr"],
                    startDate=filter_start,
                    endDate=filter_end,
                    pageSize=5000,)
    except KeyError:
        queries_list.at[i, "Status"] = "Done"
        queries_list.to_csv(os.path.join(root, save_path), index=False)
        print("\nToo many requests. \nSleeping for 10 mins...")
        while n <= 20:
            print(n*30, "secs")
            time.sleep(30)
            n+=1
            continue

    df = pd.json_normalize(filtered)
    df = df[df["countryCode"].isin(["GBR","IRL"])]

    if len(df) > 0:
        queries_list.at[i,"Total Mentions"] = len(df)
        queries_list.at[i,"Potential Impressions"] = df["impressions"].sum()
        queries_list.at[i,"Positive Sentiment"] = round(len(df[df["sentiment"].str.contains("positive")])/len(df)*100, 1)
        queries_list.at[i,"Negative Sentiment"] = round(len(df[df["sentiment"].str.contains("negative")])/len(df)*100, 1)
        blog = ("blog", round(len(df[df["pageType"].str.contains("blog")])/len(df)*100, 1))
        forum = ("forum", round(len(df[df["pageType"].str.contains("forum")])/len(df)*100, 1))
        news = ("news", round(len(df[df["pageType"].str.contains("news")])/len(df)*100, 1))
        twitter = ("twitter", round(len(df[df["pageType"].str.contains("twitter")])/len(df)*100, 1))
        tumblr = ("tumblr", round(len(df[df["pageType"].str.contains("tumblr")])/len(df)*100, 1))
        sources = [blog,forum,news,twitter,tumblr]
        sources = sorted(sources,key=itemgetter(1))
        queries_list.at[i,"Top Source (%)"] = sources[-1]
    else:
        queries_list.at[i,"Total Mentions"] = 0
        queries_list.at[i,"Potential Impressions"] = 0
        queries_list.at[i,"Positive Sentiment"] = 0
        queries_list.at[i,"Negative Sentiment"] = 0
        queries_list.at[i,"Top Source (%)"] = "n/a"

    queries_list.at[i,"Status"] = "Done"
    queries_list.to_csv(os.path.join(root, save_path), index=False)

    print(f"\nFinished {i+1} of {len(queries_list)}")

    if len(df) >= 80000:
        print("Requests over threshold.\nSleeping for 10 mins...")
        while n <= 20:
            print(n*30, "secs")
            time.sleep(30)
            n+=1
    elif 50000 <= len(df) <= 79999:
        print("Requests over threshold.\nSleeping for 5 mins...")
        while n <= 10:
            print(n*30, "secs")
            time.sleep(30)
            n+=1
    else:
        pass

# calculate SoV

queries_list = pd.read_csv(os.path.join(root, save_path))

mentions_sum = int(queries_list["Total Mentions"].sum())
for i in queries_list.index:
    share_of_voice = round(queries_list["Total Mentions"][i]/mentions_sum*100, 1)
    queries_list.at[i, "Share of Voice (%)"] = share_of_voice

queries_list.to_csv(os.path.join(root, save_path), index=False)

print("\nDURN!")

#%%