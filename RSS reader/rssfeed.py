import feedparser
import json
import pandas as pd

NewsFeed = feedparser.parse("https://www.dailymail.co.uk/property/index.rss")

output = []

for x in NewsFeed.entries:
	output.append({'title':x.title, 'link':x.link, 'date':x.published})

j = json.dumps(output)

df = pd.read_json(j)

df.to_csv('csv/rssfeed.csv', index = None, header = True)