import feedparser
import spacy
import pandas as pd
from spacy.lang.en.stop_words import STOP_WORDS

stopwords = list(STOP_WORDS)

NewsFeed = feedparser.parse("https://www.dailymail.co.uk/property/index.rss")

nlp = spacy.load('en_core_web_sm')

titles = ""

for x in NewsFeed.entries:
	titles += x.title+'\n'

docx = nlp(titles)

word_frequencies = {}
for word in docx:
    if word.text not in stopwords:
            if word.text not in word_frequencies.keys():
                word_frequencies[word.text] = 1
            else:
                word_frequencies[word.text] += 1

df = pd.DataFrame(list(word_frequencies.items()),columns = ['KEYWORD','FREQUENCY'])
df.to_csv('csv/word_frequencies.csv', index = None, header = True)