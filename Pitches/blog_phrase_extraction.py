##
## @author: Jon Lee
## @email: jon.lee@iprospect.com
## @create date: 2020-07-29 11:02:30
##

import feedparser
import spacy
import pandas as pd
from spacy.lang.en.stop_words import STOP_WORDS

stopwords = list(STOP_WORDS)

df = pd.read_excel(r'C:\Users\JLee35\Automation\RSS reader\csv\body_copy.xlsx')

nlp = spacy.load('en_core_web_sm')

titles = ''

for x in df['Body Copy Full']:
#	print(x)
    titles = titles + f'{x}\n'

docx = nlp(titles)

word_frequencies = {}
for word in docx:
    if word.text not in stopwords:
            if word.text not in word_frequencies.keys():
                word_frequencies[word.text] = 1
            else:
                word_frequencies[word.text] += 1

df = pd.DataFrame(list(word_frequencies.items()),columns = ['KEYWORD','FREQUENCY'])
df.to_csv('csv/word_frequencies_new.csv', index = None, header = True)