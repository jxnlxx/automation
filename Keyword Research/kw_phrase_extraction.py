# kw_phrase_extraction.py
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 13:44:47 2020
@author: JLee35

"""

import pandas as pd
import re
import nltk
import datetime
import collections
from nltk.corpus import stopwords

kw_input = pd.read_csv( fr'C:\Users\JLee35\OneDrive - Dentsu Aegis Network\PROJECTS\New Business\bankrate.com\stat_keywords.csv')

print('Getting Phrases')
list_of_keywords = list(filter(None, kw_input.Keyword.tolist()))
list_of_phrases = [str(kw).split(" ") for kw in list_of_keywords]

phrase_counts = collections.Counter()
print('Counting')
for phrase in list_of_phrases:
    phrase_counts.update(nltk.ngrams(phrase,1))
    phrase_counts.update(nltk.ngrams(phrase,2))
    phrase_counts.update(nltk.ngrams(phrase,3))

# Unbracket keywords
print('Filtering for Most Common Phrases')
top_phrases = phrase_counts.most_common(20000)
top_phrases = pd.DataFrame(top_phrases, columns=['keyword', 'count'])
top_phrases = top_phrases[top_phrases['keyword'] != 'nan']

print('Formatting Phrases')
def unbracket(keyword):
    fixed_keyword = ' '.join(keyword)
    return fixed_keyword

top_phrases['keyword'] = top_phrases.keyword.apply(lambda x: unbracket(x))

# Remove keyword that are in our list of stopwords
stop = stopwords.words('english')
top_phrases = top_phrases[~top_phrases['keyword'].isin(stop)]

top_phrases.to_csv(r'C:\Users\JLee35\OneDrive - Dentsu Aegis Network\PROJECTS\New Business\bankrate.com\stat_most_common.csv', index=False)

print('DURN!')