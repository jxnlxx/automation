# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 13:44:47 2020

@author: JLee35
"""

import pandas as pd
import re
import nltk
import collections
import datetime

df = pd.read_excel(r'C:\Users\JLee35\Automation\RSS reader\csv\body_copy.xlsx')

print('Getting Phrases')
kw_list = list(filter(None, df['Body Copy Full'].tolist()))
phrase_list = [str(kw).split(" ") for kw in kw_list]
phrase_counts = collections.Counter()

print('Counting Phrases')
for phrase in phrase_list:
    phrase_counts.update(nltk.ngrams(phrase,1))
    phrase_counts.update(nltk.ngrams(phrase,2))
    phrase_counts.update(nltk.ngrams(phrase,3))

n = 5000
print(f'Filtering for {n} Most Common Phrases')
top_phrases = phrase_counts.most_common(n)
top_phrases = pd.DataFrame(top_phrases, columns=["keyword", "count"])
top_phrases = top_phrases[top_phrases['keyword'] != 'nan']

print('Formatting Phrases')
def fix_brackets(keyword):
    fixed_keyword = ' '.join(keyword)
    return fixed_keyword

top_phrases['keyword']=top_phrases.keyword.apply(lambda keyword: fix_brackets(keyword))

top_phrases.to_csv(r'C:\Users\JLee35\Automation\RSS reader\csv\blog_phrase_extraction2.csv', index=False )

print('DURN!')