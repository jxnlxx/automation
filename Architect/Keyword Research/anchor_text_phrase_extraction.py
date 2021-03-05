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

start_time = datetime.datetime.now()

kw_input = pd.read_csv( fr'L:\Commercial\Operations\Technical SEO\Automation\Keyword Research\BBC Food - Lurpak - Jon Lee KW Frequency Data.csv' )

print('Getting Phrases')
list_of_keywords = list(filter(None, kw_input.Anchor.tolist()))
list_of_phrases = [str(kw).split(" ") for kw in list_of_keywords]

#print(list_of_uk_keywords)

phrase_counts = collections.Counter()
print('Counting')

for phrase in list_of_phrases:
    phrase_counts.update(nltk.ngrams(phrase,1))
    phrase_counts.update(nltk.ngrams(phrase,2))
    phrase_counts.update(nltk.ngrams(phrase,3))
    phrase_counts.update(nltk.ngrams(phrase,4))

print('Filtering for Most Common Phrases')

top_phrases = phrase_counts.most_common(5001)

#print(top_200_uk)
top_phrases = pd.DataFrame(top_phrases, columns=["keyword", "count"])
top_phrases = top_phrases[ top_phrases['keyword'] != 'nan' ]

print('Formatting Phrases')

def fix_those_brackets(keyword):
    fixed_keyword=' '.join(keyword)
    return fixed_keyword

# Now we're not writing our function any more (you can tell because of the indent)


top_phrases['keyword']=top_phrases.keyword.apply(lambda keyword: fix_those_brackets(keyword))

# =============================================================================
# print('Aggregating Search Volumes')
# 
# def find_total_search_volume(keyword, original_keyword_data, keyword_column, search_volume_column):
#     
# #     Here we just create a copy of our original keyword data, so we don't accidentally overwrite it
#     keyword_data_copy=original_keyword_data.copy(deep=True)
#     
# #     This whole bit is to make sure the search volume is always a number and handle times when the tool we used doesn't have 
# #     search volume data for the keyword
#     keyword_data_copy[search_volume_column] = pd.to_numeric(keyword_data_copy[search_volume_column], errors='coerce')   
#     keyword_data_copy[search_volume_column] = keyword_data_copy[search_volume_column].fillna(0)
#     keyword_data_copy[search_volume_column] = keyword_data_copy[search_volume_column].astype(int)  
#     
# #     This is exactly the same as the "str.contains" check we looked at before - for each keyword in our top_200 list we'll
# #     check what rows in our original keyword data contain that keyword
#     keyword_data_copy['contains_keyword?']=keyword_data_copy[keyword_column].str.contains(keyword)
#     
# #     Now we're looking at ONLY the rows in our original keyword data which contain the particular keyword we're looking at
#     keyword_data_copy=keyword_data_copy[(keyword_data_copy['contains_keyword?']==True)]
#     
# #     Now we sum together all the search volume for every keyword that matches
#     search_vol=keyword_data_copy[search_volume_column].sum()
#     
# #     We return the total search volume for wherever that keyword appeared
#     return search_vol
# 
# top_phrases['combined volume'] = top_phrases.keyword.apply(lambda x: find_total_search_volume(x, kw_input, 'Keyword', 'Search Volume'))
# 
# print('Filtering Search Volumes')
# 
# top_phrases = top_phrases[top_phrases['combined volume'] >= 500 ]
# =============================================================================

top_phrases.to_csv(r'L:\Commercial\Operations\Technical SEO\Automation\Keyword Research\anchor_text_phrase_extraction_output.csv', index=False )

print('DURN!')