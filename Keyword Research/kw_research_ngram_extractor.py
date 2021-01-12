#%% kw_research_ngram_extractor.py
'''
Created on Tue Mar 24 13:44:47 2020

@author: JLee35
'''

import pandas as pd
import re
import nltk
import collections
import datetime
import tkinter as tk
from nltk.corpus import stopwords

start_time = datetime.datetime.now()

root = tk.Tk()

filename = tk.filedialog.askopenfilename(title='Select File', filetypes=(('All Files','*.*'),('Excel files','*.xlsx')))

root.destroy()

print(f'Opening {filename}')

if filename.endswith('.csv'):
    kw_list = pd.read_csv(fr'{filename}')
elif filename.endswith(('.xls','.xlsx')):
    kw_list = pd.read_excel(fr'{filename}')
else:
    print('File type not supported. \nPlease re-run script and select a different file.')
    raise TypeError

# remove n-gram isomers
print('Removing keyword isomers...')
kw_list['isomers'] = [', '.join(sorted(row.split(), key=str.lower)) for row in kw_list['keyword']]
kw_list = kw_list.drop_duplicates(subset = ['search volume', 'isomers'], keep='first')
kw_list = kw_list.drop(labels='isomers', axis=1)

print('Getting Phrases')
list_of_keywords = list(filter(None, kw_list.keyword.tolist()))
list_of_phrases = [str(kw).split(' ') for kw in list_of_keywords]

print('Counting')
phrase_counts = collections.Counter()

for phrase in list_of_phrases:
    phrase_counts.update(nltk.ngrams(phrase, 1)) # unigrams
    phrase_counts.update(nltk.ngrams(phrase, 2)) # bigrams
    phrase_counts.update(nltk.ngrams(phrase, 3)) # trigrams
    phrase_counts.update(nltk.ngrams(phrase, 4)) # quadrigrams

print('Filtering for Most Common Phrases')
top_phrases = phrase_counts.most_common(10000)
top_phrases = pd.DataFrame(top_phrases, columns=['keyword', 'count'])
top_phrases = top_phrases[top_phrases['keyword'] != 'nan']

print('Formatting Phrases')
def fix_those_brackets(keyword):
    fixed_keyword = ' '.join(keyword)
    return fixed_keyword

# Now we're not writing our function any more (you can tell because of the indent)
top_phrases['keyword'] = top_phrases.keyword.apply(lambda keyword: fix_those_brackets(keyword))

# Remove keyword that are in our list of stopwords
stop = stopwords.words('english')
top_phrases = top_phrases[~top_phrases['keyword'].isin(stop)]

print('Aggregating Search Volumes')
print('(This may take a while...)')

def find_total_search_volume(keyword, original_keyword_data, keyword_column, search_volume_column):

#     Here we just create a copy of our original keyword data, so we don't accidentally overwrite it
    keyword_data_copy = original_keyword_data.copy(deep=True)

#     This whole bit is to make sure the search volume is always a number and handle times when the tool we used doesn't have
#     search volume data for the keyword
    keyword_data_copy[search_volume_column] = pd.to_numeric(keyword_data_copy[search_volume_column], errors='coerce')
    keyword_data_copy[search_volume_column] = keyword_data_copy[search_volume_column].fillna(0)
    keyword_data_copy[search_volume_column] = keyword_data_copy[search_volume_column].astype(int)

#     This is exactly the same as the 'str.contains' check we looked at before - for each keyword in our top_200 list we'll
#     check what rows in our original keyword data contain that keyword
    keyword_data_copy['contains_keyword?'] = keyword_data_copy[keyword_column].str.contains(keyword)

#    filter rows in our original keyword data containing particular keyword we're looking at
    keyword_data_copy = keyword_data_copy[(keyword_data_copy['contains_keyword?'] == True)]

#     sum all search volumes for keywords that match
    search_vol = keyword_data_copy[search_volume_column].sum()

#     return total search volume for wherever that keyword appeared
    return search_vol

top_phrases['combined volume'] = top_phrases.keyword.apply(lambda x: find_total_search_volume(x, kw_list, 'keyword', 'search volume'))

top_phrases['keyword'] = top_phrases['keyword'].astype(str)

#print('Filtering Search Volumes')

#top_phrases = top_phrases[top_phrases['combined volume'] >= 500]

root = tk.Tk()

savename = tk.filedialog.asksaveasfilename(title='Save File',  defaultextension='.xlsx', filetypes =(('Excel Workbook','*.xlsx'),))

root.destroy()

top_phrases.to_excel(fr'{savename}', index=False)

print('DURN!')
# %%
