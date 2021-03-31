#%% kw_research_ngram_extractor.py
'''
Created on Tue Mar 24 13:44:47 2020

@author: JLee35
'''

import re
import nltk
import datetime
import collections
import pandas as pd
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

# convert column names to lower case
kw_list.columns = kw_list.columns.str.lower()
kw_list['keyword'] = kw_list['keyword'].astype(str)

# replace instances of '+' with 'plus' so find_total_search_volume() works later (issues with to regex search)
kw_list['keyword'] = kw_list['keyword'].str.replace("  "," ")
kw_list['keyword'] = kw_list['keyword'].str.strip()

#save edited kw_list
if filename.endswith('.csv'):
    kw_list.to_csv(fr'{filename}')
elif filename.endswith(('.xls','.xlsx')):
    kw_list.to_excel(fr'{filename}')

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
top_phrases = phrase_counts.most_common(50000)
top_phrases = pd.DataFrame(top_phrases, columns=['keyword', 'count'])
top_phrases = top_phrases[top_phrases['keyword'] != 'nan']

print('Formatting Phrases')
def fix_those_brackets(keyword):
    fixed_keyword = ' '.join(keyword)
    return fixed_keyword

top_phrases['keyword'] = top_phrases.keyword.apply(lambda keyword: fix_those_brackets(keyword)).astype(str)

# Remove keyword that are in our list of stopwords
stop = stopwords.words('english')
top_phrases = top_phrases[~top_phrases['keyword'].isin(stop)]
top_phrases = top_phrases.reset_index(drop=True)
top_phrases = top_phrases.head(10000)

print('Aggregating Search Volumes')
print('(This may take a while...)')

def find_total_search_volume(keyword, original_keyword_data, keyword_column, search_volume_column):

#     Print keyword to show
    print(keyword)
#     create a copy of our original keyword data, so we don't accidentally overwrite it
    keyword_data_copy = original_keyword_data.copy(deep=True)
#     ensure search volume is always a number and handle times when we don't have volume data for the keyword
    keyword_data_copy[search_volume_column] = pd.to_numeric(keyword_data_copy[search_volume_column], errors='coerce')
    keyword_data_copy[search_volume_column] = keyword_data_copy[search_volume_column].fillna(0)
    keyword_data_copy[search_volume_column] = keyword_data_copy[search_volume_column].astype(int)

#     for each keyword, check what rows in our original keyword data contain that keyword
    keyword_data_copy['contains_keyword?'] = keyword_data_copy[keyword_column].str.contains(re.escape(keyword))

#    filter rows in our original keyword data containing particular keyword we're looking at
    keyword_data_copy = keyword_data_copy[(keyword_data_copy['contains_keyword?'] == True)]

#     sum all search volumes for keywords that match
    search_vol = keyword_data_copy[search_volume_column].sum()

#     return total search volume for wherever that keyword appeared
    return search_vol

top_phrases['combined volume'] = top_phrases.keyword.apply(lambda x: find_total_search_volume(x, kw_list, 'keyword', 'search volume'))

top_phrases['keyword'] = top_phrases['keyword'].astype(str)

for i in range(5):
    top_phrases[f'category {i+1}'] = ''

#print('Filtering Search Volumes')

#top_phrases = top_phrases[top_phrases['combined volume'] >= 500]

root = tk.Tk()

savename = tk.filedialog.asksaveasfilename(title='Save File',  defaultextension='.xlsx', filetypes =(('Excel Workbook','*.xlsx'),))
root.destroy()
top_phrases.to_excel(fr'{savename}', index=False)

print('\nDURN!')
# %%


kw_list.to_csv(fr'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Keyword Research\Data\USwitch\uswitch_kw_research_plus_removed.csv',index=False)
# %%
