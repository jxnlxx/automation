# blog_phrase_extraction.py
#
# extracts n-grams
# calculates frequency
# calculates len(n-gram)
# outputs csv

import pandas as pd
import re
import nltk
import collections
from nltk.corpus import stopwords

df = pd.read_excel(r'C:\Users\JLee35\Automation\RSS reader\csv\body_copy.xlsx')

print('Getting phrases')
kw_list = list(filter(None, df['Body Copy Full'].tolist()))
phrase_list = [str.lower(kw).split(" ") for kw in kw_list]
phrase_counts = collections.Counter()

print('Counting phrases')
gram = 10
for i in range(1,gram+1):
    for phrase in phrase_list:
        phrase_counts.update(nltk.ngrams(phrase,i))

n = 100000
print(f'Filtering for {n} most common phrases')
top_phrases = phrase_counts.most_common(n)
top_phrases = pd.DataFrame(top_phrases, columns=["keyword", "count"])
top_phrases = top_phrases[top_phrases['keyword'] != 'nan']

print('Formatting phrases')
def fix_brackets(keyword):
    fixed_keyword=' '.join(keyword)
    return fixed_keyword

top_phrases['keyword'] = top_phrases['keyword'].apply(lambda keyword: fix_brackets(keyword))
top_phrases['keyword'] = top_phrases['keyword'].replace('- ','')

print('Counting phrase length')
top_phrases['len'] = top_phrases['keyword'].str.len()
top_phrases['n-gram'] = top_phrases['keyword'].apply(lambda x: len(str(x).split(' ')))

print('Removing stopwords')
stop = stopwords.words('english')
top_phrases = top_phrases[~top_phrases['keyword'].isin(stop)]
top_phrases['keyword_clean'] = top_phrases['keyword'].apply(lambda x: ' '.join([word for word in x.split() if word not in stop]))
print('Saving')
top_phrases.to_csv(r'C:\Users\JLee35\Automation\RSS reader\csv\body_copy_phrase_count.csv', index=False )

print('DURN!')