#%% word_extract_words_in_docx_and_count_frequency.py

import docx
import pandas as pd
from nltk.corpus import stopwords
import os
from nltk.util import ngrams # function for making ngrams
import nltk, re, string, collections

# Given a text string, remove all non-alphanumeric
# characters (using Unicode definition of alphanumeric).

def stripNonAlphaNum(text):
    import re
    return re.compile(r'\W+', re.UNICODE).split(text)

def getText(filename):
    doc = docx.Document(filename)
    fullText = []
    for para in doc.paragraphs:
        fullText.append(para.text.lower())
    return ' '.join(fullText)

def wordListToFreqDict(wordlist):
    wordfreq = [wordlist.count(p) for p in wordlist]
    return dict(list(zip(wordlist,wordfreq)))

def sortFreqDict(freqdict):
    aux = [(freqdict[key], key) for key in freqdict]
    aux.sort()
    aux.reverse()
    return aux

def fix_those_brackets(keyword):
    fixed_keyword = ' '.join(keyword)
    return fixed_keyword


for filename in os.listdir(r"C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Microsoft\Word\Data\Erica's Project\Input"):
    if filename.endswith(".docx"):
        save_name = filename
        save_name = save_name.replace('.docx','')
        doc = getText(fr"C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Microsoft\Word\Data\Erica's Project\Input\{filename}")

        all_ngrams = pd.DataFrame(columns=['word','count'])

        unigrams = doc.split()

        bigrams = ngrams(unigrams,2)
        bigrams = dict(collections.Counter(bigrams))

        trigrams = ngrams(unigrams, 3)
        trigrams = dict(collections.Counter(trigrams))

        unigrams = dict(collections.Counter(unigrams))
        unigrams = pd.DataFrame.from_dict(list(unigrams.items()))
        unigrams = unigrams.rename(columns={0:'word',1:'count'})
        all_ngrams = all_ngrams.append(unigrams)

        bigrams = pd.DataFrame.from_dict(list(bigrams.items()))
        bigrams = bigrams.rename(columns={0:'word',1:'count'})
        bigrams['word'] = bigrams.word.apply(lambda x: fix_those_brackets(x))
        all_ngrams = all_ngrams.append(bigrams)

        trigrams = pd.DataFrame.from_dict(list(trigrams.items()))
        trigrams = trigrams.rename(columns={0:'word',1:'count'})
        trigrams['word'] = trigrams.word.apply(lambda x: fix_those_brackets(x))
        all_ngrams = all_ngrams.append(trigrams)

        stop = stopwords.words('english')
        all_ngrams = all_ngrams[~all_ngrams['word'].isin(stop)]

        all_ngrams = all_ngrams.sort_values(by='count',ascending=False)
        all_ngrams.to_csv(fr"C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Microsoft\Word\Data\Erica's Project\Script Output\{save_name}.csv", index=False)

# %%
