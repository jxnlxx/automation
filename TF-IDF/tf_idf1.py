# -*- coding: utf-8 -*-
# tf_idf1
#
# test script for setup of tf-idf
#

import os
import pandas as pd
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer

#add a project name for output
project_name = 'Daily Mail Property Articles'

# read sf export
sf_export = pd.read_excel(r'C:\Users\JLee35\Automation\TF-IDF\input\body_copy.xlsx')

corpus =[]
for i in sf_export.index:
    doc = sf_export['Body Copy Full'][i]
    corpus.append(doc)

# counts the length of the list
doc_count = len(corpus)
print(f'Total number of documents = {doc_count}')

# use TfidfVectorizer from Scikit-Learn to transform the corpus
stop = stopwords.words('english')
vectorizer = TfidfVectorizer(max_df=.65, min_df=1, ngram_range=(1,1), stop_words=stop, use_idf=True, norm=None)
transformed_documents = vectorizer.fit_transform(corpus)
transformed_documents_as_array = transformed_documents.toarray()

# verify that the numpy array represents the same number of documents that we have in the corpus list
array_count = len(transformed_documents_as_array)
print(f'Number of documents in array = {array_count}')

all_docs_as_df = pd.DataFrame()
# loop each item in transformed_documents_as_array, using enumerate to keep track of the current position
print('Processing documents...')
for counter, doc in enumerate(transformed_documents_as_array):
    # construct a dataframe
    tf_idf_tuples = list(zip(vectorizer.get_feature_names(), doc))
    one_doc_as_df = pd.DataFrame.from_records(tf_idf_tuples, columns=['term', 'score']).sort_values(by='score', ascending=False).reset_index(drop=True)
    one_doc_as_df = one_doc_as_df.head(1000)

    # insert one_doc_df['term'] to all_docs_as_df at col no [counter] with post meta title as label
    label = sf_export['Title 1'][counter] #this works as long as the list order corresponds to the df order
    all_docs_as_df.insert(counter, label, one_doc_as_df['term'])
    print(f'Completed {counter + 1} of {array_count}')

print('Processing complete!')

print(f'Saving {project_name}.csv')
# add 'Row' column so you can reset the csv easily when analysing the output
all_docs_as_df.insert(0, 'Row', all_docs_as_df.index + 1)

all_docs_as_df.to_csv(fr'C:\Users\JLee35\Automation\TF-IDF\output_concat\{project_name}.csv', index=False)

print('DURN!')