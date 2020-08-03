# tf_idf1
#
# test script for setup of tf-idf
#

import os
import pandas as pd
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer

#project_name = input('Please enter project name >>> ')
project_name = 'Daily Mail Property Articles'

# read sf export and add
df = pd.read_excel(r'C:\Users\JLee35\Automation\TF-IDF\input\body_copy.xlsx')

corpus =[]
for i in df.index:
    doc = df['Body Copy Full'][i]
    corpus.append(doc)

# counts the length of the list
doc_count = len(corpus)
print(f'Total number of documents = {doc_count}')

#import the TfidfVectorizer from Scikit-Learn.
stop = stopwords.words('english')
vectorizer = TfidfVectorizer(max_df=.65, min_df=1, ngram_range=(1,3), stop_words=None, use_idf=True, norm=None)
transformed_documents = vectorizer.fit_transform(corpus)
transformed_documents_as_array = transformed_documents.toarray()

# use this line of code to verify that the numpy array represents the same number of documents that we have in the file list
array_count = len(transformed_documents_as_array)
print(f'Number of documents in array = {len(transformed_documents_as_array)}')

all_docs_as_df = pd.DataFrame()
# loop each item in transformed_documents_as_array, using enumerate to keep track of the current position
print('Processing documents...')
for counter, doc in enumerate(transformed_documents_as_array):
    lable = df['Title 1'][counter] #this works as long as the list order corresponds to the df order
    for char in ['Â']:
        lable = lable.replace(char, '_')
    # construct a dataframe
    tf_idf_tuples = list(zip(vectorizer.get_feature_names(), doc))
    one_doc_as_df = pd.DataFrame.from_records(tf_idf_tuples, columns=['term', 'score']).sort_values(by='score', ascending=False).reset_index(drop=True)
    one_doc_as_df = one_doc_as_df.head(1000)
    #col = len(all_docs_as_df.columns)
    all_docs_as_df.insert(counter, lable, one_doc_as_df['term'])
    print(f'Completed {counter+1} of {len(transformed_documents_as_array)}')

    # output to a csv using the enumerated value for the filename
    #one_doc_as_df.to_csv(fr'C:\Users\JLee35\Automation\TF-IDF\output\{title}.csv', index=False)
print('Processing complete!')

print(f'Saving {project_name}.csv')
all_docs_as_df.insert(0, 'Row', all_docs_as_df.index + 1)
all_docs_as_df.to_csv(fr'C:\Users\JLee35\Automation\TF-IDF\output_concat\{project_name}.csv', index=False)

# # concatenate files into one df
# all_docs_as_df = pd.DataFrame()
# n = 1
# for filename in os.listdir(r'C:\Users\JLee35\Automation\TF-IDF\output'):
#     if filename.endswith(".csv"):
#         lable = filename
#         lable = lable.replace('.csv', '')
#         df = pd.read_csv(fr'C:\Users\JLee35\Automation\TF-IDF\output\{filename}').astype(str)
#         df['combined'] = df[['term', 'score']].apply(lambda x: ' - '.join(x), axis=1)
#         df = df.head(1000)
#         col = len(all_docs_as_df.columns)
#         all_docs_as_df.insert(col, lable, df['combined'])
#         print(f'Concatenated {n} of {len(transformed_documents_as_array)}')
#         n += 1

print(f'Saving {project_name}.csv')
all_docs_as_df.to_csv(fr'C:\Users\JLee35\Automation\TF-IDF\output_concat\{project_name}.csv', index=False)

print('DURN!')