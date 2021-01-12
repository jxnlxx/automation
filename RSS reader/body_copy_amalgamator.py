# body_copy_amalgamator.py
#
# adds new column
# concatenates all custom extraction columns in SF export
# outputs csv

import pandas as pd

print('Loading export')
df = pd.read_excel(r'C:\Users\JLee35\Automation\RSS reader\csv\sf_export.xlsx') # taken from screaming frog

print('Merging custom extractions')
df.insert(loc= 54, column='Body Copy Full', value ='')
df['Body Copy Full'] = df[df.columns[55:]].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)

print('Saving')
df.to_csv(r'C:\Users\JLee35\Automation\RSS reader\csv\body_copy.csv', index=False)

print('DURN!')