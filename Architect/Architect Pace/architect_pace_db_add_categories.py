#%% architect_pace_db_add_categories.py

import sqlite3
import numpy as np
import pandas as pd

# set this variable to the next integer based on the number of category tables in the database

n=2

# %% create categories table and add categories from csv
con = sqlite3.connect(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\architect_pace_normalised.db')
cur = con.cursor()

df = pd.read_csv(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\kw_list.csv')
df.columns = df.columns.str.lower()
df = df.fillna('')

categories = ['']
categories = list(dict.fromkeys(categories + df['category'].to_list()))

cur.execute(f'''CREATE TABLE IF NOT EXISTS category{n}(
    Id INTEGER PRIMARY KEY,
    Category TEXT NOT NULL,
    UNIQUE (Category)
    );''')

for i in categories:
    category = i.strip()
    print(category)
    cur.execute(f'INSERT OR IGNORE INTO category{n} (Category) VALUES (?)', (category,))

con.commit()
con.close()

#%% add column into keyword table and set devault value to 1 (which is an empty category as per above)

con = sqlite3.connect(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\architect_pace_normalised.db')
cur = con.cursor()

sql = f'''ALTER TABLE keywords
    ADD COLUMN Category{n}Id INTEGER NOT NULL
    DEFAULT 1;'''
    #DEFAULT 1 is the CategoryId for blank/untagged

cur.execute(sql)

con.commit()
con.close()

# %% update new category column with normalised CategoryId

con = sqlite3.connect(r'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Architect\Architect PACE\Data\Database\architect_pace_normalised.db')
cur = con.cursor()

for i in df.index:
    keyword = df['keyword'][i].strip()
    category = df[f'category'][i].strip()
    print(f'{i+1} of {len(df)} - {keyword}')

    cur.execute(f'SELECT Id FROM category{n} WHERE Category = ? LIMIT 1', (category,))
    try:
        CategoryId = int(cur.fetchone()[0])
    except TypeError:
        cur.execute(f'INSERT OR IGNORE INTO category{n} (Category) VALUES (?)', (category,))
        cur.execute(f'SELECT Id FROM category{n} WHERE Category = ? LIMIT 1', (category,))
        CategoryId = int(cur.fetchone()[0])

    cur.execute(f'UPDATE keywords SET Category{n}Id = ? WHERE Keyword = ?', (CategoryId, keyword))
    con.commit()

con.close()

# %%


