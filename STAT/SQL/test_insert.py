import os
import sqlite3
import pandas as pd

conn = sqlite3.connect(r'L:\Commercial\Operations\Technical SEO\Automation\Data\STAT\SQL\stat_ranks_relational.db')

with conn:

    c = conn.cursor()

    c.execute('''INSERT INTO Keyword(Keyword)
        VALUES('dresses')

        ;''')

    c.execute('''INSERT INTO (KeywordMarket)
    VALUES('en-gb')