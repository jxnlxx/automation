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

keyword_list = pd.read_csv( fr'L:\Commercial\Operations\Technical SEO\Automation\Keyword Research\01_sample_keywords.csv' )
phrase_input = pd.read_csv( fr'L:\Commercial\Operations\Technical SEO\Automation\Keyword Research\03_tagged_keywords.csv' )

load keyword_list
load phrase_input 
search keywords for phrases
create list