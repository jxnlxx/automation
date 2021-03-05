# -*- coding: utf-8 -*-
"""
Created on Tue Jul  7 16:06:22 2020

@author: JLee35
"""


import os
import time
import shutil
import datetime as dt
import requests
import openpyxl
import pandas as pd
import xlsxwriter as xl
from xlsxwriter.utility import xl_range
from deepcrawl import dc_key, dc_value, dc_url                                        # saved locally in C:\Users\JLee35\AppData\Local\Programs\Python\Python37-32\Lib

# =============================================================================
# START
# =============================================================================

start_time = dt.datetime.now().replace(microsecond=0)

# =============================================================================
# create dates 
# =============================================================================

report_month = start_time.replace(day=1)
backup_month = report_month - dt.timedelta(days=1)
report_month = report_month.strftime( '%b-%y' )
backup_month = backup_month.strftime( '%Y-%m' )

# =============================================================================
# backup healthcheck templayes if no backup exists
# =============================================================================

n = 1
if os.path.exists(fr'L:\Commercial\Operations\Technical SEO\Automation\Technical Healthchecks\Data\Crawl Data\Backup\{backup_month}'):
    pass
else:
    print('Creating backup of data...')
    os.mkdir(fr'L:\Commercial\Operations\Technical SEO\Automation\Technical Healthchecks\Data\Crawl Data\Backup\{backup_month}')
    src_dir = fr'L:\Commercial\Operations\Technical SEO\Automation\Technical Healthchecks\Data\Crawl Data'
    dst_dir = fr'L:\Commercial\Operations\Technical SEO\Automation\Technical Healthchecks\Data\Crawl Data\Backup\{backup_month}'
    time.sleep(2)
    for filename in os.listdir(fr'L:\Commercial\Operations\Technical SEO\Automation\Technical Healthchecks\Data\Crawl Data'):
        if filename.endswith(".xlsx"):
            if filename.startswith('~$'): # skips hidden temp files
                continue
            else:
                src_file = os.path.join(src_dir, filename)
                dst_file = os.path.join(dst_dir, filename)
                print(n, filename)
                shutil.copy(src_file, dst_file)
                n += 1
        else:
            pass
    print('Done!')

''# =============================================================================
''# END
# =============================================================================

end_time = dt.datetime.now().replace(microsecond=0)

total_time = end_time - start_time


print('\n'+f'Total time taken: {total_time}')

