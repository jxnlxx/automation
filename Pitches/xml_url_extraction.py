#

import requests
import pandas as pd
from bs4 import BeautifulSoup

url = 'https://www.thisismoney.co.uk/sitemap-articles-year~2020.xml'

response = requests.get(url)

sitemap_index = BeautifulSoup(response.content, 'html.parser')
urls = [element.text for element in sitemap_index.findAll('loc')]

url_list = []
for url in urls:
    response = requests.get(url)
    sitemap_index = BeautifulSoup(response.content, 'html.parser')
    urls = [element.text for element in sitemap_index.findAll('loc')]
    url_list.append(urls)

df = pd.DataFrame()
for u_list in url_list:
    for item in u_list:
        temp = {'URL': item}
        df = df.append(temp, ignore_index=True)

df.to_csv(r'L:\Commercial\Operations\Technical SEO\Automation\Data\xml_urls.csv')