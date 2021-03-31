#%%

import os
import mammoth
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup

input_dir = r"C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Editorial\Clients\MANCHESTER\The Entertainer\HTML Conversion\Input"
output_dir =  r"C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Editorial\Clients\MANCHESTER\The Entertainer\HTML Conversion\Output"

for filename in os.listdir(input_dir): # For all files in input directory (must be .docx)
    if filename.startswith('~$'): # skips hidden temp files
        continue
    if filename.endswith('.docx'):
        print(f'Executing script on {filename}...')
        with open(os.path.join(input_dir,filename),'rb') as docx_file: # open as read binary
            result = mammoth.convert_to_html(docx_file) # Parse word doc for html
            html = result.value # The generated HTML
            messages = result.messages # Any messages, such as warnings during conversion

#   replace variants
            html = html.replace("<br /><br />","</p><p>") # Replace variants
            html = html.replace("<p></p></em>","</em></p><p>") # Replace Variants
            html = html.replace("</em><p></p>","</em></p><p>") # Replace Variants
            html = html.replace("</p></strong>","</strong></p>") # replace variants

#   add line breaks
            html = html.replace("</strong></p><p>","</strong></p>\n<p>") # Add line break after end of product name
            html = html.replace("</p><p><strong>","</p>\n<p><strong>") # Add line break between product description and product name
            html = html.replace("</li></ul><p><strong>","</li></ul>\n<p><strong>") # Add line break after list and product name
            html = html.replace("</em></p><p>","</em></p>\n<p>") # Add line break after end of italics (product sku)

#   standardise formatting of html

            html = html.replace("\n ","\n") # Remove code from after item id
            html = html.replace(" </p> ","</p>") # Remove code from after item id

            html_split = html.splitlines() # Split the string into list by linebreaks

            df1 = html_split[0::3] # Slice only the bold text (item names)
            df_names = []
            for line in df1:
                soup = BeautifulSoup(line,'lxml')
                df_names.append(soup.get_text()) # Append the plain text from df_html

            df2 = html_split[1::3] # Slice only the italic text (item sku)
            df_sku = []
            for line in df2:
                soup = BeautifulSoup(line,'lxml')
                df_sku.append(soup.get_text()) # Append the plain text from df_html

            df_html = html_split[2::3] # Slice only the description html

            df_text = [] # Empty list to strip html from df_html
            for line in df_html:
                soup = BeautifulSoup(line,'lxml')
                df_text.append(soup.get_text()) # Append the plain text from df_html

            name = Path(filename).stem  # Filename without extension

            d = {'Product': df_names, 'Product SKU': df_sku, 'Copy': df_text, 'HTML': df_html,} # Compile data
            df = pd.DataFrame(data=d) # Create dataframe from data
            df.to_excel(os.path.join(output_dir, name + '.xlsx'), index=False) # Export to .xlsx
        print(f'Finished {filename}!')
    else:
        continue

print('DURN!')

# %%
