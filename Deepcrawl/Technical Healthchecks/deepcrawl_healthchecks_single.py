#%% deepcrawl_single.py

import os
import time
import shutil
import math
import requests
import openpyxl
import pandas as pd
import tkinter as tk
import datetime as dt
import xlsxwriter as xl
import deepcrawl
import gspread
import gspread_formatting
from xlsxwriter.utility import xl_range
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery

# time script
start_time = dt.datetime.now().replace(microsecond=0)

# file paths
templates_path = fr"C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Deepcrawl\Tech Healthchecks\Data\Templates"
logos_path = fr"C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\Deepcrawl\Tech Healthchecks\Data\Branding"
teams_path = fr"C:\Users\JLee35\dentsu\Monthly Healthchecks - Documents\General\Monthly Healthchecks"

# create dates
report_month = start_time.replace(day=1)
backup_month = report_month - dt.timedelta(days=1)
report_month = report_month.strftime("%b-%y")
backup_month = backup_month.strftime("%Y-%m")

# auth with google sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
google_auth = r"C:\Users\JLee35\OneDrive - Dentsu Aegis Network\PROJECTS\Python\APIs\keys\iprospectseonorth\google_auth.json"
creds = ServiceAccountCredentials.from_json_keyfile_name(google_auth, scope)
client = gspread.authorize(creds)

# load google sheets tech seo projects list
gspread_id = "1H3qkPyGolEbq3pMpHYEWEb0kZkudy6mnbJT90L2kl1k"
gsheet_name = "Tech Healthchecks"
sheet = client.open_by_key(gspread_id)
worksheet = sheet.worksheet(gsheet_name)

# load data to dataframe from gsheet
print("Retrieving client list from Google Sheets...")
hc_list = get_as_dataframe(sheet.worksheet(gsheet_name), parse_dates=True) # usecols=range(0, NUMBERCOLS))

# get_as_dataframe automatically loads 25 cols x 1k rows, so we trim it:
hc_list = hc_list.dropna(axis=1, how="all")
hc_list = hc_list.dropna(axis=0, how="all")

# sort dataframe by Client alphabetically, ignoring case
hc_list = hc_list.iloc[hc_list.Client.str.lower().argsort()]
hc_list = hc_list.reset_index(drop=True)
hc_list.deepcrawl_project_id = hc_list.deepcrawl_project_id.astype(int)
print("Client list received!")

# create pop up to select clients
options = hc_list["Client"].tolist()

rows = math.ceil(len(options)/5)+1
if len(options) >=5:
    cols = 5
else:
    cols = len(options)
class CheckBox(tk.Checkbutton):
    boxes = []  # Storage for all buttons

    def __init__(self, master=None, **options):
        tk.Checkbutton.__init__(self, master, options)  # Subclass checkbutton to keep other methods
        self.boxes.append(self)
        self.var = tk.BooleanVar()  # var used to store checkbox state (on/off)
        self.text = self.cget("text")  # store the text for later
        self.configure(variable=self.var)  # set the checkbox to use our var

root = tk.Tk()
root.title("SEO Technical Healthchecks")
title = tk.Label(root, text="Please select client(s) for which you\"d like to generate healthchecks:")
title.grid(row=0, column=0, columnspan=5, padx=400, pady=10)

for index, item in enumerate(options):
    x = math.floor(index/5)+1
    y = index-((math.floor(index/5))*5)
    cb = CheckBox(root, text=item)
    cb.grid(row=x, column=y, sticky=tk.W)

selection = []
def selected():
    for box in CheckBox.boxes:
        if box.var.get():  # Checks if the button is ticked
            selection.append(box.text)
    root.destroy()

rows = math.ceil(len(options)/5)+1
button = tk.Button(root, text="Select", command=selected, padx=20)
button.grid(row=rows, column=2, pady=20)

root.mainloop()
hc_list = hc_list[hc_list["Client"].isin(selection)].reset_index(drop=True)

# log in to deepcrawl
print("Logging into DeepCrawl...")
headers = deepcrawl.get_token()
print("Auth token received!")
print("\n*   *   *   *   *   *   *   *   *   *\n")

# update healthcheck templates
skipped_clients = []
#iterate through list to grab data, ping server to get data, then update reports
for i in hc_list.index:
    n = 0
    client = hc_list["Client"][i]
    project_id = int(hc_list["deepcrawl_project_id"][i])
    if project_id == "":
        print(f"{client} does not have a project ID in the Healthchek List. Please rectify.")
        continue
    else:
        print(f"Initialising {client}")
        pass

    template_file = f"{client} - Tech Healthcheck Template.xlsx"
    healthcheck_file = f"{client} - Tech Healthcheck.xlsx"
    blank_healthcheck = "_Blank Tech Healthcheck Template.xlsx"

    brand = hc_list["Brand"][i]
    url = f"https://api.deepcrawl.com/accounts/117/projects/{project_id}"
    response = requests.get(url, headers=headers)

    # handle errors if project does not exists on DeepCrawl
    if response.status_code != 200:
        print("{client} Project does not exist on DeepCrawl - please check Project ID on Healthcheck List")
        continue
    else:
        response_json = response.json()

    # if no last crawl, skip client
    site_primary = response_json.get("site_primary")
    last_crawl = response_json.get("_crawls_last_href")
    if last_crawl == None:
        print(f"{client} has no _crawls_last_href.")
        continue

    # begin building dataframe for crawl
    print(f"Getting Data for {site_primary}")
    df = pd.DataFrame()                                                                 # create a blank DataFrame for the project
    url = "https://api.deepcrawl.com" + last_crawl + "/reports?page=1&per_page=200"     # create a URL from the href
    crawl = requests.get(url, headers=headers)                                          # ping the URL to get the data

    # check status code and retry if not 200
    while True:
        if crawl.status_code != 200:
            print(f"\n{crawl} \nRetrying...\n")
            time.sleep(2)
            crawl = requests.get(url, headers=headers)                                    # ping the URL again to get the data
        else:
            print("Success!\n")
            break

    crawl = crawl.json() # convert the JSON response into something pythonic
    df = df.append(crawl, ignore_index = True) # fill the DataFrame with data from the response
    n += 1 # add 1 to "n"
    print(n, url) # print "n, url" so the script shows it"s not idle
    while url != "https://api.deepcrawl.com" + crawl.links["last"]["url"]: # check if the URL is the same as the last page in the response header
        url = "https://api.deepcrawl.com" + crawl.links["next"]["url"] # if not, change the url to the "next" page in the pagination
        crawl = requests.get(url, headers=headers) # ping the new url to get the data
        crawl = crawl.json() # convert the JSON response into something pythonic
        df = df.append(crawl, ignore_index = True) # append the data to the DataFrame
        n += 1 # add 1 to "n"
        print(n, url) # print "n, url" so the script shows it"s not idle

    # try removing duplicates from "report_template" col, filter for "report_template" and "basic_total" cols
    try:
        df = df.drop_duplicates(subset = "report_template")
        df = df[["report_template", "basic_total"]]
    # if "KeyError", add client name to list for mesaging at end, then skip client
    except KeyError:
        skipped_clients = skipped_clients + [f"{client}"]
        continue

    # see if client"s template exists, and if not, open blank template

    try:
        df_data = pd.read_excel(os.path.join(templates_path, template_file), sheet_name="Data")
        df_healthcheck = pd.read_excel(os.path.join(templates_path, template_file), sheet_name="Healthcheck")

    except FileNotFoundError:
        df_data = pd.read_excel(os.path.join(templates_path, blank_healthcheck), sheet_name="Data", read_only=True)
        df_healthcheck = pd.read_excel(os.path.join(templates_path, blank_healthcheck), sheet_name="Healthcheck", read_only=True)

    # if column labelled with "report_month" already exists, drop it
    try:
        df_data = df_data.drop(columns=[report_month])
        df_healthcheck = df_healthcheck.drop(columns=[report_month])
    except KeyError:
        pass

    # update "data" tab
    df_data = df_data.merge(df, on= "report_template", how="left")
    df_data = df_data.rename(columns={"basic_total":report_month})
    df_data = df_data.fillna(0)

    print("\n- Data Tab Updated")

    # update "healthcheck" tab
    sumif_data = df_data[["Issue", report_month]]
    sumif_data = sumif_data.groupby("Issue")[report_month].sum()
    df_healthcheck = df_healthcheck.merge(sumif_data , on="Issue", how="left")

    print("- Healthcheck Tab Updated")

    # save template
    writer = pd.ExcelWriter(os.path.join(templates_path, template_file), engine="xlsxwriter") # pylint: disable=abstract-class-instantiated
    df_data.to_excel(writer, sheet_name = "Data", index=False)
    df_healthcheck.to_excel(writer, sheet_name = "Healthcheck", index=False)
    # set zoom at 85
    data_ws = writer.sheets["Data"]
    healthcheck_ws = writer.sheets["Healthcheck"]
    data_ws.set_zoom(85)
    healthcheck_ws.set_zoom(85)
    writer.save()

    print(f"\n{client} Template Saved")
    print("\n*   *   *   *   *   *   *   *   *   *\n")

print("Healthcheck Templates Updated!")
print("\n*   *   *   *   *   *   *   *   *   *\n")

# time script
mid_time = dt.datetime.now().replace(microsecond=0)

# iterate through rows of healthcheck list and produce one healthcheck per client
for i in hc_list.index:
    client = hc_list["Client"][i]
    project_id = hc_list["deepcrawl_project_id"][i]
    brand = hc_list["Brand"][i]

    # load workbook as openpyxl and convert to dataframe
    # (gets around an issue where pandas wouldn't load the workbook properly)
    print(f"Opening {client} healthcheck")
    try:
        df = openpyxl.load_workbook(filename=os.path.join(templates_path, template_file), data_only=True)
        df = df["Healthcheck"]
        df = pd.DataFrame(df.values)
        df_rows, df_cols = df.shape
        xl_rows = df_rows + 4
        print(f"Starting to write {client} healthcheck")
    except FileNotFoundError:
        print(f"{client} Healthcheck template not found")
        continue
    df_rows, df_cols = df.shape
    xl_rows = df_rows+4
    df = df.fillna("")

    # create blank workbook with xlsxwriter at save location
    wb = xl.Workbook(os.path.join(templates_path, healthcheck_file))
    ws = wb.add_worksheet("Healthcheck")

    #  CELL FORMATTING

    # heading formatting (conditional on brand client is with)
    if brand == "Carat":
        brand_colour = "#00beff" # Carat
        brand_spark = "#4ee6C6" # Carat
        ws.insert_image("B2", logos_path + fr"\Carat Logo.png", {"y_offset":5, "x_scale":0.13, "y_scale": 0.13})

    if brand == "Vizeum":
        brand_colour = "#fac600" # Vizeum
        brand_spark = "#f6861f" # Vizeum
        ws.insert_image("B1", logos_path + fr"\Vizeum Logo.png", {"x_offset":20, "y_offset":10, "x_scale":0.4, "y_scale": 0.4})

    if brand not in ["Carat", "Vizeum"]:
        brand_colour = "#3ec67f" # iPro
        brand_spark = "#027580" # iPro
        ws.insert_image("B2", logos_path + fr"\iProspect Logo.png", {"x_offset":-10,"y_offset":5, "x_scale":0.45, "y_scale": 0.45})

    text = "#636363" # Generic

    # table Formatting
    client_name = wb.add_format({"bold":True, "indent":0, "font_color":text, "font_size":20, "align":"center", "valign":"vcenter", "rotation":0})
    l_head = wb.add_format({"bold":False, "indent":0, "bg_color":brand_colour, "font_color":brand_colour, "font_size":12, "align":"center", "valign":"vcenter", "rotation":0, "border_color":text, "top":1, "bottom":1, "left":1, "right":0})
    c_head = wb.add_format({"bold":False, "indent":0, "bg_color":brand_colour, "font_color":brand_colour, "font_size":12, "align":"center", "valign":"vcenter", "rotation":0, "border_color":text, "top":1, "bottom":1, "left":0, "right":0})
    r_head = wb.add_format({"bold":False, "indent":0, "bg_color":brand_colour, "font_color":"#ffffff", "font_size":12, "align":"center", "valign":"vcenter", "rotation":90, "border_color":text, "top":1, "bottom":1, "left":1, "right":1, "num_format":"mmm-yy"})

    sub1 = wb.add_format({"bold":True, "indent":1, "font_color":text, "font_size":18, "align":"left", "valign":"vcenter", "rotation":0, "border_color":text, "top":1, "bottom":1, "left":0, "right":0})
    sub2 = wb.add_format({"bold":True, "indent":1, "font_color":"#ffffff", "font_size":18, "align":"left", "valign":"vcenter", "rotation":0, "border_color":text, "top":1, "bottom":1, "left":0, "right":0})
    issue = wb.add_format({"bold":True, "indent":1, "font_color":text, "font_size":12, "align":"left", "valign":"vcenter", "rotation":0, "border_color":text, "top":1, "bottom":1, "left":1, "right":1})
    detail = wb.add_format({"bold":False, "indent":1, "font_color":text, "font_size":12, "align":"left", "valign":"vcenter", "rotation":0, "border_color":text, "top":1, "bottom":1, "left":1, "right":1})
    spark = wb.add_format({"bold":False, "indent":1, "font_color":"#ffffff", "font_size":12, "align":"left", "valign":"vcenter", "rotation":0, "border_color":text, "top":1, "bottom":1, "left":1, "right":1})
    data = wb.add_format({"bold":False, "indent":0, "font_color":text, "font_size":12, "align":"center", "valign":"vcenter", "rotation":0, "border_color":text, "top":1, "bottom":1, "left":1, "right":1})

    # row groups for adding formatting
    xl_head = [5]
    xl_sub = [6, 14, 21, 28, 37, 42, 46]
    xl_data = [7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19, 20, 22, 23, 24, 25, 26, 27, 29, 30, 31, 32, 33, 34, 35, 36, 38, 39, 40, 41, 43, 44, 45, 47, 48, 49, 50, 51, 52, 53]

    # format + fill table header
    ws.set_row(2, 30)
    ws.write(2, 2, f"{client} Healthcheck", client_name)

    # format + fill table data
    x = 5
    n = 0 #for df.loc[n]
    while x <= xl_rows:
        #print("2")
        if x in xl_head:
            #print("3")
            y = 1
            df_list = list(df.iloc[n])
            ws.set_row (x, 60)
            while y <= df_cols:
                if y == 1:
                    ws.write(x, y, df_list[y-1], l_head)
                    y += 1
                if y == 2:
                    ws.write(x, y, df_list[y-1], c_head)
                    y += 1
                if y == 3:
                    ws.write(x, y, "", c_head)
                    y += 1
                if y in range (4, df_cols):
                    ws.write(x, y, df_list[y-1], r_head)
                    y += 1
                if y == df_cols:
                    ws.write(x, y, df_list[y-1], r_head)
                    y += 1
                    x += 1
                    n += 1
        if x in xl_sub:
            #print("4")
            y = 1
            df_list = list(df.iloc[n])
            ws.set_row(x, 60)
            while y <= df_cols:
                if y == 1:
                    ws.write(x, y, df_list[y-1], sub1)
                    y += 1
                if y in range(2, df_cols):
                    ws.write(x, y, df_list[y-1], sub2)
                    y += 1
                if y == df_cols:
                    ws.write(x, y, df_list[y-1], sub2)
                    y += 1
                    x += 1
                    n += 1
        if x in xl_data:
        #print("5")
            y = 1
            df_list = list(df.iloc[n])
            ws.set_row(x, 30)
            while y <= df_cols:
                if y == 1:
                    ws.write(x, y, df_list[y-1], issue)
                    y += 1
                if y == 2:
                    ws.write(x, y, df_list[y-1], detail)
                    y += 1
                if y == 3:
                    ws.write(x, y, "", spark)
                    y += 1
                if y in range (4, df_cols):
                    ws.write(x, y, df_list[y-1], data)
                    y += 1
                if y == df_cols:
                    ws.write(x, y, df_list[y-1], data)
                    y += 1
                    x += 1
                    n += 1

    # Add sparklines
    x, y = 7, 3
    while x <= xl_rows:
        sparkline = xl_range(x, 4, x, df_cols)
        ws.add_sparkline(x, y, {"range": sparkline, "series_color": brand_spark, "markers":True})
        x += 1
    ws.set_tab_color(brand_colour)

    # ADDITIONAL FORMATTING

    # column widths
    ws.set_column(0, 0, 5)
    ws.set_column(1, 1, 45)
    ws.set_column(2, 2, 90)
    ws.set_column(3, 3, 60)

    ws.freeze_panes(6, 2)   # Freeze first 6 rows and first 2 columns
    ws.set_zoom(70) # Set zoom to 70%
    ws.hide_gridlines(2) # Hide Gridlines
    #ws.hide_row_col_headers() # hide headers - not great for unhiding hiddent tabs

    if df_cols > 9:
        ws.set_column(4, df_cols-6, 0) # hide columns so only 6 months of data is visible

    wb.close()
    print(f"{client} Healthcheck Done"+"\n\n"
            "*   *   *   *   *   *   *   *   *   *")

print("\n"+"DURN!")

print("\n"+"Checking for skipped Clients:")
time.sleep(1)

if len(skipped_clients) > 0:
    for c in skipped_clients:
        print(c)
else:
    print("\n"+"All clients in Healthcheck List sheet were completed successfully!")

# END

end_time = dt.datetime.now().replace(microsecond=0)
dc_time = mid_time - start_time
xl_time = end_time - mid_time
total_time = end_time - start_time

print(f"Time taken to get data from DeepCrawl: {dc_time}"+"\n"
      f"Time taken to generate Excel healthchecks: {xl_time}"+"\n"
      f"Total time taken: {total_time}")

# %%

