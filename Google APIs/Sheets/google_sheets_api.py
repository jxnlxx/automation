
import gspread
import gspread_formatting
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery

# auth with google sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
google_auth = r"C:\Users\JLee35\OneDrive - Dentsu Aegis Network\PROJECTS\Python\APIs\keys\iprospectseonorth\google_auth.json"
creds = ServiceAccountCredentials.from_json_keyfile_name(google_auth, scope)
client = gspread.authorize(creds)

# load google sheets tech seo projects list
gspread_id = "GOOGLE SHEET ID"
gsheet_name = "SHEETNAME"
sheet = client.open_by_key(gspread_id)
worksheet = sheet.worksheet(gsheet_name)

# load data to dataframe from gsheet
print("Retrieving client list from Google Sheets...")
df = get_as_dataframe(sheet.worksheet(gsheet_name), parse_dates=True) # usecols=range(0, NUMBERCOLS))

# get_as_dataframe automatically loads 25 cols x 1k rows, so we trim it:
df = df.dropna(axis=1, how="all")
df = hc_list.dropna(axis=0, how="all")


# DO STUFF HERE


# upload dataframe to google sheets
client = gspread.authorize(creds)
gspread_id = "GOOGLE SHEET ID"
gsheet_name = "SHEETNAME"
sheet = client.open_by_key(gspread_id)

try:
    worksheet = sheet.worksheet(gsheet_name)
    worksheet.clear()
except gspread.exceptions.WorksheetNotFound as err:
    worksheet = sheet.add_worksheet(title=gsheet_name, rows=1, cols=1)

set_with_dataframe(worksheet, df)

