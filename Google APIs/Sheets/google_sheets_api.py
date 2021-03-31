
import gspread
import gspread_formatting
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery

worksheet = "FILE ON GOOGLE DRIVE TO READ"
SHEETNAME = "SHEET NAME"
NUMBERCOLS = "NUMBER OF COLS TO READ"
json_creds = r"location of google auth json creds file"

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(json_creds, scope)
client = gspread.authorize(creds)
sheet = client.open(FILENAME)

df = get_as_dataframe(sheet.worksheet(SHEETNAME), parse_dates=True, usecols=range(0, NUMBERCOLS))

#DO STUFF HERE


try:
    worksheet = sheet.worksheet(SHEETNAME)
    worksheet.clear()
except gspread.exceptions.WorksheetNotFound as err:
    worksheet = sheet.add_worksheet(title=SHEETNAME, rows=1, cols=1)


set_with_dataframe(sheet.worksheet(SHEETNAME), df)
