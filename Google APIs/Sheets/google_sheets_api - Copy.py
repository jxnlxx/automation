FILENAME = <FILE ON GOOGLE DRIVE TO READ>
SHEETNAME = <SHEET NAME>
NUMBERCOLS = <NUMBER OF COLS TO READ>

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(<CREDS JSON FILE>, scope)
client = gspread.authorize(creds)
sheet = client.open(FILENAME)

df = get_as_dataframe(sheet.worksheet(SHEETNAME), parse_dates=True, usecols=range(0, NUMBERCOLS))

#DO STUFF HERE

set_with_dataframe(sheet.worksheet(SHEETNAME), df)
