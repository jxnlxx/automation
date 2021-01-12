#%%
# # autogui.py

import time
import random
import pyautogui
import datetime as dt

width, height = pyautogui.size()

width -= 100
height -= 100

start = dt.datetime.now().strftime('%H:%M')
print(f'Started at {start}\n')

while True:
    print(dt.datetime.now().strftime('%H:%M'))
    x = random.randint(100,width)
    y = random.randint(100,height)
    pyautogui.moveTo(x, y, duration=1)
    pyautogui.press('shift')
    interval = random.randint(60, 121)
    time.sleep(interval)

# %%
