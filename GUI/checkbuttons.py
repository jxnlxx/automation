# test.py
import tkinter as tk
#from tkinter import *
import math

options = ['one', 'two', 'three', 'four', 'five','six','seven','eight','nine','ten','eleven']

#options = ['one', 'two']

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
        self.text = self.cget('text')  # store the text for later
        self.configure(variable=self.var)  # set the checkbox to use our var

root = tk.Tk()
root.title('SEO Technical Healthchecks')
title = tk.Label(root, text='Please select client(s) for which you\'d like to generate healthchecks:').grid(row=0, column=0, columnspan=5, padx=200, pady=10)

#   create a 5 column grid
for index, item in enumerate(options):
    x = math.floor(index/5)+1
    y = index-((math.floor(index/5))*5)
    cb = CheckBox(root, text=item)
    cb.grid(row=x, column=y, sticky=tk.W)

projects = []
def selected():
    for box in CheckBox.boxes:
        if box.var.get():  # Checks if the button is ticked
            projects.append(box.text)
    root.destroy()

rows = math.ceil(len(options)/5)+1
button = tk.Button(root, text="Select", command=selected)
button.grid(row=rows, column=2, pady=20)

root.mainloop()

print(projects)
