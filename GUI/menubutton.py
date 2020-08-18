
import tkinter
from tkinter.constants import *


class CheckBox(tkinter.Checkbutton):
    boxes = []  # Storage for all buttons

    def __init__(self, master=None, **options):
        tkinter.Checkbutton.__init__(self, master, options)  # Subclass checkbutton to keep other methods
        self.boxes.append(self)
        self.var = tkinter.BooleanVar()  # var used to store checkbox state (on/off)
        self.text = self.cget('text')  # store the text for later
        self.configure(variable=self.var)  # set the checkbox to use our var

root = tkinter.Tk()
label = tkinter.Label(root, text='Please select clients:').pack()

options = ['one', 'two', 'three', 'four']

for item in options:
    cb = CheckBox(root, text=item)  # Replace Checkbutton
    cb.pack()

selection = []

def selected():
    global selection

    selection = []

    for box in CheckBox.boxes:
        if box.var.get():  # Checks if the button is ticked
            selection.append(box.text)
    root.destroy()
    return selection

select = tkinter.Button(root, text='Select', command=selected).pack()

print(selection)



root.mainloop()

