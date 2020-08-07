# test.py
from tkinter import *

class CheckBox(Checkbutton):
    boxes = []  # Storage for all buttons

    def __init__(self, master=None, **options):
        Checkbutton.__init__(self, master, options)  # Subclass checkbutton to keep other methods
        self.boxes.append(self)
        self.var = BooleanVar()  # var used to store checkbox state (on/off)
        self.text = self.cget('text')  # store the text for later
        self.configure(variable=self.var)  # set the checkbox to use our var

root = Tk()
title = Label(root, text="choose file(s)")
title.pack(fill=X, expand=1)

options = ['one', 'two', 'three', 'four']


for item in options:
    cb = CheckBox(root, text=item)  # Replace Checkbutton
    cb.pack()

def selected():
    global projects

    projects = []
    for box in CheckBox.boxes:
        if box.var.get():  # Checks if the button is ticked
            projects.append(box.text)
    root.destroy()
    return projects

button = Button(root, text="Select", command=selected)
button.pack()

root.mainloop()

print(projects)
