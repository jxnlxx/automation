#tkinter_test.py

from tkinter import *
from PIL import ImageTk, Image

root = Tk()
root.title('Please Select Client')
root.geometry("300x200")

options = ['1', '2', '3']

clicked = StringVar()
clicked.set(options[0])

drop = OptionMenu(root, clicked, *options)
drop.pack()

def show():
    label = Label(root, text=clicked.get()).pack()

button = Button(root, text='Select', command=show).pack()

root.mainloop()
