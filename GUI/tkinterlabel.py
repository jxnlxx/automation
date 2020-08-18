# tkinterlabel.py

from tkinter import *

# definitions


root = Tk()
root.title('Technical Healthcheck')
root.geometry("300x200")

#   create label widget
label = Label(root, text='Please select client:')

#   create dropdown widget
options = ['1', '2', '3', '4']
clicked = StringVar()
clicked.set(options[0])
drop = OptionMenu(root, clicked, *options)

#   create checkbox widget



# create button widget

def selected():
    global selection
    myLabel = Label(root, text=clicked.get()).pack()
    selection = [clicked.get()]
    root.destroy()
    return selection

button = Button(root, text='Select', command=selected, padx=20, pady=5)
button.pack()

#   display it on screen
label.pack()
drop.pack()
#drop.pack()
#   create loop
root.mainloop()