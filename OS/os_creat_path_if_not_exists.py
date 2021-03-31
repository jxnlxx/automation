#%% os_creat_path_if_not_exists.py

import os

existing_path = fr'C:\Users\JLee35\dentsu\iProspect Hub - Documents\Channels\Owned & Earned\Automation\STAT\Clients'

new_folder = 'new folder'

if not os.path.exists(os.path.join(existing_path, new_folder)):
    os.mkdir(os.path.join(existing_path, new_folder))

