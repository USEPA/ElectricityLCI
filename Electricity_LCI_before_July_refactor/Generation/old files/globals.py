import os
def set_dir(directory):
    if not os.path.exists(directory): os.makedirs(directory)
    return directory


datadir = 'electricitylci/data/'
outputdir = set_dir('output/')
