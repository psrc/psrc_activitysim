# python utilities
import os

def check_file(fname, keyword):
    with open(fname) as f:
        if keyword in f.read():
            return True
        else:
           return False

# perform a seatch for a string in all files
keyword = 'dest_density_measure'
filedir = r'C:\Workspace\activitysim\activitysim\examples\example_psrc\configs_dev'

for fname in os.listdir(filedir):
    if check_file(os.path.join(filedir,fname), keyword):
        print(fname)