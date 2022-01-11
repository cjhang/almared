import os
import sys

# ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if 'ALMARED_HOME' in os.environ.keys():
    ROOT_DIR = os.environ['ALMARED_HOME']
else:
    ROOT_DIR = os.path.join(os.path.expanduser('~'), 'work/projects/almared/')
ROOT_DIR_CODE = os.path.join(ROOT_DIR, 'code')
print("Project path: {}".format(ROOT_DIR))
sys.path.append(ROOT_DIR)

execfile(os.path.join(ROOT_DIR_CODE, 'almared_run.py'))
