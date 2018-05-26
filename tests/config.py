import sys, os
from textwrap import dedent

DB_NAME  =      "test_database"
TBL_NAME =      "test_tbl"
TBL_FILE =      TBL_NAME + ".tbl"

TBL_CONTENTS = '''a1 (int)  | s1 (varchar(20))  | f1 (float) 
1 | hello | 3.14
8 | this is a string | 5
100 | x | 3.14
'''

# Allow for importing of modules in the parent folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def CreateTable():
    fid = open(f'{DB_NAME}/{TBL_FILE}', 'w')
    fid.write(TBL_CONTENTS)
    fid.close()
