import unittest 

from test_AlterStatement import TestAlterStatement
from test_CreateStatement import TestCreateStatement
from test_Database import TestDatabase
from test_DeleteStatement import TestDeleteStatement
from test_DropStatement import TestDropStatement
from test_InsertStatement import TestInsertStatement
from test_main import TestMain
from test_Parser import TestParser
from test_Query import TestQuery
from test_SelectStatement import TestSelectStatement
from test_Table import TestTable
from test_UpdateStatement import TestUpdateStatement
from test_UseStatement import TestUseStatement

class DBTests(unittest.TestSuite):
    def __init__(self):
        tests = [
            TestAlterStatement,
            TestCreateStatement,
            TestDatabase,
            TestDeleteStatement,
            TestDropStatement,
            TestInsertStatement,
            TestMain,
            TestParser,
            TestQuery,
            TestSelectStatement,
            TestTable,
            TestUpdateStatement,
            TestUseStatement
        ]
        super(DBTests, self).__init__(tests)

if __name__ == '__main__':
    unittest.main()