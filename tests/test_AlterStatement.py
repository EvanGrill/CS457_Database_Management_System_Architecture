import unittest 
import os 
import shutil 

import config

from AlterStatement import AlterStatement
from Database import Database 

class TestAlterStatement(unittest.TestCase):

    @classmethod 
    def setUpClass(cls):
        ''' 
        Purpose : Make a test database / table
        Parameters : 
            None
        Returns: None
        ''' 
        # Create the test database directory 
        os.mkdir(config.DB_NAME)
        config.CreateTable()

    @classmethod 
    def tearDownClass(cls):
        ''' 
        Purpose : Remove the test database 
        Parameters : 
            None
        Returns: None
        ''' 
        # Remove the test database directory
        shutil.rmtree(config.DB_NAME)

    def setUp(self):
        ''' 
        Purpose : Create a database object for use before every test
        Parameters : 
            None
        Returns: None
        ''' 
        # Create a database object 
        self.db = Database(config.DB_NAME)

    def test_execute_add(self):
        ''' 
        Purpose : Test adding a column to a table
        Parameters : 
            None
        Returns: None
        ''' 
        stmt = self.newAlterStatement("ADD s3 char(20)")

        stmt.execute()

        # Ensure that the column was added 
        table = self.db.getTableByName(config.TBL_NAME)
        self.assertIn("s3", table.schema.keys())

    @unittest.expectedFailure
    def test_execute_drop(self):
        ''' 
        Purpose : Test dropping a column
        Parameters : 
            None
        Returns: None
        ''' 
        stmt = self.newAlterStatement("DROP COLUMN a1")

        stmt.execute()

        # Ensure the column isn't in the database anymore 
        table = self.db.getTableByName(config.TBL_NAME)
        self.assertNotIn("a1", table.schema.keys())

    @unittest.expectedFailure
    def test_execute_modify(self):
        ''' 
        Purpose : Test modifying a column
        Parameters : 
            None
        Returns: None
        ''' 
        stmt = self.newAlterStatement("MODIFY COLUMN f1 int")

        stmt.execute()

        # Ensure the datatype has been changed 
        table = self.db.getTableByName(config.TBL_NAME)
        self.assertEqual(table.schema["f1"], int)

    def newAlterStatement(self, qiAfterTable):
        ''' 
        Purpose : Create an alter statement 
        Parameters : 
            qiAfterTable: Input to the statement 
        Returns: None
        ''' 
        qi = qiAfterTable.split()
        qi.insert(0, config.TBL_NAME)
        
        stmt = AlterStatement(qi)
        stmt.setDBContext(self.db)

        return stmt

if __name__ == '__main__':
    unittest.main()