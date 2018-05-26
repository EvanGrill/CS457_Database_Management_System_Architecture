
import unittest 
import os, shutil 
import glob 

import config 

from Database import Database
from Table import Table 

class TestDatabase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ''' 
        Purpose : Create a test database
        Parameters : 
            None
        Returns: None
        ''' 
        # Create the database 
        os.mkdir(f'{config.DB_NAME}')

    @classmethod
    def tearDownClass(cls):
        ''' 
        Purpose : Remove the test database
        Parameters : 
            None
        Returns: None
        ''' 
        # Remove the database 
        shutil.rmtree(f'{config.DB_NAME}')

    def setUp(self):
        ''' 
        Purpose : Make a test table
        Parameters : 
            None
        Returns: None
        ''' 
        # Make a table in the database 
        config.CreateTable()

        # Initialize the database 
        self.db = Database(config.DB_NAME)

    def test_saveKeepTable(self):
        ''' 
        Purpose : Test saving a table that actually works
        Parameters : 
            None
        Returns: None
        ''' 
        self.db.save()
        fid = open(f'{config.DB_NAME}/{config.TBL_FILE}')
        contents = fid.read()
        fid.close() 

        self.assertEqual(contents, config.TBL_CONTENTS)

    def test_saveDropTable(self):
        ''' 
        Purpose : test saving when a table was dropped
        Parameters : 
            None
        Returns: None
        ''' 
        self.db.tables = {}
        self.db.save()

        numTables = len(glob.glob(f'{config.DB_NAME}/*.tbl'))
        self.assertEqual(numTables, 0)

    def test_init(self):
        ''' 
        Purpose : Test initialization of a database
        Parameters : 
            None
        Returns: None
        ''' 
        # tables should store one table object 
        numTables = len(self.db.tables)
        self.assertEqual(numTables, 1)

    def test_addTable(self):
        ''' 
        Purpose : Test adding a table to the database
        Parameters : 
            None
        Returns: None
        ''' 
        temp = Table(config.DB_NAME, "new_table", True)
        self.db.addTable(temp)

        # Check the table was actually added 
        self.assertTrue(temp in self.db.tables.values())

        # Set the dbName to None and try again 
        self.db.dbName = None 
        temp = Table(config.DB_NAME, "not_added_table", True)
        self.db.addTable(temp)

        self.assertFalse(temp in self.db.tables.values())

    def test_removeTable(self):
        ''' 
        Purpose : Test removing a table
        Parameters : 
            None
        Returns: None
        ''' 
        # Try to remove a table that doesn't exist
        self.db.removeTable("table_that_doesnt_exist")
        numTables = len(self.db.tables)
        self.assertEqual(numTables, 1)

        # Remove the table that does exist
        self.db.removeTable(config.TBL_NAME)
        numTables = len(self.db.tables)
        self.assertEqual(numTables, 0)

    def test_tableInDB(self):
        ''' 
        Purpose : Test checking for an existing table in a database
        Parameters : 
            None
        Returns: None
        ''' 
        self.assertFalse(self.db.tableInDB("table_that_doesnt_exist"))
        self.assertTrue(self.db.tableInDB(config.TBL_NAME))

    def test_getTableByName(self):
        ''' 
        Purpose : Test getting a table by its name
        Parameters : 
            None
        Returns: None
        ''' 
        # Get a table that doesn't exist
        self.assertIs(self.db.getTableByName("table_doesnt_exist"), None)

        # Get a table that does exist
        table = self.db.getTableByName(config.TBL_NAME)
        self.assertIsInstance(table, Table)

        # Ensure the data is correct 
        self.assertEqual(table.getFriendlyName(), config.TBL_NAME)
        
        rows = [
            {'a1': '1', 's1': 'hello', 'f1': '3.14'},
            {'a1': '8', 's1': 'this is a string', 'f1': '5'},
            {'a1': '100', 's1': 'x', 'f1': '3.14'}
        ]

        for row in rows:
            self.assertIn(row, table.rows)


if __name__ == '__main__':
    unittest.main()
