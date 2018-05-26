
import unittest
import os
import shutil

import config

from CreateStatement import CreateStatement
from Database import Database


class TestCreateStatement(unittest.TestCase):

    def setUp(self):
        ''' 
        Purpose : Make a create statement to test and remove the test db
        Parameters : 
            None
        Returns: None
        ''' 
        self.stmt = CreateStatement("DATABASE", config.DB_NAME)
        try:
            shutil.rmtree(config.DB_NAME)
        except:
            pass

    def tearDown(self):
        ''' 
        Purpose : Try removing the test db
        Parameters : 
            None
        Returns: None
        ''' 
        try:
            shutil.rmtree(config.DB_NAME)
        except:
            pass

    def test_execute_database(self):
        ''' 
        Purpose : Test creating a database
        Parameters : 
            None
        Returns: None
        ''' 
        self.stmt.execute()
        dirs = next(os.walk("."))[1]
        self.assertIn(config.DB_NAME, dirs)

        shutil.rmtree(config.DB_NAME)

        self.stmt.dbName = None
        self.stmt.execute()
        dirs = next(os.walk("."))[1]
        self.assertNotIn(config.DB_NAME, dirs)

    def test_execute_table(self):
        ''' 
        Purpose : Test creating a table
        Parameters : 
            None
        Returns: None
        ''' 
        os.mkdir(config.DB_NAME)
        db = Database(config.DB_NAME)

        self.assertEqual(len(db.tables), 0)

        self.stmt.createType = "TABLE"
        self.stmt.attributes = [
            config.TBL_NAME + "(",
            "a1",
            "int,",
            "s1",
            "varchar(20),",
            "f1",
            "float)"
        ]

        self.stmt.setDBContext(db)

        self.stmt.execute()

        self.assertEqual(len(db.tables), 1)


if __name__ == '__main__':
    unittest.main()
