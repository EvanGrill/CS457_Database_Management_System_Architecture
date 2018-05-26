
import unittest 
import os, shutil

import config 

from DeleteStatement import DeleteStatement

class TestDeleteStatement(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ''' 
        Purpose : Create a test database
        Parameters : 
            None
        Returns: None
        ''' 
        os.mkdir(config.DB_NAME)

    @classmethod
    def tearDownClass(cls):
        ''' 
        Purpose : Remove the test database
        Parameters : 
            None
        Returns: None
        ''' 
        shutil.rmtree(config.DB_NAME)

    def setUp(self):
        ''' 
        Purpose : Create a test table
        Parameters : 
            None
        Returns: None
        ''' 
        config.CreateTable()

    def test_execute(self):
        ''' 
        Purpose : Test deleting rows from a table
        Parameters : 
            None
        Returns: None
        ''' 
        pass
        


if __name__ == '__main__':
    unittest.main()
