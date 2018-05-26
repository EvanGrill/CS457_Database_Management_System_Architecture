################################################################################
#	File : DropStatement.py
#	Purpose : Models a DROP query on a SQL database
#	Author : Jonathan Weatherspoon, Evan Grill
#	Date : Feb 08, 2018
################################################################################

import shutil
import os

from Query import Query
from Database import Database

################################################################################
#	Class : DropStatement
#	Purpose : Models a DROP statement in a SQL database
################################################################################
class DropStatement(Query):
    '''Models a DROP statement in a SQL database'''
    def __init__(self, dropType, name):
        super(DropStatement, self).__init__()
        self.name = name
        self.dropType = dropType
        self.database = None
     
    def execute(self):
        '''
        Purpose:    Implementation of the DROP statement.  
        Parameters: None
        Returns: None
        '''
        # Determine whether DROP DATABASE or DROP TABLE query.
        if self.dropType == "DATABASE":
            # Check if the databse already exists. If yes, delete database
            # folder otherwise print error.
            dirs = next(os.walk("."))[1] 
            if self.name in dirs:
                shutil.rmtree(self.name)
                print ("Database", self.name, "deleted.")
            else:
                print ("!Failed to delete", self.name, "because it does not exist.")
            
            # Return an empty database context
            return Database() 

        elif self.dropType == "TABLE" and self.database is not None:
            
            # Search for table name in database. If found, remove it from the
            # current database.
            if self.database.tableInDB(self.name):
                self.database.removeTable(self.name)
                print ("Table", self.name, "deleted.")
            else:
                print ("!Failed to delete", self.name, "because it does not exist.")
        else:
            print ("!Invalid SQL Statement!")
            
