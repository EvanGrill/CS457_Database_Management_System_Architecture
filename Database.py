################################################################################
#	File : Database.py
#	Purpose : Models a database within a Database Management Software (DBMS)
#	Author : Jonathan Weatherspoon, Evan Grill
#	Date : Feb 08, 2018 
################################################################################

from Table import Table
import os
import glob

################################################################################
#	Class : Database
#	Purpose : Models and stores metadata for a SQL database
################################################################################
class Database(object):
    '''Models and stores metadata for a SQL database'''
    def __init__(self, dbName=None):
        '''
        Purpose:    Initializer function.  
        Parameters: dbName: name of the database.  (default value: none)
        Returns: None
        '''
        # Initialize member variables.
        self.dbName = dbName
        self.tables = {}

        self.transactionInProgress = False
        self.successfulTransactions = 0

        # If we know the database name (i.e. it exists already) read in the
        # tables and add them to the Database object.
        if self.dbName is not None:
            tablesList = glob.glob(f"./{self.dbName}/*.tbl")
            for entry in tablesList:
                entry = entry.split("/")[-1]
                if entry.endswith(".tbl"):
                    entry = entry[0:-4]
                temp = Table(self.dbName, entry)
                #Strip off the .tbl extension 
                self.tables[temp.safeName] = temp 

    def save(self):
        '''
        Purpose:    Saves the Database and all member tables currently in
                    memory to the disk.  
        Parameters: None
        Returns: None
        '''
        # Call Table.save() for all tables in the database.
        for table in self.tables.values():
            table.save()
        
        # Delete all files no longer stored in the model 
        if self.dbName is not None:
            dbDir = "./" + self.dbName + "/"

            tableFiles = [tbl.fileName for tbl in self.tables.values()]
            diskFiles = os.listdir(dbDir)
            for filename in diskFiles:
                if filename not in tableFiles:
                    os.remove(dbDir + filename)

    def addTable(self, newTable):
        '''
        Purpose:    Add parameter table to the database.  
        Parameters: newTable: Table object to be added to the database.
        Returns: None
        '''
        if self.dbName is not None:
            self.tables[newTable.safeName] = newTable

    def removeTable(self, tableName):
        '''
        Purpose:    Remove the given table from the database.  
        Parameters: tableName: string name of the table to be removed.
        Returns: None
        '''
        tableName = tableName.lower()
        if self.tableInDB(tableName):
            self.tables.pop(tableName)

    def tableInDB(self, tableName):
        '''
        Purpose:    Boolean function to indicate if a given table is in the
                    database.
        Parameters: tableName: string name of the table to search for.
        Returns:    True if table is in the database, False if not.
        '''
        return (tableName.lower() in self.tables.keys())
    
    def getTableByName(self, tableName):
        '''
        Purpose:    Retreives Table object by givent table name. 
        Parameters: tableName: string name of the table to return.
        Returns:    Correlated Table object for the given name.  None, if
                    table does not exist in the database.
        '''
        tableName = tableName.lower()
        if self.tableInDB(tableName):
            tname = self.tables[tableName].tableName 
            self.tables[tableName] = Table(self.dbName, tname)
            return self.tables[tableName]
        else:
            return None
        
    def isWritable(self, tableName):
        ''' 
        Purpose : Check if the given table is locked or not
        Parameters : 
            tableName: The table name to search for
        Returns: True if the table is writable; False otherwise
        ''' 
        # Check for a lock file 
        files = glob.glob(f"./{self.dbName}/{tableName}.*")
        if len(files) > 1:
            # Check the pid matches 
            pid = str(os.getpid())
            for filename in files:
                extension = filename.split('.')[2]
                if extension == pid:
                    return True 
            return False 
        return True 
        
