################################################################################
#	File : CreateStatement.py
#	Purpose : Models a CREATE statement in a SQL database
#	Author : Jonathan Weatherspoon, Evan Grill
#	Date : Feb 08, 2018
################################################################################

from Query import Query
import os
from Database import Database
from Table import Table

################################################################################
#	Class : CreateStatement
#	Purpose : Models a CREATE statement in a SQL databse
################################################################################
class CreateStatement(Query):
    '''Models a CREATE statement in a SQL database'''
    def __init__(self, createType, dbName=None, attributes=None):
        super(CreateStatement, self).__init__()
        self.createType = createType
        self.dbName = dbName
        self.attributes = attributes
        self.database = None
    
    def execute(self):
        '''
        Purpose:    Implementation of the CREATE statement.  
        Parameters: None
        Returns: None
        '''
        # Determine if CREATE DATABASE or CREATE TABLE
        if self.createType.upper() == "DATABASE" and self.dbName is not None:
            
            # Error if the target directory exists, otherwise create it
            dirs = next(os.walk("."))[1] 
            if self.dbName in dirs:
                print ("!Failed to create database", self.dbName, "because it already exists.")
            else:
                os.makedirs(self.dbName)
                print ("Database", self.dbName, "created.")
        if self.createType.upper() == "TABLE" and self.database is not None:
            tableName = self.attributes[0]
            # Check for a space between the table name and (
            if '(' in tableName:
                ts = tableName.split('(') 
                tableName = ts[0] 
                self.attributes.insert(1, ts[1])
            # Create table and add it to the currently selected database
            # (If one is selected)
            if self.database.dbName is not None:
                dirs = os.listdir("./" + self.database.dbName )
                if self.database.tableInDB(tableName):
                    print ("!Failed to create table", tableName, "because it already exists.")
                else:
                    schema = self.__parseSchema(self.attributes[1:])
                    newTable = Table(self.database.dbName, tableName, True)
                    newTable.setSchema(schema)
                    self.database.addTable(newTable)
                    print ("Table", tableName, "created.")
            else:
                print ("!Failed to create table", tableName, "because no database is selected.")

    def __parseSchema(self, schemaInput):
        '''
        Purpose:    Private helper function.  
        Parameters: schemaInput: an aray of column names and types.
        Returns:    schema: a dictionary with keys as column names and values
                    as types.
        '''
        joined = " ".join(schemaInput).strip()
        if(joined.endswith(')')): joined = joined[:-1]
        schemaVals = joined.split(',')

        schema = {}
        for val in schemaVals:
            val = val.strip().split()
            schema[val[0]] = val[1] 
        return schema
