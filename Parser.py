################################################################################
#	File : Parser.py
#	Purpose : Class for parsing SQL input. 
#	Author : Jonathan Weatherspoon, Evan Grill
#	Date : Feb 08, 2018 
################################################################################

import os
import shutil
from Database import Database
from CreateStatement import CreateStatement
from UseStatement import UseStatement
from DropStatement import DropStatement
from SelectStatement import SelectStatement
from AlterStatement import AlterStatement
from InsertStatement import InsertStatement
from DeleteStatement import DeleteStatement
from UpdateStatement import UpdateStatement
from BeginTransactionStatement import BeginTransactionStatement
from CommitStatement import CommitStatement

from queue import Queue

################################################################################
#	Class : Parser
#	Purpose : Class used for parsing SQL input from file / stdin and storing
#             and returning parsed commands.
################################################################################
class Parser(object):
    '''
    Class used for parsing SQL input from file / stdin and storing
    and returning parsed commands.
    '''
    def __init__(self):
        self.queries = Queue()

    def parse(self, SQLinput):
        '''
    	Purpose:    Parse a SQL input
    	Parameters: SQLinput: The input to parse. Can be type string or file
    	Returns:    Array of parsed queries to be executed.
        '''
        # Setup states for different SQL statement types.
        states = {
            "CREATE": self.__parseCreate,
            "USE": self.__parseUse,
            "ALTER": self.__parseAlter,
            "DROP": self.__parseDrop,
            "SELECT": self.__parseSelect,
            "INSERT": self.__parseInsert,
            "DELETE": self.__parseDelete,
            "UPDATE": self.__parseUpdate,
            "BEGIN": self.__parseBegin,
            "COMMIT": self.__parseCommit
        }
        # Split up input and handle comments and ".EXIT" statement. 
        SQLinput = SQLinput.strip()
        statements = SQLinput.split(';')
        for input in statements:
            input = input.strip()
            if input.upper() == ".EXIT":
                    self.queries.put("EXIT")
                    continue
            elif input.startswith("--"):
                continue
            elif input == "":
                continue
            # Call appropriate __parseXXXX helper function.
            tokens = input.split()
            if tokens[0].upper() in states.keys():
                self.queries.put(states[tokens[0].upper()](tokens[1:]))
            else:
                self.__invalidStatement()
        return self.queries
    
    def __parseCreate(self, input):
        '''
    	Purpose:    Parse a CREATE statement
    	Parameters: input: The input to parse   
    	Returns:    CreateStatement object for current query.
        '''
        if input[0].upper() == "DATABASE":
            return CreateStatement("DATABASE", dbName=input[1])
        elif input[0].upper() == "TABLE":
            return CreateStatement("TABLE", attributes=input[1:])
        else:
            pass #ERROR

    def __parseUse(self, input):
        '''
    	Purpose:    Parse a USE statement
    	Parameters: input: The input to parse  
    	Returns:    UseStatement object for current query.
        '''
        #Check for existing database
        dbName = input[0]
        return UseStatement(dbName) 

    def __parseSelect(self, input):
        '''
    	Purpose:    Parse a SELECT statement
    	Parameters: input: The input to parse
    	Returns:    SelectStatement object for current query.
        '''
        return SelectStatement(input) 

    def __parseDrop(self, input):
        '''
    	Purpose:    Parse a DROP statement
    	Parameters: input: The input to parse
    	Returns:    DropStatement object for current query.
        '''
        if input[0].upper() == "DATABASE":
            return DropStatement("DATABASE", input[1])        
        elif input[0].upper() == "TABLE":
            return DropStatement("TABLE", input[1])
        self.__invalidStatement()
        return None 

    def __parseAlter(self, input):
        '''
    	Purpose:    Parse an ALTER statement
    	Parameters: input: The input to parse
    	Returns: AlterStatement object for current query.
        '''
        if input[0].upper() == "TABLE":
            return AlterStatement(input[1:])
        else:
            return None 

    def __parseInsert(self, input):
        '''
    	Purpose : Parse an INSERT statement
    	Parameters :
    		input: The input to parse
    	Returns: InsertStatement object for current query.
        '''
        if input[0].upper() != "INTO":
            self.__invalidStatement()
            return None
        return InsertStatement(input[1:])

    def __parseDelete(self, input):
        '''
    	Purpose : Parse a DELETE statement
    	Parameters : 
    		input: The input to parse
    	Returns: DeleteStatement object for current query
        '''
        if input[0].upper() != "FROM":
            self.__invalidStatement()
            return None 
        return DeleteStatement(input[1:])

    def __parseUpdate(self, input):
        '''
    	Purpose : Parse an UPDATE statement
    	Parameters : 
    		input: The input to parse
    	Returns: UpdateStatement object for current query
        '''
        if len(input) < 3:
            self.__invalidStatement()
            return None 
        return UpdateStatement(input)

    def __invalidStatement(self):
        '''
    	Purpose : Print a generic error for an invalid SQL statement
    	Parameters :
    		None
    	Returns: None
        '''
        print ("!Invalid SQL statement!")

    def __parseBegin(self, input):
        if not len(input) == 1:
            self.__invalidStatement()
            return None
        if not input[0].lower() == "transaction":
            self.__invalidStatement()
            return None
        return BeginTransactionStatement()
    
    def __parseCommit(self, input):
        if not len(input) == 0:
            self.__invalidStatement()
            return None
        return CommitStatement()

        
