################################################################################
#	File : DeleteStatement.py
#	Purpose : Models a SQL DELETE statement
#	Author : Jonathan Weatherspoon, Evan Grill  
#	Date : March 25, 2018   
################################################################################

from Query import Query 

################################################################################
#	Class : DeleteStatement
#	Purpose : Models a DELETE statement
################################################################################
class DeleteStatement(Query):
    '''Models a DELETE statement in a SQL database'''
    def __init__(self, queryInput):
        self.database = None
        self.tableName, self.conditions = self.__parseDelete(queryInput)

    def execute(self):
        ''' 
        Purpose : Execute a delete statement
        Parameters : 
            None
        Returns: None
        ''' 
        if self.database is None:
            print("!Failed to delete from table", self.tableName, "because no database is selected!")
            return None 
        
        table = self.database.getTableByName(self.tableName) 

        if table is None:
            print("!Failed to delete from table", self.tableName, "because table does not exist!")
            return None

        # Check for a lock
        if not self.database.isWritable(table.tableName):
            print(f"Error: Table {table.tableName} is locked!")
            return

        table.delete(self.conditions)

        self.database.successfulTransactions += 1

    def __parseDelete(self, queryInput):
        ''' 
        Purpose : Parse input to build a delete statement
        Parameters : 
            queryInput: The input to parse
        Returns: tablename, conditions
        ''' 
        tableName = queryInput[0] 

        conditions = queryInput[2:]

        return tableName, conditions
