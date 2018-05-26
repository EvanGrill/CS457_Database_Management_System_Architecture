################################################################################
#	File : InsertStatement.py
#	Purpose : Models a SQL INSERT statement
#	Author : Jonathan Weatherspoon, Evan Grill
#	Date : March 25, 2018
################################################################################

from Query import Query 

################################################################################
#	Class : InsertStatement
#	Purpose : Models an INSERT statement
################################################################################
class InsertStatement(Query):
    '''Models an INSERT statement'''
    def __init__(self, queryInput):
        '''
    	Purpose : Initialize an insert statement
    	Parameters : 
        	tableName: The name of the table to insert data into
            attrDataDict: A dictionary where the key / value pairs are the attribute
                          names and the corresponding data 
    	Returns: None
        '''
        self.__parseInsert(queryInput)
        self.database = None 

    def execute(self):
        ''' 
        Purpose : Execute an insert statement
        Parameters : 
            None
        Returns: None
        ''' 
        if self.database is None:
            print ("!Failed to execute INSERT on table", self.tableName, "because no database is selected!")
            return 


        table = self.database.getTableByName(self.tableName)

        if table is None:
            print ("!Failed to execute INSERT on table", self.tableName, "because it does not exist!")
            return

        # Check for a lock
        if not self.database.isWritable(table.tableName):
            print(f"Error: Table {table.tableName} is locked!")
            return

        table.insert(self.values)
        
        self.database.successfulTransactions += 1

    def __parseInsert(self, queryInput):
        ''' 
        Purpose : Parse data to build an insert statement
        Parameters : 
            queryInput: Input to parse
        Returns: None
        ''' 
        tableName = queryInput[0] 
        
        # queryInput[1:] is now the values to insert. last value might have ) attached 
        queryInput = queryInput[1:]

        # Remove "values" and '(' from first element 
        queryInput[0] = queryInput[0][6:].replace('(', '')
        if(queryInput[0] == ''):
            queryInput = queryInput[1:] 
            
        values = []
        for i in range(len(queryInput)):
            temp = queryInput[i].replace(')', '').replace('(', '').strip().split(',')
            for val in temp:
                if val != '':
                    values.append(val.replace("'", ''))

        self.tableName = tableName # Sanitized table name 
        self.values = values # List of sanitized values to insert 
