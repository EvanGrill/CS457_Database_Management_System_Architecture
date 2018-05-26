################################################################################
#	File : UpdateStatement.py
#	Purpose : Models a SQL UPDATE statement
#	Author : Jonathan Weatherspoon, Evan Grill  
#	Date : March 25, 2018
################################################################################

from Query import Query 
import re 

################################################################################
#	Class : UpdateStatement
#	Purpose : Models a SQL UPDATE statement
################################################################################
class UpdateStatement(Query):
    def __init__(self, queryInput):
        self.database = None
        self.tableName, self.targets, self.conditions = self.__parseUpdate(queryInput) 

    def execute(self):
        if self.database is None:
            print("!Failed to execute query because no database is selected!")
            return None 

        if self.tableName is not None:
            table = self.database.getTableByName(self.tableName)

            if table is None:
                print("!Failed to execute query on table", self.tableName, "because it does not exist!")
                return None 

            # Check for a lock
            if not self.database.isWritable(table.tableName):
                print(f"Error: Table {table.tableName} is locked!")
                return

            table.update(self.targets, self.conditions)

            self.database.successfulTransactions += 1

    def __parseUpdate(self, queryInput):
        tableName = queryInput[0] 

        if queryInput[1].lower() != "set":
            print ("!Invalid SQL statement!")
            return None, None, None 
        
        # Join the input into a string so it can be manipulated more easily
        joinStr = ' '.join(queryInput[2:]) 
        splitJoin = re.split("where", joinStr, flags=re.IGNORECASE)

        updates = {}
        conditions = None

        # Parse out the required bits of the statement 
        updatesSplit = re.split("=|,", splitJoin[0])
        if len(updatesSplit) < 2:
            print ("!Invalid SQL Statement!")
            return None, None, None,
        
        for i in range(0, len(updatesSplit), 2):
            try: 
                col = updatesSplit[i].strip().replace("'", '')
                val = updatesSplit[i + 1].strip().replace("'", '')
                updates[col] = val 
            except:
                print ("!Invalid SQL statement!")
                return None, None, None 

        # Check for a where clause 
        if len(splitJoin) > 1:
            conditions = list(filter(None, re.split(r"(=|!=|<>|<|>|<=|>=)", splitJoin[1])))
            conditions = [x.strip().replace("'", '') for x in conditions]
        
        return tableName, updates, conditions
            


