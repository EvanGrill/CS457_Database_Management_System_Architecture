################################################################################
#	File : AlterStatement.py
#	Purpose : Models an ALTER statment in a SQL database
#	Author : Jonathan Weatherspoon, Evan Grill
#	Date : Feb 08, 2018
################################################################################

from Query import Query 

################################################################################
#	Class : AlterStatement
#	Purpose : Models an ALTER statement in a SQL database
################################################################################
class AlterStatement(Query):
    '''Models an ALTER statement in a SQL database'''
    def __init__(self, queryInput):
        super(AlterStatement, self).__init__()
        self.queryInput = queryInput
        self.database = None

    ###########################################################################
    #   Function:   execute
    #   Purpose:    Implementation of the ALTER statement.  (Presently only has
    #               ADD implemented to conform with PA1 requirements.
    #   Parameters: None
    #   Returns: None
    ###########################################################################
    def execute(self):
        '''
        Purpose:    Implementation of the ALTER statement.  (Presently only has
                    ADD implemented to conform with PA1 requirements.
        Parameters: None
        Returns: None
        '''
        # Define states to execute correct helper function.
        states = {
            "ADD": self.__executeAdd,
            "DROP": self.__executeDrop,
            "MODIFY": self.__executeModify
        }

        # Fetch table name from SQL query.
        tableName = self.queryInput[0]

        # If the query is of sufficient length and uses a supported state,
        # call the helper function.
        if len(self.queryInput) > 1:
            state = self.queryInput[1].upper()
            if state in states.keys():
                states[state](tableName, self.queryInput[2:])
                print ("Table", tableName, "modified.")
            else:
                print ("!Invalid SQL Statement!")
        else:
            print ("!Invalid SQL Statement!")
    
    ###########################################################################
    #   Function:   __executeAdd
    #   Purpose:    Private helper function to perform an ADD column.
    #   Parameters: None
    #   Returns: None
    ###########################################################################
    def __executeAdd(self, tableName, addInput):
        '''
        Purpose:    Private helper function to perform an ADD column.
        Parameters: None
        Returns: None
        '''
        if len(addInput) > 1:
            attrName = addInput[0].strip()
            dataType = addInput[1].strip()
            table = self.database.getTableByName(tableName)
            if table is not None:

                # Check for a lock
                if not self.database.isWritable(table.tableName):
                    print(f"Error: Table {table.tableName} is locked!")
                    return

                table.addColumn(attrName, dataType)

                self.database.successfulTransactions += 1

    ###########################################################################
    #   Function:   __executeDrop
    #   Purpose:    Private helper function to perform a DROP column. (Not
    #               presently implemented)
    #   Parameters: None
    #   Returns: None
    ###########################################################################
    def __executeDrop(self, tableName, dropInput):
        pass

    ###########################################################################
    #   Function:   __executeModify
    #   Purpose:    Private helper function to perform a MODIFY column. (Not
    #               presently implemented)
    #   Parameters: None
    #   Returns: None
    ###########################################################################    
    def __executeModify(self, tableName, modifyInput):
        pass
