################################################################################
#	File : Query.py
#	Purpose : Models a generic Query on a SQL database
#	Author : Jonathan Weatherspoon, Evan Grill
#	Date : Feb 08, 2018
################################################################################

################################################################################
#	Class : Query
#	Purpose : Parent class for all SQL queries
################################################################################
class Query(object):
    def __init__(self):
        pass 

    def setDBContext(self, dbContext):
        '''
    	Purpose : Set the database context for a Query. Required for some queries
                  to properly execute.
    	Parameters : 
    		dbContext: The currently selected database context
    	Returns: None
        '''
        self.database = dbContext
