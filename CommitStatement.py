################################################################################
#	File : CommitStatement.py
#	Purpose : Models a COMMIT SQL statement
#	Author : Jonathan Weatherspoon, Evan Grill  
#	Date : May 9, 2018
################################################################################

from Query import Query 

class CommitStatement(Query):
    '''Models a COMMIT SQL statement'''
    def __init__(self):
        pass 

    def execute(self): 
        ''' 
        Purpose : Execute a COMMIT statement
        Parameters : 
            None
        Returns: None
        ''' 
        if self.database is not None:
            self.database.transactionInProgress = False 
            self.database.save() 

            if self.database.successfulTransactions > 0:
                print("Transaction commited.")
            else: 
                print("Transaction abort.")