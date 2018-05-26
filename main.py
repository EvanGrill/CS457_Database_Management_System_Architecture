import sys

from Parser import Parser
from UseStatement import UseStatement
from Query import Query
from Database import Database 

EXIT_COMMAND = "EXIT"

parser = Parser()
dbContext = Database()

def main():
    global dbContext 

    print ("Enter SQL queries ending with ; or .EXIT to end session")
    while True:
        SQLInput = GetSQLInput()
        if SQLInput == "":
            continue
        parsedQueries = parser.parse(SQLInput)
        
        if ExecuteQueries(parsedQueries) == EXIT_COMMAND:
            break

    # Save before exiting 
    if dbContext is not None:
        dbContext.save() 
    
    print ("All done.")

################################################################################
#	Function : GetSQLInput
#	Purpose : Get SQL input from the user at the interpreter
#   Note : Must end with either ; or .exit (or some variation)
#	Parameters : 
#		None
#	Returns: A string containing the SQL input entered by the user. 
################################################################################
def GetSQLInput():
    ''' 
    Purpose : Get SQL input from the user at the interpreter
    Note : Must end with either ; or .exit (or some variation)
 	Parameters : 
		None
	Returns: A string containing the SQL input entered by the user.
    ''' 
    SQLInput = ""
    try:
        SQLInput = input()
        if SQLInput.startswith("--"): SQLInput = ""
        while not (SQLInput.endswith(';') or SQLInput.upper().endswith('.EXIT')):
            line = input()
            if line.startswith("--"): line = ""
            SQLInput += ' ' + line
        return SQLInput
    except EOFError:
        SQLInput += "\n"
        return SQLInput

################################################################################
#	Function : ExecuteQueries
#	Purpose : Execute a queue of SQL queries returned by a parser
#	Parameters : 
#		queries: The queue of queries to execute
#	Returns: EXIT_COMMAND if the .EXIT query is executed. None otherwise.
################################################################################
def ExecuteQueries(queries):
    ''' 
    Purpose : Execute a queue of SQL queries returned by a parser
	Parameters : 
		queries: The queue of queries to execute
	Returns: EXIT_COMMAND if the .EXIT query is executed. None otherwise. 
    ''' 
    global dbContext

    while not queries.empty():
        query = queries.get()        

        if type(query) is UseStatement:
            # If query type is USE and a database is currently selected,
            # Write the current DB to disk before using the next 
            dbContext.save() 
        if isinstance(query, Query):
            query.setDBContext(dbContext)
        if query == EXIT_COMMAND:
            return EXIT_COMMAND
        elif query is not None:
            retVal = query.execute()

            if not dbContext.transactionInProgress:
                for table in list(dbContext.tables.values()):
                    table.save()
            

            # If the query returned a database object, set the current
            # database context 
            if type(retVal) is Database:
                dbContext = retVal

# Ensure that main() function is called properly by the interpreter. 
if __name__ == "__main__":
    main()
