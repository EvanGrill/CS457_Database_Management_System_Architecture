################################################################################
#	File : Table.py
#	Purpose : Models a table within a database
#	Author : Jonathan Weatherspoon, Evan Grill
#	Date : Feb 08, 2018
################################################################################

import os
import Joins

################################################################################
#	Class : Table
#	Purpose : Models a table in a SQL database and stores data in that table
################################################################################


class Table(object):
    def __init__(self, dbName, tableName, newlyCreated=False):
        # Initialize member variables.
        self.dbName = dbName
        self.tableName = tableName
        # if not self.tableName.endswith('.tbl'):
        #    self.tableName += '.tbl'
        self.fileName = tableName + ".tbl"
        self.safeName = tableName.lower()

        # If the table is not brand new:
        if not newlyCreated:
            # Check to ensure the table does exist.  If it does, read in
            # the table schema and data.
            dbDir = './' + self.dbName + '/'
            if self.fileName not in os.listdir(dbDir):
                print("!Table", self.tableName, "not found")
            else:
                self.schema = None
                self.rows = []
                # Read in and store all data from the table
                fid = open(dbDir + self.fileName)
                lines = fid.readlines()

                # Set the table schema
                self.schema = self.__parseSchema(lines[0])

                # Store each row as an entry in the dictionary
                for row in lines[1:]:
                    parsedRow = self.__parseRow(row)
                    self.rows.append(parsedRow)
                fid.close()
        # If the table is new, create empty table.
        else:
            self.schema = {}
            self.rows = []

    def getFriendlyName(self):
        '''
        Purpose : Get the table name minus the extension
     	Parameters :
    		None
    	Returns: A string containing the friendly name of the table
        '''
        return self.safeName

    def save(self):
        '''
        Purpose:    Write a table file to disk for persistent storage
    	Parameters: None
    	Returns:    None
        '''
        if self.dbName is not None:
            dbDir = './' + self.dbName + '/'
            fid = open(dbDir + self.fileName, mode='w')

            # Write the schema as the first row
            fid.write(self.getSchemaString())

            # Write each row to the file
            for row in self.rows:
                fid.write(" | ".join(row.values()))
                fid.write('\n')
            fid.close()

    def getDataByAttrName(self, attrList, where=None, joinType=None):
        '''
        Purpose:    Fetches table data from specified columns.
        Parameters: attrList: array of attributes to fetch.
                    where: conditional to filter (Not yet implemented)
        Returns:    array of tuples filtered by attributes requested.
        '''
        returnSet = []
        if ["*" in tmp for tmp in attrList][0]:
            attrList = list(self.schema.keys())
        if where is None or len(where) == 0:
            for row in self.rows:
                temp = {}
                for key in list(self.schema.keys()):
                    if key in attrList:
                        temp[key] = row[key]
                returnSet.append(temp)
        else:
            for row in self.rows:
                if self.conditionCheck(where[0], where[1], where[2], row):
                    temp = self.__buildRow(row, attrList)
                    returnSet.append(temp)
                elif joinType == Joins.LEFT_OUTER_JOIN:
                    # Set all values from left table to NULL 
                    prefix = self.__getPrefix(list(self.schema.keys())[0])
                    temp = self.__buildRow(row, attrList, joinType, prefix)
                    returnSet.append(temp)



        return returnSet

    def addColumn(self, attrName, dataType):
        '''
        Purpose:    Adds a column to the table.
        Parameters: attrName: column name to be added.
                    dataType: datatype of added column.
        Returns:    Returns True if column added.
        '''
        # Check if column already exists
        if self.attrExists(attrName):
            print("!Failed to add column", attrName,
                  "because it already exists")
            return False

        self.schema[attrName] = dataType
        for row in self.rows:
            row[attrName] = "NULL"
        return True

    def dropColumn(self, attrName):
        '''
        Purpose:    Drops a column from the table.
        Parameters: attrName: column name to be dropped.
        Returns: True if the column is dropped. False if not.
        '''
        if not self.__attrInSchema(attrName):
            return False

        self.schema.pop(attrName)
        for row in self.rows:
            row.pop(attrName)
        return True

    def modifyColumn(self, attrName, dataType):
        '''
        Purpose:    alters the type of a given column in the table.
        Parameters: attrName: column to be re-typed.
                    dataType: new datatype
        Returns: True if column type is modified. False if not.
        '''
        if not self.__attrInSchema(attrName):
            return False

        castable = True
        for row in self.rows:
            if not self.__isCastableTo(row[attrName], dataType):
                castable = False
                break
        if castable:
            self.schema[attrName] = dataType
            return True
        else:
            return False

    def setSchema(self, schema):
        '''
        Purpose:    Set the schema of a Table object
     	Parameters: schema: A dictionary containing the schema for the table
    	Returns:    None
        '''
        self.schema = schema

    def getSchemaString(self):
        '''
        Purpose:    Creates formatted schema string for printing to file.
        Parameters: None
        Returns:    string of current schema.
        '''
        schemaStr = ""
        index = 0
        iterSchema = list(self.schema.items())
        for attributeName, dataType in iterSchema:
            schemaStr += attributeName + " (" + dataType + ") "
            schemaStr += " | " if (index < len(self.schema) - 1) else "\n"
            index += 1
        return schemaStr

    def insert(self, values, columns=None):
        '''
        Purpose : Insert some data to the table
    	Parameters :
    		values: A list of values to insert in the order of the table schema
            columns: A list of columns. If not included, length of values must be
                     the same as length of schema
    	Returns: Boolean corresponding to the status of the operation
        '''
        # For every attribute in the table, check if it exists in attrDataDict
        # If it does: Add the attrDataDict value to that spot, else add "NULL" there

        if columns is None:
            # Check that we have a value for every attribute
            if len(values) != len(self.schema):
                print("!Failed to insert on table", self.tableName,
                      "because there must be a value for every attribute")
                return False

            row = {}
            index = 0
            for attrName, dataType in list(self.schema.items()):
                # Check that the data type of this element is correct
                if not self.__isCastableTo(values[index], dataType):
                    print("!Failed to insert on table", self.tableName,
                          "because data type does not match schema!")
                    return False
                # Add to the row
                row[attrName] = values[index]
                index += 1
            self.rows.append(row)
            print("1 new record inserted.")
            return True

        else:
            if len(values) != len(columns):
                print("!Failed to insert on table", self.tableName,
                      "because there must be a value for every attribute")
                return False
            
            row = {}
            index = 0 
            for attrName, dataType in list(self.schema.items()):
                # Check to see if the column has data 
                if attrName in columns:
                    # Check the data type 
                    if not self.__isCastableTo(values[index], dataType):
                        print("!Failed to insert on table", self.tableName,
                              "because data type does not match schema!")
                        return False
                    # Add to the row 
                    row[attrName] = values[index]
                    index += 1
                else:
                    # Set value to NULL 
                    row[attrName] = "NULL"
            self.rows.append(row)
            print("1 new record inserted.")
            return True 

    def delete(self, where=None):
        '''
        Purpose : Delete items from the table given certain conditions
    	Parameters :
    		where: A list of conditions. If None, delete all entries in the table
    	Returns: Boolean representing status of the operation
        '''
        if where is None or len(where) == 0:
            self.rows = []
            return True

        # Check that the column exists
        column = where[0]

        if not self.attrExists(column):
            print("!Failed to delete from table", self.tableName,
                  "because column", column, "does not exist")
            return False

        # Go through all data, check conditions. If the condition passes, delete
        delRows = self.getDataByAttrName('*', where)
        for row in delRows:
            self.rows.remove(row)
        print(len(delRows), "records" if len(
            delRows) > 1 else "record", "deleted.")
        return True

    def update(self, updates, where=None):
        '''
        Purpose : Updates items from the table given certain conditions
    	Parameters :
            updates: A dictionary of updates to make to the table
    		where: A list of conditions. If None, update all entries in the table
    	Returns: Boolean representing status of the operation
        '''
        # Verify that the columns to update exist in the table.
        for column, value in list(updates.items()):
            if not self.attrExists(column):
                print("!Failed to update table", self.tableName,
                      "because", column, "is not an attribute in the table.")
                return False
        # If where isn't specified, update every row.
        if where is None or len(where) == 0:
            for row in self.rows:
                for column, value in list(updates.items()):
                    row[column] = value
            return True

        # Else update only the rows that match the where.
        column = where[0]
        operator = where[1]
        value = where[2]

        # Verify that the where column exists.
        if not self.attrExists(column):
            print("!Failed to update table", self.tableName, "because",
                  column, "is not an attribute in the table.")
            return False

        # For every row of the table, check against the where conditional and update if successful.
        for row in self.rows:
            if self.conditionCheck(column, operator, value, row):
                for upCol, upVal in list(updates.items()):
                    row[upCol] = upVal

        return True

    def __parseSchema(self, schemaInput):
        '''
        Purpose:    Parses schema in format saved to files.
     	Parameters: schemaInput: The input to parse into a schema
    	Returns: A dictionary containing the schema for the table
        
        '''
        schema = {}

        columns = schemaInput.split('|')
        for column in columns:
            column = column.strip()
            data = column.split()
            attributeName = data[0]
            dataType = data[1][1:-1]
            schema[attributeName] = dataType

        return schema

    def __parseRow(self, rowInput):
        '''
    	Purpose:    Parse a row of data into a structured form
    	Parameters: rowInput: The row to parse
    	Returns:    A dictionary containing the data and column names
        '''
        columns = rowInput.split('|')
        attributes = list(self.schema.keys())

        row = {}
        for i in range(len(columns)):
            row[attributes[i]] = columns[i].strip()

        return row

    def __attrInSchema(self, attrName):
        '''
    	Purpose:    Determines if a given column (by name) exists in the table.
    	Parameters: attrName: column name to search for.
    	Returns:    True if the column is in the table, False if not.
        '''
        return (attrName in self.schema.keys())

    def __isCastableTo(self, val, newType):
        '''
    	Purpose : Checks to see if a value is castable to a type
    	Parameters :
    		val: The value to try to cast
            newType: The type to attempt to cast val to
    	Returns: Boolean determining the result of the operation
        '''
        newType = self.getType(newType)
        if newType is str:
            return True
        try:
            self.castColumn(val, newType)
            return True
        except ValueError:
            return False

    def attrExists(self, attrName):
        ''' 
        Purpose : Determine if an attribute exists in the table schema
        Parameters : 
            attrName: The attribute name to search for 
        Returns: Boolean value corresponding to the result
        ''' 
        return self.__attrInSchema(attrName)

    def conditionCheck(self, column, operator, value, row):
        ''' 
        Purpose : Check if a set of values matches a conditional statement
        Parameters : 
            column: LHS of the conditional
            operator: operator as a string
            value: RHS of the conditional
            row: The row to compare with
        Returns: True if the values match the given conditional statement
        ''' 
        # Data sanitization
        column = column.strip().replace("'", '')
        operator = operator.strip()
        value = value.strip().replace("'", '')

        castType = None 
        try:
            castType = self.getType(self.schema[column])
            if '.' in value:
                castType2 = self.getType(self.schema[value])
                value = self.castColumn(row[value], castType2)
            else:
                value = self.castColumn(value, castType)
        except:
            return False 

        testValue = self.castColumn(row[column], castType)
        return self.__conditionCompare(testValue, operator, value)

    def __conditionCompare(self, lVal, operator, rVal):
        ''' 
        Purpose : Compare two values based on a string operator
        Parameters : 
            lVal: LHS
            operator: Operator as a string
            rVal: RHS
        Returns: The result of the comparison
        ''' 
        if operator == "=":
            return lVal == rVal
        if operator == "!=" or operator == "<>":
            return lVal != rVal
        if operator == "<":
            return lVal < rVal
        if operator == ">":
            return lVal > rVal
        if operator == "<=":
            return lVal <= rVal
        if operator == ">=":
            return lVal >= rVal
        return False

    def castColumn(self, column, castType):
        ''' 
        Purpose : Cast a value to a type
        Parameters : 
            column: The value to cast
            castType: The type to cast to
        Returns: The casted value
        ''' 
        return castType(column)

    def getType(self, string):
        ''' 
        Purpose : Get a python type from a string
        Parameters : 
            string: The string to parse
        Returns: A corresponding python type. None if no type matches
        ''' 
        string = string.split("(")
        string = string[0]
        string = string.lower()
        if string == "char":
            return str
        if string == "varchar":
            return str
        if string == "int":
            return int
        if string == "float":
            return float
    
    def printTableByAttr( self, attrList, where=None, joinType=None):
        ''' 
        Purpose : Print a table given attributes / conditionals
        Parameters : 
            attrList: A list of attributes to print
            where: Optional conditional
            joinType: Optional join type
        Returns: None
        ''' 
        header = ""
        body = ""
        rows = self.getDataByAttrName(attrList, where, joinType)
        index = 1
        if ["*" in tmp for tmp in attrList][0]:
            attrList = self.schema.keys()
        for attr in attrList:
            cleanAttr = attr.split(".")
            if( len(cleanAttr) > 1):
                cleanAttr = cleanAttr[1]
            else:
                cleanAttr = cleanAttr[0]
            header += cleanAttr + " " + self.schema[attr]
            header += "|" if(index<len(attrList)) else ""
            index += 1
        print(header)

        for row in rows:
            index = 1
            for value in row.values():
                body += value
                body += "|" if(index<len(row)) else ""
                index += 1
            body += "\n"
        body = body[0:-1]
        print(body)

    def printTable( self ):
        ''' 
        Purpose : Print an entire table (SELECT *)
        Parameters : 
            None
        Returns: None
        ''' 
        self.printTableByAttr( ["*"] )

    def __getPrefix(self, attribute):
        '''
    	Purpose : Get the prefix of a given attribute
    	Parameters : 
    		attribute: The attribute to get
    	Returns: The prefix (before the .) of the attribute. None if there is no .
        '''
        result = attribute.split('.')
        if(len(result) == 1):
            return None 
        return result[0]

    def __buildRow(self, row, attrList, joinType=Joins.NO_JOIN, prefix=None):
        '''
    	Purpose : Build a row based on a join type
    	Parameters : 
            row: The row currently being looked at 
            attrList: The list of attributes to be returned 
    		joinType: the type of join performed
            prefix: The tablename used for building the row
    	Returns: A row built based on the join type / prefix given
        '''
        temp = {}
        for key in list(self.schema.keys()):
            if key in attrList:
                if joinType == Joins.LEFT_OUTER_JOIN and self.__getPrefix(key) != prefix:
                    temp[key] = ""
                else:
                    temp[key] = row[key]
        return temp 

    def __addTableNameToRow(self, tname, row):
        '''
    	Purpose : Add a table's name to a row's keys
    	Parameters :
    		tname: The name of the table to prepend to the row's keys
            row: The row to correct
    	Returns: A new row with the table name prepended to each key
        '''
        newRow = {}
        for k, v in list(row.items()):
            newRow[tname + "." + k] = v
        return newRow

    @classmethod 
    def OuterJoin(cls, ltable, rtable, joinType, conditions):
        ''' 
        Purpose : Do an outer join on two tables 
        Parameters : 
            ltable: The left table 
            rtable: The right table
            joinType: The type of join (use Joins.TYPE)
            conditions: Conditions for adding rows
        Returns: Table object representing the join operation
        ''' 
        found = False 
        T = Table(None, "MERGE", True)
        L, R = None, None
        if joinType == Joins.LEFT_OUTER_JOIN:
            L = ltable 
            R = rtable 
        elif joinType == Joins.RIGHT_OUTER_JOIN:
            R = ltable 
            L = rtable 
        else:
            return None

        lName = L.getFriendlyName()
        rName = R.getFriendlyName()

        lCol = conditions[0].strip() # lCol = conditions[0].replace(lName, '')[1:]
        operator = conditions[1].strip()
        rCol = conditions[2].strip() # rCol = conditions[2].replace(rName, '')[1:]

        for lRow in L.rows:
            found = False 
            lRow = cls.__addTableNameToRow(L, lName, lRow)
            lVal = lRow[lCol]
            index = 1
            for rRow in R.rows:
                rRow = cls.__addTableNameToRow(R, rName, rRow)
                rVal = rRow[rCol]
                if(cls.__conditionCompare(L, lVal, operator, rVal)):
                    # Add the row normally 
                    row = {**lRow, **rRow}
                    T.rows.append(row)
                    found = True 
                elif index == len(R.rows) and found == False:
                    for k in list(rRow.keys()):
                        rRow[k] = ""
                    row = {**lRow, **rRow}
                    T.rows.append(row)
                index += 1
        return T 

