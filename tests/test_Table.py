
import unittest 
import os, shutil 

import config 

from Table import Table

class TestTable(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ''' 
        Purpose : Make a test database
        Parameters : 
            None
        Returns: None
        ''' 
        os.mkdir(config.DB_NAME)

    @classmethod
    def tearDownClass(cls):
        ''' 
        Purpose : Remove the test database
        Parameters : 
            None
        Returns: None
        ''' 
        shutil.rmtree(config.DB_NAME)

    def setUp(self):
        ''' 
        Purpose : Make a test table
        Parameters : 
            None
        Returns: None
        ''' 
        config.CreateTable()
        self.tbl = Table(config.DB_NAME, config.TBL_NAME)

    def test_getFriendlyName(self):
        ''' 
        Purpose : Get a table's friendly name
        Parameters : 
            None
        Returns: None
        ''' 
        expected = config.TBL_NAME
        self.assertEqual(self.tbl.getFriendlyName(), expected)

    def test_save(self):
        ''' 
        Purpose : Test saving a table to disk
        Parameters : 
            None
        Returns: None
        ''' 
        # Save the default table and check the files contents
        self.tbl.save()
        
        fid = open(f'{config.DB_NAME}/{config.TBL_FILE}')
        contents = fid.read()
        self.assertEqual(contents, config.TBL_CONTENTS)

    def test_addColumn(self):
        ''' 
        Purpose : Test adding a column to a table   
        Parameters : 
            None    
        Returns: None
        ''' 
        # Try to add a column that already exists 
        self.assertFalse(self.tbl.addColumn("a1", "char(5)"))

        # Add a column that doesn't exist and make sure the data is correct
        self.assertTrue(self.tbl.addColumn("s2", "char(10)"))
        schema = self.tbl.schema 
        
        self.assertEqual(schema["s2"], "char(10)")

        for row in self.tbl.rows:
            self.assertEqual(row["s2"], "NULL")

    def test_dropColumn(self):
        ''' 
        Purpose : Test dropping a column from a table
        Parameters : 
            None
        Returns: None
        ''' 
        # Test when attribute does not exist
        self.assertFalse(self.tbl.dropColumn("s2"))

        # Drop a column that exists
        self.assertTrue(self.tbl.dropColumn("a1"))
        with self.assertRaises(KeyError):
            self.tbl.schema["a1"]
        
        # Ensure the error is raised for all rows 
        for row in self.tbl.rows:
            with self.assertRaises(KeyError):
                row["a1"]

    def test_modifyColumn(self):
        ''' 
        Purpose : Test modifying a column in a table
        Parameters : 
            None
        Returns: None
        ''' 
        # Modify a column that doesn't exist
        self.assertFalse(self.tbl.modifyColumn("a2", "float"))

        # Modify an existing column with data that is correctly castable 
        self.assertTrue(self.tbl.modifyColumn("a1", "varchar(20)"))

        # Modify an existing column with data that cannot be casted to the type
        self.assertFalse(self.tbl.modifyColumn("f1", "int"))

    def test_setSchema(self):
        ''' 
        Purpose : Test setting the schema of a table
        Parameters : 
            None
        Returns: None
        ''' 
        newSchema = {'a4': 'int', 'f3': 'float'}
        self.tbl.setSchema(newSchema)
        self.assertEqual(self.tbl.schema, newSchema)

    def test_getSchemaString(self):
        ''' 
        Purpose : Test getting a schema string from a table
        Parameters : 
            None
        Returns: None
        ''' 
        expected = "a1 (int)  | s1 (varchar(20))  | f1 (float) \n"
        self.assertEqual(self.tbl.getSchemaString(), expected)

    def test_delete(self):
        ''' 
        Purpose : Test deleting rows 
        Parameters : 
            None
        Returns: None
        ''' 
        # delete when column doesnt exist
        self.assertFalse(self.tbl.delete(["f3", "=", "0"]))

        # delete a single row 
        self.assertTrue(self.tbl.delete(["s1", "=", "hello"]))
        for row in self.tbl.rows:
            self.assertFalse("hello" in row.values())

        # Delete all rows 
        self.assertTrue(self.tbl.delete())
        self.assertTrue(self.tbl.rows == [])

    def test_attrExists(self):
        ''' 
        Purpose : Test checking if an attribute exists
        Parameters : 
            None
        Returns: None
        ''' 
        self.assertFalse(self.tbl.attrExists("f3"))
        self.assertTrue(self.tbl.attrExists("a1"))

    def test_conditionCheck(self):
        ''' 
        Purpose : Test checking conditions
        Parameters : 
            None
        Returns: None
        ''' 
        conditions = [
            (["s1", "=", "hello"], True),
            (["a1", "< ", "8"], True),
            (["f1", " > ", "1"], True),
            (["a1", "!=", "2"], True),
            (["f1", "<>", "3.14"], False),
            (["a1", "<=", "1"], True),
            (["f1", ">=", "3.15"], False),
            (["f3", "!=", "10"], False),
            (["a1", "~=", "1"], False)
        ]

        tRow = None
        for row in self.tbl.rows:
            if row['a1'] == '1':
                tRow = row 
                break 
        
        for condition in conditions:
            k = condition[0]
            v = condition[1]
            msg = f'k={k},v={v}'
            self.assertEqual(self.tbl.conditionCheck(k[0], k[1], k[2], tRow), v, msg=msg)

    def test_castColumn(self):
        ''' 
        Purpose : Test casting columns to other types
        Parameters : 
            None
        Returns: 
        ''' 
        self.assertIsInstance(self.tbl.castColumn(1, str), str)
        self.assertIsInstance(self.tbl.castColumn("1", int), int)
        self.assertIsInstance(self.tbl.castColumn("1", float), float)

        with self.assertRaises(ValueError):
            self.tbl.castColumn("1.3", int)

    def test_getType(self):
        ''' 
        Purpose : Test getting a type from a string
        Parameters : 
            None
        Returns: None
        ''' 
        self.assertIs(self.tbl.getType("char"), str)
        self.assertIs(self.tbl.getType("varchar"), str)
        self.assertIs(self.tbl.getType("int"), int)
        self.assertIs(self.tbl.getType("float"), float)
        self.assertIs(self.tbl.getType("non-existant"), None)
        

    def test_update(self):
        ''' 
        Purpose : Test updating a table's rows
        Parameters : 
            None
        Returns: None
        ''' 
        # Ensure it fails when columns dont exist
        self.assertFalse(self.tbl.update({'a1': 3, 's1': 'hi', 'f3': 3.1}))

        updates = {'a1': 12, 's1': 'pls'}
        # When the column specified by where doesn't exist
        self.assertFalse(self.tbl.update(updates, ['a12', '=', '1']))

        # Get index of update for next test 
        index = 0 
        for row in self.tbl.rows:
            if int(row['a1']) == 1:
                break
            index += 1

        # Should update the table correctly
        self.assertTrue(self.tbl.update(updates, ['a1', '=', '1']))
        row = self.tbl.rows[index]
        for k, v in list(updates.items()):
            self.assertEqual(row[k], v)

        # Set all f1 values to 0.1
        self.assertTrue(self.tbl.update({'f1': 0.1}))
        for row in self.tbl.rows:
            self.assertEqual(row['f1'], 0.1)

    def test_insert(self):
        ''' 
        Purpose : Test inserting data into a table
        Parameters : 
            None
        Returns: None
        ''' 
        # Test insert when length of values is greater than length of columns
        self.assertFalse(self.tbl.insert([1,1,1,1]))

        # Test when lenght of values is less than length of columns
        self.assertFalse(self.tbl.insert([1,1]))

        # Test when data is not castable (put in wrong order)
        self.assertFalse(self.tbl.insert(["hi", 1, 3.14]))

        # Test when columns is specified and lengths are different 
        self.assertFalse(self.tbl.insert([1,"hi"], ["a1", "s1", "f1"]))

        # Test when columns is specified and data types are not castable 
        self.assertFalse(self.tbl.insert(["hi", "no"], ["s1", "f1"]))

        # Test when no columns specified and row is actually inserted
        row = {'a1': 2, 's1': 'str', 'f1': 4.0} 
        self.assertTrue(self.tbl.insert(list(row.values())))

        self.assertIn(row, self.tbl.rows) 

        # Test when columns are specified and row is inserted 
        self.assertTrue(self.tbl.insert(['hello', 2.19], ['s1', 'f1']))

        row = {'a1': 'NULL', 's1': 'hello', 'f1': 2.19}
        self.assertIn(row, self.tbl.rows)

    def test_getDataByAttrName(self):
        ''' 
        Purpose : Test getting data by an attribute name
        Parameters : 
            None
        Returns: None
        ''' 
        # Get everything from the table 
        dataset = self.tbl.getDataByAttrName(["*"])
        expected = [
            {'a1': "1", 's1': 'hello', 'f1': "3.14"},
            {'a1': "8", 's1': 'this is a string', 'f1': "5"},
            {'a1': "100", 's1': 'x', 'f1': "3.14"}
        ]

        self.assertEqual(dataset, expected)

        # Get just a few columns
        dataset = self.tbl.getDataByAttrName(["a1", "s1"])
        expected = [
            {'a1': "1", 's1': 'hello'},
            {'a1': "8", 's1': 'this is a string'},
            {'a1': "100", 's1': 'x'}
        ]

        self.assertEqual(dataset, expected)

        # get everything with a condition
        dataset = self.tbl.getDataByAttrName("*", ["s1", "=", "hello"])
        expected = [
            {'a1': "1", 's1': 'hello', 'f1': "3.14"}
        ]

        self.assertEqual(dataset, expected)

        # Get set of columns with condition
        dataset = self.tbl.getDataByAttrName(["s1"], ["a1", ">", "1"])
        expected = [
            {'s1': 'this is a string'},
            {'s1': 'x'}
        ]

        self.assertEqual(dataset, expected)

if __name__ == '__main__':
    unittest.main()
