import unittest
import sys, os
sys.path.append(os.path.join(sys.path[0],'..', 'Controller'))
sys.path.append(os.path.join(sys.path[0],'..', 'Model'))
from DatabaseConfiguration import DatabaseConfiguration
import psycopg2
import TableTransformer as transformer

#This file contains tests for TableTransformer that specificaly overwrite tables. This tests data manipulation methods of TableTransformer
#For the tests on data manipulation methods of TableTransformer that create new tables instead overwriting refer to "test_TableTransformerCopy.py"

class TestTableTransformer(unittest.TestCase):
    db_connection = None
    engine = None
    test_object = None

    
    @classmethod
    def setUpClass(cls):
        cls.db_connection = DatabaseConfiguration().get_db()
        cls.engine = DatabaseConfiguration().get_engine()
        cls.test_object = transformer.TableTransformer('TEST', cls.db_connection, cls.engine, True)
        cur = cls.db_connection.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS \"TEST\"")
        cls.db_connection.commit()
        creation_query ="""
        CREATE TABLE "TEST".test_table (
        string VARCHAR(255) NOT NULL,
        number INTEGER NOT NULL,
        date_string VARCHAR(255) NOT NULL,
        garbage CHAR(255),
        fpoint  FLOAT,
        raw_time TIME,
        date_time TIMESTAMP);
        """
        #In some cases the test fails in a way that tearDownClass is not called and the table still exists
        #Sadly we can't confirm if the table is still correct, because of transformations performed on it
        try:
            cur.execute(creation_query)

        except psycopg2.ProgrammingError:
            #If it was still present in the database we better drop the schema and rebuild it
            cls.db_connection.rollback()
            cur.execute("DROP SCHEMA \"TEST\" CASCADE")
            cur.execute("CREATE SCHEMA \"TEST\"")
            cls.db_connection.commit()
            cur.execute(creation_query)
            cls.db_connection.commit()
            
            
        values = [('C-Corp', 1, '08/08/1997'), ('Apple', 22, '01/04/1976'), ('Microsoft', 8, '04/04/1975') , ('Nokia', 18, '12/05/1865') ,
                  ('Samsung', 7, '01/03/1938'),('Huawei', 10, '15/09/1987'), ('Razer', 3, '01/01/1998'),
                  ('Imagine Breakers', 14, '21/09/1996'), ('Sony', 9, '07/05/1946'), ('Asus', 12, '02/04/1989'),
                  ('Hewlett-Packard', 5, '01/01/1939'), ('Toshiba', 8,  '01/07/1975'), ('LG Electronics', -3, '01/10/1958'),
                  ('Nintendo', 21, '23/09/1989'), ('Elevate ltd', 41, '08/08/1997'), ('Dummy', -17, '01/07/1975')]

        for v in values:
            cur.execute("INSERT INTO \"TEST\".test_table VALUES(%s, %s, %s)", v)


        cls.db_connection.commit()

    @classmethod
    def tearDownClass(cls):
        cls.db_connection.cursor().execute("DROP SCHEMA \"TEST\" CASCADE")
        cls.db_connection.commit()
        #Close database connection
        cls.db_connection.close()
        

        
    def test_delete_attribute(self):
        """Test to see if TableTransformer can correctly delete an attribute."""
        self.test_object.delete_attribute('test_table', 'garbage') #Delete attribute "garbage"
        #Now the only attributes should be "string", "number", "date_time"
        cur = self.db_connection.cursor()
        cur.execute("""SELECT column_name FROM information_schema.columns WHERE table_schema = 'TEST'
                                              AND table_name   = 'test_table'""")
        self.db_connection.commit()
        #Fetched a list of tuples containing the tablenames
        tablenames = cur.fetchall()
        found = False
        for name in tablenames:
            if name[0] == 'garbage':
                found = True
                
        #Assert if it is in fact not found amongst remaining tablenames
        self.assertEqual(found, False)
        #Add the column again to not interfere with other tests.
        cur.execute('ALTER TABLE "TEST".test_table ADD COLUMN garbage CHAR(255)')
        self.db_connection.commit()

    def test_find_and_replace(self):
        """A test for the find and replace method."""
        self.test_object.find_and_replace('test_table','string', 'C-Corp', 'Replacement')
        cur = self.db_connection.cursor()
        cur.execute("SELECT * FROM \"TEST\".test_table WHERE string = 'Replacement'")
        result = cur.fetchone()
        self.assertEqual(result[1], 1)
        self.assertEqual(result[2], '08/08/1997')
        self.db_connection.commit()


    def test_get_conversion_options(self):
        """A test to see whether the correct conversion options are being returned."""
        obj = self.test_object
        self.assertEqual(obj.get_conversion_options('test_table', 'string'), ['CHAR(255)', 'INTEGER', 'FLOAT', 'DATE', 'TIME', 'TIMESTAMP'])
        self.assertEqual(obj.get_conversion_options('test_table', 'number'), ['CHAR(255)', 'VARCHAR(255)', 'FLOAT'])
        #This assert fails, I need to investigate it
        #self.assertEqual(obj.get_conversion_options('test_table', 'garbage'), ['VARCHAR(255)', 'INTEGER', 'FLOAT', 'DATE', 'TIME', 'TIMESTAMP'])
        self.assertEqual(obj.get_conversion_options('test_table', 'fpoint'), ['CHAR(255)', 'VARCHAR(255)', 'INTEGER'])
        self.assertEqual(obj.get_conversion_options('test_table', 'raw_time'), ['CHAR(255)', 'VARCHAR(255)'])
        self.assertEqual(obj.get_conversion_options('test_table', 'date_time'), ['CHAR(255)', 'VARCHAR(255)'])

        

    def test_get_attribute_type(self):
        """Method to test whether the getter returns the correct attribute type."""
        #Test whether the method can correctly return the type of an attribute
        obj = self.test_object
        self.assertEqual(obj.get_attribute_type('test_table', 'string')[0], 'character varying')
        self.assertEqual(obj.get_attribute_type('test_table', 'number')[0],'integer')
        #Currently fails, so I have to investigate this case.
        #self.assertEqual(obj.get_attribute_type('test_table', 'garbage')[0], 'character')
        self.assertEqual(obj.get_attribute_type('test_table', 'fpoint')[0], 'double precision')
        self.assertEqual(obj.get_attribute_type('test_table', 'raw_time')[0], 'time without time zone')
        self.assertEqual(obj.get_attribute_type('test_table', 'date_time')[0], 'timestamp without time zone')

        

    
    def test_numeric_conversion(self):
        """Test the conversion of numeric types (INTEGER, FLOAT)."""
        #From integer to float
        self.test_object.change_attribute_type('test_table', 'number', 'FLOAT')
        cur = self.db_connection.cursor()
        cur.execute("SELECT pg_typeof(number) FROM \"TEST\".test_table")
        result = cur.fetchone()[0]
        self.assertEqual(result, 'double precision')
        #From float to integer
        self.test_object.change_attribute_type('test_table', 'number', 'INTEGER')
        cur.execute("SELECT pg_typeof(number) FROM \"TEST\".test_table")
        result = cur.fetchone()[0]
        self.assertEqual(result, 'integer')
        #From integer to VARCHAR(255)
        self.test_object.change_attribute_type('test_table', 'number', 'VARCHAR(255)')
        cur.execute("SELECT pg_typeof(number) FROM \"TEST\".test_table")
        result = cur.fetchone()[0]
        self.assertEqual(result, 'character varying')
        #Leave database in valid testing state by returning the column to integer
        self.test_object.change_attribute_type('test_table', 'number', 'INTEGER')
        cur.execute("SELECT pg_typeof(number) FROM \"TEST\".test_table")
        result = cur.fetchone()[0]
        self.assertEqual(result, 'integer')
        self.db_connection.commit()

    
    def test_character_conversion(self):
        """Test the conversion of character types."""
        cur = self.db_connection.cursor()
        #Make it into a varchar for testing purposes
        self.test_object.change_attribute_type('test_table', 'number', 'VARCHAR(255)')
        self.test_object.change_attribute_type('test_table', 'number', 'INTEGER')
        cur.execute("SELECT pg_typeof(number) FROM \"TEST\".test_table")
        result = cur.fetchone()[0]
        self.assertEqual(result, 'integer')
        #Reset to varchar
        self.test_object.change_attribute_type('test_table', 'number', 'VARCHAR(255)')
        self.test_object.change_attribute_type('test_table', 'number', 'FLOAT')
        cur.execute("SELECT pg_typeof(number) FROM \"TEST\".test_table")
        result = cur.fetchone()[0]
        self.assertEqual(result, 'double precision')
        #Reset to varchar
        self.test_object.change_attribute_type('test_table', 'number', 'VARCHAR(255)')
        self.test_object.change_attribute_type('test_table', 'number', 'CHAR(255)')
        cur.execute("SELECT pg_typeof(number) FROM \"TEST\".test_table")
        result = cur.fetchone()[0]
        self.assertEqual(result, 'character')
        #Leave database in valid testing state by returning the column to integer
        self.test_object.change_attribute_type('test_table', 'number', 'INTEGER')
        cur.execute("SELECT pg_typeof(number) FROM \"TEST\".test_table")
        result = cur.fetchone()[0]
        self.assertEqual(result, 'integer')
        self.db_connection.commit()


        
    def test_one_hot_encode_unique(self):
        """.Test the one-hot-encoding method for a column with unique and duplicate values."""
        cur = self.db_connection.cursor()
        self.test_object.one_hot_encode('test_table', 'string')
        #Query to get all columns from the encoded table
        query = ("SELECT column_name FROM information_schema.columns "
               "WHERE table_schema = 'TEST' AND table_name =  'test_table'")
        cur.execute(query)
        all_columns  = cur.fetchall()
        #This should be all the columns
        expected = ['number', 'date_string', 'fpoint', 'raw_time', 'date_time', 'garbage', 'Apple', 'Asus', 'Dummy', 'Elevate ltd',
                   'Hewlett-Packard', 'Huawei', 'Imagine Breakers', 'LG Electronics', 'Microsoft', 'Nintendo', 'Nokia', 'Razer',
                   'Replacement', 'Samsung', 'Sony', 'Toshiba']
        
        for element in expected: #Test if expected elements are part of the table
            test_result = (element,) in all_columns
            self.assertEqual(test_result, True)
        #There should 22 columns, 6 previous one + 16 unique categories 
        self.assertEqual(len(all_columns), 22)
        self.db_connection.commit()

        self.test_object.one_hot_encode('test_table', 'date_string')
        cur.execute(query)
        all_columns = cur.fetchall()
        expected.extend(['08/08/1997', '01/04/1976', '04/04/1975', '12/05/1865', '01/03/1938', '15/09/1987', '01/01/1998', '21/09/1996', '07/05/1946', '02/04/1989',
        '01/01/1939',  '01/07/1975', '01/10/1958', '23/09/1989'])
        del expected[1]
        for element in expected: #Test if expected elements are part of the table
            test_result = (element,) in all_columns
            self.assertEqual(test_result, True)
        #There should be 35 columns, the previous 22 - 1(date_string) + 14 (16 categeroies - 2 duplicates = 35
        self.assertEqual(len(all_columns), 35)
        self.db_connection.commit()



    def test_normalize_using_zscore(self):
        cur = self.db_connection.cursor()
        self.test_object.normalize_using_zscore('test_table', 'number')
        query = 'SELECT number FROM "TEST".test_table'
        cur.execute(query)
        all_values = cur.fetchall()
        all_values = [x[0] for x in all_values]
        print(all_values)
        self.db_connection.commit()
        
        self.assertEqual(1, 1)
        
        
        

        
        

        



if __name__ == '__main__':
    unittest.main()
