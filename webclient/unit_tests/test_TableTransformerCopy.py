import unittest
from Model.DatabaseConfiguration import DatabaseConfiguration
import psycopg2
import Controller.TableTransformer as transformer

#This file contains tests for TableTransformer that specifically creates new tables when transforming. This tests data manipulation methods of TableTransformer
#For the tests on data manipulation methods of TableTransformer that don't copy but overwrite the tables refer to "test_TableTransformer.py"

class TestTransformerCopy(unittest.TestCase):
    db_connection = None
    engine = None
    test_object = None

    @classmethod
    def setUpClass(cls):
        connection_string = "dbname='{}' user='{}' host='{}' password='{}'".format(*(DatabaseConfiguration().get_packed_values()))
        cls.db_connection = psycopg2.connect(connection_string)
        cls.test_object = transformer.TableTransformer('TEST', cls.db_connection, None, False)
        cur = cls.db_connection.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS \"TEST\"")
        cls.db_connection.commit()
        creation_query = """CREATE TABLE "TEST".test_table (
        string VARCHAR(255) NOT NULL,
        number INTEGER NOT NULL,
        date_time VARCHAR(255) NOT NULL,
        garbage VARCHAR(255));"""
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


    def __test_table_exists(self, tablename):
        """Test whether a table exists in the TEST schema."""
        cur = self.db_connection.cursor()
        cur.execute('SELECT table_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s ', ['TEST', tablename])
        result = cur.fetchone()
        if result is None:
            return False
        else:
            return True
        

    def test_delete_attribute(self):
        """Test to see if TableTransformer can correctly delete an attribute resulting in a new table."""
        self.test_object.delete_attribute('test_table', 'garbage', 'new_table1') #Delete attribute "garbage"
        #Now the only attributes should be "string", "number", "date_time"
        cur = self.db_connection.cursor()
        result = self.__test_table_exists('new_table1')
        self.assertTrue(result)
        #Let's see if we can find the deleted attribute
        cur.execute("""SELECT column_name FROM information_schema.columns WHERE table_schema = 'TEST'
                        AND table_name   = 'new_table1'""")
        tablenames = cur.fetchall()
        found = False
        for name in tablenames:
            if name[0] == 'garbage':
                found = True

        #Assert if it is in fact not found amongst remaining tablenames
        self.assertEqual(found, False)
        self.db_connection.commit()


    def test_find_and_replace(self):
        """A test for the find and replace method resulting in a new table."""
        self.test_object.find_and_replace('test_table','string', 'C-Corp', 'Replacement', new_name='new_table2')
        result = self.__test_table_exists('new_table2')
        self.assertTrue(result)
        cur = self.db_connection.cursor()
        cur.execute("SELECT * FROM \"TEST\".new_table2 WHERE string = 'Replacement'")
        result = cur.fetchone()
        self.assertEqual(result[1], 1)
        self.assertEqual(result[2], '08/08/1997')
        self.db_connection.commit()


    def test_find_and_replace_string(self):
        """A test for find and replace method but for finding substrings resulting in a new table."""
        #Find a word with substring Sam and replace the whole word with Foobar
        self.test_object.find_and_replace('test_table', 'string', 'Sam', 'Foobar', False, True, 'new_table3')
        result = self.__test_table_exists('new_table3')
        self.assertTrue(result)
        cur = self.db_connection.cursor()
        cur.execute("SELECT * FROM \"TEST\".new_table3 WHERE string = 'Foobar'")
        result = cur.fetchone()
        self.assertEqual(result[1], 7)
        self.assertEqual(result[2], '01/03/1938')

        #Find a word with substring To and replace the substring only with Waka
        self.test_object.find_and_replace('test_table', 'string', 'To', 'Waka', False, False, 'new_table4')
        result = self.__test_table_exists('new_table1')
        self.assertTrue(result)
        cur = self.db_connection.cursor()
        cur.execute("SELECT * FROM \"TEST\".new_table4 WHERE string = 'Wakashiba'")
        result = cur.fetchone()
        #We found Toshiba but replaced To with Waka to get Wakashiba
        self.assertEqual(result[1], 8)
        self.assertEqual(result[2], '01/07/1975')



    def test_regex_find_and_replace(self):
        """A test for the method of TableTransformer that uses regular expressions. This will result in a new table."""
        #Use a regular expression to find Nintendo and replace it with SEGA
        self.test_object.regex_find_and_replace('test_table', 'string', 'Nin.*', 'SEGA', False, 'new_table5')
        result = self.__test_table_exists('new_table5')
        self.assertTrue(result)
        cur = self.db_connection.cursor()
        cur.execute("SELECT * FROM \"TEST\".new_table5 WHERE string = 'SEGA'")
        result = cur.fetchone()
        self.assertEqual(result[1], 21)
        self.assertEqual(result[2], '23/09/1989')

        #Use the regex to find a word without case sensitivity
        self.test_object.regex_find_and_replace('test_table', 'string', 'sega', 'SEGA', False, 'new_table6')
        result = self.__test_table_exists('new_table6')
        self.assertTrue(result)
        cur = self.db_connection.cursor()
        cur.execute("SELECT * FROM \"TEST\".new_table6 WHERE string = 'SEGA'")
        result = cur.fetchone()
        self.assertEqual(result[1], 21)
        self.assertEqual(result[2], '23/09/1989')

        #Use the regex to find a word with case sensitivity
        self.test_object.regex_find_and_replace('test_table4', 'string', 'sega', 'Ethereal', True, 'new_table7')
        result = self.__test_table_exists('new_table7')
        self.assertTrue(result)
        cur = self.db_connection.cursor()
        cur.execute("SELECT * FROM \"TEST\".test_table7 WHERE string = 'Ethereal'")
        result = cur.fetchone()
        self.assertIsNone(result) #Shouldn't be able to find out due the difference in case


    def test_numeric_conversion(self):
        """Test the conversion of numeric types (INTEGER, FLOAT)."""
        #From integer to float
        self.test_object.change_attribute_type('test_table', 'number', 'FLOAT', new_name='new_table8')
        result = self.__test_table_exists('new_table8')
        self.assertTrue(result)
        cur = self.db_connection.cursor()
        cur.execute("SELECT pg_typeof(number) FROM \"TEST\".new_table8")
        result = cur.fetchone()[0]
        self.assertEqual(result, 'double precision')
        #From float to integer
        self.test_object.change_attribute_type('new_table8', 'number', 'INTEGER', new_name='new_table9')
        result = self.__test_table_exists('new_table9')
        self.assertTrue(result)
        cur.execute("SELECT pg_typeof(number) FROM \"TEST\".new_table9")
        result = cur.fetchone()[0]
        self.assertEqual(result, 'integer')
        #From integer to VARCHAR(255)
        self.test_object.change_attribute_type('test_table', 'number', 'VARCHAR(255)', new_name='new_table10')
        result = self.__test_table_exists('new_table10')
        self.assertTrue(result)
        cur.execute("SELECT pg_typeof(number) FROM \"TEST\".new_table10")
        result = cur.fetchone()[0]
        self.assertEqual(result, 'character varying')


    
    
