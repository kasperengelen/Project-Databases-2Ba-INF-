import unittest
from Model.DatabaseConfiguration import TestConnection
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
        cls.db_connection = TestConnection().get_db()
        cls.engine = TestConnection().get_engine()
        cls.test_object = transformer.TableTransformer('TEST', cls.db_connection, cls.engine, False, False)
        cur = cls.db_connection.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS \"TEST\"")
        cls.db_connection.commit()
        creation_query = """CREATE TABLE "TEST".test_table (
        string VARCHAR(255) NOT NULL,
        number INTEGER NOT NULL,
        date_time VARCHAR(255) NOT NULL,
        garbage VARCHAR(255));"""
        creation_query1 = 'CREATE TABLE "TEST".test_table1 AS TABLE "TEST".test_table'
        creation_query2 = 'CREATE TABLE "TEST".test_table2 AS TABLE "TEST".test_table'
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

        cur.execute(creation_query1)
        cur.execute(creation_query2)
        cur.execute('UPDATE "TEST".test_table1 SET number = null  WHERE number > 40')

        
        cls.db_connection.commit()

    @classmethod
    def tearDownClass(cls):
        cls.db_connection.cursor().execute("DROP SCHEMA \"TEST\" CASCADE")
        cls.db_connection.commit()
        #Close database connection
        cls.db_connection.close()


    def __test_table_exists(self, tablename):
        """Test whether a table exists in the TEST schema after performing a operation that should create new table in the schema."""
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
        self.test_object.regex_find_and_replace('new_table5', 'string', 'sega', 'SEGA', False, 'new_table6')
        result = self.__test_table_exists('new_table6')
        self.assertTrue(result)
        cur = self.db_connection.cursor()
        cur.execute("SELECT * FROM \"TEST\".new_table6 WHERE string = 'SEGA'")
        result = cur.fetchone()
        self.assertEqual(result[1], 21)
        self.assertEqual(result[2], '23/09/1989')

        #Use the regex to find a word with case sensitivity
        self.test_object.regex_find_and_replace('new_table6', 'string', 'sega', 'Ethereal', True, 'new_table7')
        result = self.__test_table_exists('new_table7')
        self.assertTrue(result)
        cur = self.db_connection.cursor()
        cur.execute("SELECT * FROM \"TEST\".new_table7 WHERE string = 'Ethereal'")
        result = cur.fetchone()
        self.assertIsNone(result) #Shouldn't be able to find out due the difference in case"""


    def test_numeric_conversion(self):
        """Test the conversion of numeric types (INTEGER, FLOAT). This will result in a new table."""
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


    def test_character_conversion(self):
        """Test the conversion of character types. This will result in a new table."""
        cur = self.db_connection.cursor()
        #Make it into a varchar for testing purposes
        self.test_object.change_attribute_type('test_table', 'number', 'VARCHAR(255)', new_name='new_table11')
        result = self.__test_table_exists('new_table11')
        self.assertTrue(result)
        cur.execute("SELECT pg_typeof(number) FROM \"TEST\".new_table11")
        result = cur.fetchone()[0]
        self.assertEqual(result,'character varying')

        self.test_object.change_attribute_type('new_table11', 'number', 'FLOAT', new_name='new_table12')
        result = self.__test_table_exists('new_table12')
        self.assertTrue(result)
        cur.execute("SELECT pg_typeof(number) FROM \"TEST\".new_table12")
        result = cur.fetchone()[0]
        self.assertEqual(result, 'double precision')
        
        #Change to varchar(30)
        self.test_object.change_attribute_type('new_table12', 'number', 'VARCHAR(n)', "", '30', new_name='new_table13')
        result = self.__test_table_exists('new_table13')
        self.assertTrue(result)
        cur.execute("SELECT pg_typeof(number) FROM \"TEST\".new_table13")
        result = cur.fetchone()[0]
        self.assertEqual(result, 'character varying')


    def test_datetime_conversion(self):
        """Test the conversion of an attribute to  a date/time type. This will result in a new table."""
        cur = self.db_connection.cursor()
        #Convert date_string column to actual DATE type
        self.test_object.change_attribute_type('test_table', 'date_time', 'DATE', 'DD/MM/YYYY', new_name='new_table14')
        result = self.__test_table_exists('new_table14')
        self.assertTrue(result)
        cur.execute('SELECT pg_typeof(date_time) FROM "TEST".new_table14')
        result = cur.fetchone()[0]
        self.assertEqual(result, 'date')
        self.db_connection.commit()
        #Convert the same column to a timestamp
        self.test_object.change_attribute_type('test_table', 'date_time', 'TIMESTAMP', 'DD/MM/YYYY TIME', new_name='new_table15')
        result = self.__test_table_exists('new_table15')
        self.assertTrue(result)
        cur.execute('SELECT pg_typeof(date_time) FROM "TEST".new_table15')
        result = cur.fetchone()[0]
        self.assertEqual(result, 'timestamp without time zone')
        self.db_connection.commit()
        #Set date_string of another to a time string and try to convert it
        query_1 = 'UPDATE "TEST".test_table SET garbage = \'08:42 PM\' WHERE garbage is NULL'
        cur.execute(query_1)
        self.test_object.change_attribute_type('test_table', 'garbage', 'TIME', 'HH12:MI AM/PM', new_name='new_table16')
        result = self.__test_table_exists('new_table16')
        self.assertTrue(result)
        cur.execute('SELECT pg_typeof(garbage) FROM "TEST".new_table16')
        result = cur.fetchone()[0]
        self.assertEqual(result, 'time without time zone')
        self.db_connection.commit()


    def test_one_hot_encode(self):
        """Test the one-hot-encoding method for a column with unique and duplicate values. This will result in a new table."""
        cur = self.db_connection.cursor()
        self.test_object.one_hot_encode('test_table', 'string', 'new_table17')
        result = self.__test_table_exists('new_table17')
        self.assertTrue(result)
        #Query to get all columns from the encoded table
        query = ("SELECT column_name FROM information_schema.columns "
               "WHERE table_schema = 'TEST' AND table_name =  'new_table17'")
        cur.execute(query)
        all_columns  = cur.fetchall()
        #This should be all the columns
        expected = ['number', 'date_time', 'garbage', 'Apple', 'Asus', 'Dummy', 'Elevate ltd',
                   'Hewlett-Packard', 'Huawei', 'Imagine Breakers', 'LG Electronics', 'Microsoft', 'Nintendo', 'Nokia', 'Razer',
                   'C-Corp', 'Samsung', 'Sony', 'Toshiba']
        
        for element in expected: #Test if expected elements are part of the table
            test_result = (element,) in all_columns
            self.assertTrue(test_result)
        #There should 22 columns, 3 previous one + 16 unique categories 
        self.assertEqual(len(all_columns), 19)
        self.db_connection.commit()


    def test_equidistant_discretization(self):
        """Test the equidistant discretization method. This will result in a new table."""
        cur = self.db_connection.cursor()
        self.test_object.discretize_using_equal_width('test_table', 'number', 4, 'new_table18')
        result = self.__test_table_exists('new_table18')
        self.assertTrue(result)
        cur.execute('SELECT DISTINCT number_categorical FROM "TEST".new_table18')
        self.db_connection.commit()
        all_values = cur.fetchall()
        all_values = [x[0] for x in all_values]
        #There should be 3 buckets.
        self.assertEqual(len(all_values), 4)
        all_values = sorted(all_values)
        expected_values = ['[-17 , -2[', '[-2 , 13[', '[13 , 28[', '[28 , 43[']
        self.assertEqual(all_values, expected_values)
        #Let's check if the values are actually being put in the correct buckets
        cur.execute('SELECT * FROM "TEST".new_table18 WHERE number < -2 AND number > -17 '
                    'AND number_categorical <> \'[-17 , -2[\'')
        result = cur.fetchone()
        self.assertIsNone(result)
        cur.execute('SELECT * FROM "TEST".new_table18 WHERE number < 13 AND number > -2 '
                    'AND number_categorical <> \'[-2 , 13[\'')
        result = cur.fetchone()
        self.assertIsNone(result)
        cur.execute('SELECT * FROM "TEST".new_table18 WHERE number < 43 AND number > 28 '
                    'AND number_categorical <> \'[28 , 43[\'')
        result = cur.fetchone()
        self.assertIsNone(result)
        self.db_connection.commit()


    def test_equifrequent_discretization(self):
        """Test the equifrequent discretization method. This will result in a new table."""
        cur = self.db_connection.cursor()
        self.test_object.discretize_using_equal_frequency('test_table', 'number', 'new_table19')
        result = self.__test_table_exists('new_table19')
        self.assertTrue(result)
        cur.execute('SELECT DISTINCT number_categorical FROM "TEST".new_table19')
        self.db_connection.commit()
        all_values = cur.fetchall()
        all_values = [x[0] for x in all_values]
        all_values = sorted(all_values)
        expected_values = ['[-17 , 4[', '[15 , 42[', '[4 , 9[', '[9 , 15[']
        #There should be 4 buckets
        self.assertEqual(len(all_values), 4)
        self.assertEqual(all_values, expected_values)
        #Let's check if the values are actually being put in the correct buckets
        cur.execute('SELECT * FROM "TEST".new_table19 WHERE number < -4 AND number > -17 '
                    'AND number_categorical <> \'[-17 , -4[\'')
        result = cur.fetchone()
        self.assertIsNone(result)
        cur.execute('SELECT * FROM "TEST".new_table19 WHERE number < 9 AND number > 4 '
                    'AND number_categorical <> \'[4 , 9[\'')
        result = cur.fetchone()
        self.assertIsNone(result)
        cur.execute('SELECT * FROM "TEST".new_table19 WHERE number < 42 AND number > 15 '
                    'AND number_categorical <> \'[15 , 42[\'')
        result = cur.fetchone()
        self.assertIsNone(result)
        self.db_connection.commit()


    def test_discretization_with_custom_ranges(self):
        """Test the discretization with custom ranges method. This will result in a new table."""
        #Let's simulate equidistant discretization with our custom bins.
        cur = self.db_connection.cursor()
        ranges = [-17, -2, 13, 28, 43]
        #self.test_object.set_to_overwrite()
        self.test_object.discretize_using_custom_ranges('test_table', 'number', ranges, True, 'new_table20')
        result = self.__test_table_exists('new_table20')
        self.assertTrue(result)
        cur.execute('SELECT DISTINCT number_categorical FROM "TEST".new_table20')
        self.db_connection.commit()
        all_values = cur.fetchall()
        all_values = [x[0] for x in all_values]
        all_values = sorted(all_values)
        expected_values = ['[-17 , -2[', '[-2 , 13[', '[13 , 28[', '[28 , 43[']
        #There should be 4 buckets
        self.assertEqual(len(all_values), 4)
        self.assertEqual(all_values, expected_values)
        #Let's check if the values are actually being put in the correct buckets
        cur.execute('SELECT * FROM "TEST".new_table20 WHERE number < -2 AND number > -17 '
                    'AND number_categorical <> \'[-17 , -2[\'')
        result = cur.fetchone()
        self.assertIsNone(result)
        cur.execute('SELECT * FROM "TEST".new_table20 WHERE number < 43 AND number > 28 '
                    'AND number_categorical <> \'[28 , 43[\'')
        result = cur.fetchone()
        self.assertIsNone(result)
        self.db_connection.commit()

        #Let's simulate equifrequent discretization with our custom bins.
        ranges = [-17, 4, 9, 15, 42]
        self.test_object.discretize_using_custom_ranges('test_table', 'number', ranges, True, 'new_table21')
        result = self.__test_table_exists('new_table21')
        self.assertTrue(result)
        cur.execute('SELECT DISTINCT number_categorical FROM "TEST".new_table21')
        self.db_connection.commit()
        all_values = cur.fetchall()
        all_values = [x[0] for x in all_values]
        all_values = sorted(all_values)
        expected_values = ['[-17 , 4[', '[15 , 42[', '[4 , 9[', '[9 , 15[']
        #There should be 4 buckets
        self.assertEqual(len(all_values), 4)
        self.assertEqual(all_values, expected_values)
        cur.execute('SELECT * FROM "TEST".new_table21 WHERE number < -4 AND number > -17 '
                    'AND number_categorical <> \'[-17 , -4[\'')
        result = cur.fetchone()
        self.assertIsNone(result)
        cur.execute('SELECT * FROM "TEST".new_table21 WHERE number < 42 AND number > 15 '
                    'AND number_categorical <> \'[15 , 42[\'')
        result = cur.fetchone()
        self.assertIsNone(result)
        self.db_connection.commit()



    def test_delete_outliers(self):
        """Test the method of TableTransformer to delete outliers. This will result in a new table."""
        #Test outliers larger than presented value
        cur = self.db_connection.cursor()
        self.test_object.delete_outlier('test_table', 'number', True, 40, 'new_table22')
        result = self.__test_table_exists('new_table22')
        self.assertTrue(result)
        cur.execute('SELECT * FROM "TEST".new_table22 WHERE number > 40')
        result = cur.fetchone()
        self.assertIsNone(result)

        self.test_object.delete_outlier('test_table', 'number', True, 20, 'new_table23')
        result = self.__test_table_exists('new_table23')
        self.assertTrue(result)
        cur.execute('SELECT * FROM "TEST".new_table23 WHERE number > 20')
        result = cur.fetchone()
        self.assertIsNone(result)

        #Test outliers smaller than presented value
        self.test_object.delete_outlier('test_table', 'number', False, -15, 'new_table24')
        result = self.__test_table_exists('new_table24')
        self.assertTrue(result)
        cur.execute('SELECT * FROM "TEST".new_table24 WHERE number < -15')
        result = cur.fetchone()
        self.assertIsNone(result)

        self.test_object.delete_outlier('test_table', 'number', False, 0, 'new_table25')
        result = self.__test_table_exists('new_table25')
        self.assertTrue(result)
        cur.execute('SELECT * FROM "TEST".new_table25 WHERE number < 0')
        result = cur.fetchone()
        self.assertIsNone(result)
        self.db_connection.commit()


    def test_fill_nulls_with_mean(self):
        """Test the method of TableTransformer that fills null values with the mean. This will result in a new table."""
        cur = self.db_connection.cursor()
        self.test_object.fill_nulls_with_mean('test_table1', 'number', 'new_table26')
        result = self.__test_table_exists('new_table26')
        self.assertTrue(result)
        #Test if it's really set to null
        cur.execute('SELECT * FROM "TEST".new_table26 WHERE number > 40')
        result = cur.fetchone()
        self.assertIsNone(result)
        #Test whether any nulls are left open
        cur.execute('SELECT * FROM "TEST".new_table26 WHERE number is null')
        result = cur.fetchone()
        self.assertIsNone(result)
        #The mean by excluding values > 40 is 10 (cast to int), let's check if the value is here
        cur.execute('SELECT * FROM "TEST".new_table26 WHERE number = 10 AND string = \'Elevate ltd\'')
        result = cur.fetchall()
        self.assertIsNotNone(result)
        self.assertEqual(1,1)


    def test_fill_nulls_with_median(self):
        """Test the method of TableTransformer that fills null values with the median. This will result in a new table."""
        cur = self.db_connection.cursor()
        self.test_object.fill_nulls_with_median('test_table1', 'number', 'new_table27')
        result = self.__test_table_exists('new_table27')
        self.assertTrue(result)
        #Test if it's really set to null
        cur.execute('SELECT * FROM "TEST".new_table27 WHERE number > 40')
        result = cur.fetchone()
        self.assertIsNone(result)
        #Test whether any nulls are left open
        cur.execute('SELECT * FROM "TEST".new_table27 WHERE number is null')
        result = cur.fetchone()
        self.assertIsNone(result)
        #The median by excluding values > 40 is 9, let's check if the value is here
        cur.execute('SELECT * FROM "TEST".new_table27 WHERE number = 9 AND string = \'Elevate ltd\'')
        result = cur.fetchall()
        self.assertIsNotNone(result)


    def test_fill_nulls_with_custom_value(self):
        """Test the method of TableTransformer that fills null values with a custom value."""
        cur = self.db_connection.cursor()
        self.test_object.fill_nulls_with_custom_value('test_table1', 'number', 10000, 'new_table28')
        result = self.__test_table_exists('new_table28')
        self.assertTrue(result)
        #The value we used should correspond to the row with string = 'Dummy'
        cur.execute('SELECT * FROM "TEST".new_table28 WHERE number = 10000 AND string = \'Elevate ltd\'')
        result = cur.fetchall()
        self.assertIsNotNone(result)


    def incomplete_test_delete_rows_using_conditions(self):
        """Test method of TableTransformer deletes rows by using provided predicates. This will result in a new table."""
        cur = self.db_connection.cursor()
        predicate1 = ['string', '=', 'C-Corp']
        self.test_object.delete_rows_using_predicate_logic('test_table', predicate1, 'new_table29')
        result = self.__test_table_exists('new_table29')
        self.assertTrue(result)
        query = "SELECT * FROM \"TEST\".new_table29 WHERE string = 'C-Corp'"
        cur.execute(query)
        result = cur.fetchone()
        self.assertIsNone(result)

        predicate2 = ['string', '=', 'Nokia', 'AND', 'number', '=', '18', 'AND', 'date_time', '!=', '01/01/2001']
        self.test_object.delete_rows_using_predicate_logic('test_table', predicate2, 'new_table30')
        result = self.__test_table_exists('new_table30')
        self.assertTrue(result)
        query = "SELECT * FROM \"TEST\".new_table30 WHERE string = 'Nokia' AND number = 18"
        cur.execute(query)
        result = cur.fetchone()
        self.assertIsNone(result)



    def test_datetime_extraction(self):
        """This one is for testing the extraction of parts of the date/time done by TableTransformer. This will result in a new table."""
        cur = self.db_connection.cursor()
        cur.execute('ALTER TABLE "TEST".test_table2 ALTER COLUMN date_time TYPE DATE USING to_date(date_time , \'DD/MM/YYYY\')')
        self.test_object.extract_part_of_date('test_table2', 'date_time', 'MONTH', 'new_table31')
        result = self.__test_table_exists('new_table31')
        self.assertTrue(result)
        #Get all the column names for the table
        query = ("SELECT column_name FROM information_schema.columns "
               "WHERE table_schema = 'TEST' AND table_name =  'new_table31'")
        cur.execute(query)
        all_columns = cur.fetchall()
        result = False
        
        for elem in all_columns:
            if elem[0] == 'date_time_part':
                result = True

        self.assertTrue(result)

        query = "SELECT * FROM \"TEST\".new_table31 WHERE number = 21 AND date_time_part = 'September'"
        cur.execute(query)
        result = cur.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 'Nintendo')




    
    
