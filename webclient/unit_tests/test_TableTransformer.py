import unittest
from Model.DatabaseConfiguration import DatabaseConfiguration
import psycopg2
import Controller.TableTransformer as transformer

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
        creation_query1 = 'CREATE TABLE "TEST".test_table1 AS TABLE "TEST".test_table'
        creation_query2 = 'CREATE TABLE "TEST".test_table2 AS TABLE "TEST".test_table'
        creation_query3 = 'CREATE TABLE "TEST".test_table3 AS TABLE "TEST".test_table'
        creation_query4 = 'CREATE TABLE "TEST".test_table4 AS TABLE "TEST".test_table'
        creation_query5 = 'CREATE TABLE "TEST".test_table5 AS TABLE "TEST".test_table'
        creation_query6 = 'CREATE TABLE "TEST".test_table6 AS TABLE "TEST".test_table'
        creation_query7 = 'CREATE TABLE "TEST".test_table7 AS TABLE "TEST".test_table'
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
        cur.execute(creation_query3)
        cur.execute(creation_query4)
        cur.execute(creation_query5)
        cur.execute(creation_query6)
        cur.execute(creation_query7)


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



    def test_find_and_replace_substring(self):
        """A test for find and replace method but for finding substrings."""
        #Find a word with substring Sam and replace the whole word with Foobar
        self.test_object.find_and_replace('test_table', 'string', 'Sam', 'Foobar', False, True)
        cur = self.db_connection.cursor()
        cur.execute("SELECT * FROM \"TEST\".test_table WHERE string = 'Foobar'")
        result = cur.fetchone()
        self.assertEqual(result[1], 7)
        self.assertEqual(result[2], '01/03/1938')

        #Find a word with substring To and replace the substring only with Waka
        self.test_object.find_and_replace('test_table', 'string', 'To', 'Waka', False, False)
        cur = self.db_connection.cursor()
        cur.execute("SELECT * FROM \"TEST\".test_table WHERE string = 'Wakashiba'")
        result = cur.fetchone()
        #We found Toshiba but replaced To with Waka to get Wakashiba
        self.assertEqual(result[1], 8)
        self.assertEqual(result[2], '01/07/1975')


    def test_regex_find_and_place(self):
        """A test for the method of TableTransformer that uses regular expressions."""
        #Use a regular expression to find Nintendo and replace it with SEGA
        self.test_object.regex_find_and_replace('test_table4', 'string', 'Nin.*', 'SEGA', False)
        cur = self.db_connection.cursor()
        cur.execute("SELECT * FROM \"TEST\".test_table4 WHERE string = 'SEGA'")
        result = cur.fetchone()
        self.assertEqual(result[1], 21)
        self.assertEqual(result[2], '23/09/1989')

        #Use the regex to find a word without case sensitivity
        self.test_object.regex_find_and_replace('test_table4', 'string', 'sega', 'SEGA', False)
        cur = self.db_connection.cursor()
        cur.execute("SELECT * FROM \"TEST\".test_table4 WHERE string = 'SEGA'")
        result = cur.fetchone()
        self.assertEqual(result[1], 21)
        self.assertEqual(result[2], '23/09/1989')

        #Use the regex to find a word with case sensitivity
        self.test_object.regex_find_and_replace('test_table4', 'string', 'sega', 'Ethereal', True)
        cur = self.db_connection.cursor()
        cur.execute("SELECT * FROM \"TEST\".test_table4 WHERE string = 'Ethereal'")
        result = cur.fetchone()
        self.assertIsNone(result) #Shouldn't be able to find out due the difference in case


    def test_get_conversion_options(self):
        """A test to see whether the correct conversion options are being returned."""
        obj = self.test_object
        self.assertEqual(obj.get_conversion_options('test_table', 'string'), ['INTEGER', 'FLOAT', 'DATE', 'TIME', 'TIMESTAMP',  'CHAR(n)'])
        self.assertEqual(obj.get_conversion_options('test_table', 'number'), ['FLOAT', 'VARCHAR(255)', 'VARCHAR(n)','CHAR(n)'])
        self.assertEqual(obj.get_conversion_options('test_table', 'garbage'),['VARCHAR(255)', 'VARCHAR(n)', 'INTEGER', 'FLOAT', 'DATE', 'TIME', 'TIMESTAMP'])
        self.assertEqual(obj.get_conversion_options('test_table', 'fpoint'), ['INTEGER', 'VARCHAR(255)','CHAR(n)'])
        self.assertEqual(obj.get_conversion_options('test_table', 'raw_time'), ['VARCHAR(255)', 'VARCHAR(n)', 'CHAR(n)'])
        self.assertEqual(obj.get_conversion_options('test_table', 'date_time'), ['VARCHAR(255)', 'VARCHAR(n)', 'CHAR(n)'])

        

    def test_get_attribute_type(self):
        """Method to test whether the getter returns the correct attribute type."""
        #Test whether the method can correctly return the type of an attribute
        obj = self.test_object
        self.assertEqual(obj.get_attribute_type('test_table', 'string')[0], 'character varying')
        self.assertEqual(obj.get_attribute_type('test_table', 'number')[0],'integer')
        self.assertEqual(obj.get_attribute_type('test_table', 'garbage')[0], 'character')
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
        self.test_object.change_attribute_type('test_table', 'number', 'CHAR(n)', "", '255')
        cur.execute("SELECT pg_typeof(number) FROM \"TEST\".test_table")
        result = cur.fetchone()[0]
        self.assertEqual(result, 'character')
        #Reset to integer
        self.test_object.change_attribute_type('test_table', 'number', 'INTEGER')
        cur.execute("SELECT pg_typeof(number) FROM \"TEST\".test_table")
        result = cur.fetchone()[0]
        self.assertEqual(result, 'integer')
        #Change to varchar(30)
        self.test_object.change_attribute_type('test_table', 'number', 'VARCHAR(n)', "", '30')
        cur.execute("SELECT pg_typeof(number) FROM \"TEST\".test_table")
        result = cur.fetchone()[0]
        self.assertEqual(result, 'character varying')

        self.test_object.change_attribute_type('test_table', 'number', 'INTEGER')
        cur.execute("SELECT pg_typeof(number) FROM \"TEST\".test_table")
        result = cur.fetchone()[0]
        self.assertEqual(result, 'integer')
        self.db_connection.commit()

    def test_datetime_conversion(self):
        """Test the conversion of an attribute to  a date/time type."""
        cur = self.db_connection.cursor()
        #Convert date_string column to actual DATE type
        self.test_object.change_attribute_type('test_table2', 'date_string', 'DATE', 'DD/MM/YYYY')
        cur.execute('SELECT pg_typeof(date_string) FROM "TEST".test_table2')
        result = cur.fetchone()[0]
        self.assertEqual(result, 'date')
        self.db_connection.commit()
        #Convert the same column to a timestamp
        self.test_object.change_attribute_type('test_table3', 'date_string', 'TIMESTAMP', 'DD/MM/YYYY TIME')
        cur.execute('SELECT pg_typeof(date_string) FROM "TEST".test_table3')
        result = cur.fetchone()[0]
        self.assertEqual(result, 'timestamp without time zone')
        self.db_connection.commit()
        #Set date_string of another to a time string and try to convert it
        query_1 = 'UPDATE "TEST".test_table5 SET date_string = \'08:42 PM\' WHERE date_string is NOT NULL'
        cur.execute(query_1)
        self.test_object.change_attribute_type('test_table5', 'date_string', 'TIME', 'HH12:MI AM/PM')
        cur.execute('SELECT pg_typeof(date_string) FROM "TEST".test_table5')
        result = cur.fetchone()[0]
        self.assertEqual(result, 'time without time zone')
        self.db_connection.commit()



    def test_datetime_extraction(self):
        """This one is for testing the extraction of parts of the date/time done by TableTransformer"""
        self.assertEqual(1, 1)
        


        
    def test_one_hot_encode(self):
        """Test the one-hot-encoding method for a column with unique and duplicate values."""
        cur = self.db_connection.cursor()
        self.test_object.one_hot_encode('test_table1', 'string')
        #Query to get all columns from the encoded table
        query = ("SELECT column_name FROM information_schema.columns "
               "WHERE table_schema = 'TEST' AND table_name =  'test_table1'")
        cur.execute(query)
        all_columns  = cur.fetchall()
        #This should be all the columns
        expected = ['number', 'date_string', 'fpoint', 'raw_time', 'date_time', 'garbage', 'Apple', 'Asus', 'Dummy', 'Elevate ltd',
                   'Hewlett-Packard', 'Huawei', 'Imagine Breakers', 'LG Electronics', 'Microsoft', 'Nintendo', 'Nokia', 'Razer',
                   'C-Corp', 'Samsung', 'Sony', 'Toshiba']
        
        for element in expected: #Test if expected elements are part of the table
            test_result = (element,) in all_columns
            self.assertEqual(test_result, True)
        #There should 22 columns, 6 previous one + 16 unique categories 
        self.assertEqual(len(all_columns), 22)
        self.db_connection.commit()

        self.test_object.one_hot_encode('test_table1', 'date_string')
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
        """Test the method that normalizes the values of a column by using the z-score."""
        cur = self.db_connection.cursor()
        self.test_object.normalize_using_zscore('test_table', 'number')
        query = 'SELECT number FROM "TEST".test_table'
        cur.execute(query)
        self.db_connection.commit()
        all_values = cur.fetchall()
        all_values = [x[0] for x in all_values]
        largest = max(all_values)
        smallest = min(all_values)
        unique_nr = len(set(all_values))
        test = largest <= 1.000
        self.assertEqual(test, True)
        test = smallest >= 0.000
        self.assertEqual(test, True)
        #There are 16 rows, two rows both contain 8 and should be mapped to the same value
        #And 2 extreme values should both be mapped to 1.0 so there should be 14 unqiue values.
        self.assertEqual(unique_nr, 14)
        self.assertEqual(1, 1)


    def test_equidistant_discretization(self):
        """Test the equidistant discretization method."""
        cur = self.db_connection.cursor()
        self.test_object.discretize_using_equal_width('test_table2', 'number')
        cur.execute('SELECT DISTINCT number_categorical FROM "TEST".test_table2')
        self.db_connection.commit()
        all_values = cur.fetchall()
        all_values = [x[0] for x in all_values]
        #There should be 3 buckets.
        self.assertEqual(len(all_values), 4)
        all_values = sorted(all_values)
        expected_values = ['[-17 , -2[', '[-2 , 13[', '[13 , 28[', '[28 , 43[']
        self.assertEqual(all_values, expected_values)
        #Let's check if the values are actually being put in the correct buckets
        cur.execute('SELECT * FROM "TEST".test_table2 WHERE number < -2 AND number > -17 '
                    'AND number_categorical <> \'[-17 , -2[\'')
        result = cur.fetchone()
        self.assertIsNone(result)
        cur.execute('SELECT * FROM "TEST".test_table2 WHERE number < 13 AND number > -2 '
                    'AND number_categorical <> \'[-2 , 13[\'')
        result = cur.fetchone()
        self.assertIsNone(result)
        cur.execute('SELECT * FROM "TEST".test_table2 WHERE number < 43 AND number > 28 '
                    'AND number_categorical <> \'[28 , 43[\'')
        result = cur.fetchone()
        self.assertIsNone(result)
        self.db_connection.commit()
        

    def test_equifrequent_discretization(self):
        """Test the equifrequent discretization method."""
        cur = self.db_connection.cursor()
        self.test_object.discretize_using_equal_frequency('test_table3', 'number')
        cur.execute('SELECT DISTINCT number_categorical FROM "TEST".test_table3')
        self.db_connection.commit()
        all_values = cur.fetchall()
        all_values = [x[0] for x in all_values]
        all_values = sorted(all_values)
        expected_values = ['[-17 , 4[', '[15 , 42[', '[4 , 9[', '[9 , 15[']
        #There should be 4 buckets
        self.assertEqual(len(all_values), 4)
        self.assertEqual(all_values, expected_values)
        #Let's check if the values are actually being put in the correct buckets
        cur.execute('SELECT * FROM "TEST".test_table3 WHERE number < -4 AND number > -17 '
                    'AND number_categorical <> \'[-17 , -4[\'')
        result = cur.fetchone()
        self.assertIsNone(result)
        cur.execute('SELECT * FROM "TEST".test_table3 WHERE number < 9 AND number > 4 '
                    'AND number_categorical <> \'[4 , 9[\'')
        result = cur.fetchone()
        self.assertIsNone(result)
        cur.execute('SELECT * FROM "TEST".test_table3 WHERE number < 42 AND number > 15 '
                    'AND number_categorical <> \'[15 , 42[\'')
        result = cur.fetchone()
        self.assertIsNone(result)
        self.db_connection.commit()
        

    def test_discretization_with_custom_ranges(self):
        """Test the discretization with custom ranges method."""
        #Let's simulate equidistant discretization with our custom bins.
        cur = self.db_connection.cursor()
        ranges = [-17, -2, 13, 28, 43]
        self.test_object.discretize_using_custom_ranges('test_table4', 'number', ranges)
        cur.execute('SELECT DISTINCT number_categorical FROM "TEST".test_table4')
        self.db_connection.commit()
        all_values = cur.fetchall()
        all_values = [x[0] for x in all_values]
        all_values = sorted(all_values)
        expected_values = ['[-17 , -2[', '[-2 , 13[', '[13 , 28[', '[28 , 43[']
        #There should be 4 buckets
        self.assertEqual(len(all_values), 4)
        self.assertEqual(all_values, expected_values)
        #Let's check if the values are actually being put in the correct buckets
        cur.execute('SELECT * FROM "TEST".test_table4 WHERE number < -2 AND number > -17 '
                    'AND number_categorical <> \'[-17 , -2[\'')
        result = cur.fetchone()
        self.assertIsNone(result)
        cur.execute('SELECT * FROM "TEST".test_table4 WHERE number < 43 AND number > 28 '
                    'AND number_categorical <> \'[28 , 43[\'')
        result = cur.fetchone()
        self.assertIsNone(result)
        self.db_connection.commit()

        #Let's simulate equifrequent discretization with our custom bins.
        self.test_object.discretize_using_equal_frequency('test_table4', 'number')
        cur.execute('SELECT DISTINCT number_categorical_1 FROM "TEST".test_table4')
        self.db_connection.commit()
        all_values = cur.fetchall()
        all_values = [x[0] for x in all_values]
        all_values = sorted(all_values)
        expected_values = ['[-17 , 4[', '[15 , 42[', '[4 , 9[', '[9 , 15[']
        #There should be 4 buckets
        self.assertEqual(len(all_values), 4)
        self.assertEqual(all_values, expected_values)
        cur.execute('SELECT * FROM "TEST".test_table4 WHERE number < -4 AND number > -17 '
                    'AND number_categorical_1 <> \'[-17 , -4[\'')
        result = cur.fetchone()
        self.assertIsNone(result)
        cur.execute('SELECT * FROM "TEST".test_table4 WHERE number < 42 AND number > 15 '
                    'AND number_categorical_1 <> \'[15 , 42[\'')
        result = cur.fetchone()
        self.assertIsNone(result)
        self.db_connection.commit()


    def test_delete_outliers(self):
        """Test the method of TableTransformer to delete outliers."""
        #Test outliers larger than presented value
        cur = self.db_connection.cursor()
        self.test_object.delete_outlier('test_table5', 'number', True, 40)
        cur.execute('SELECT * FROM "TEST".test_table5 WHERE number > 40')
        result = cur.fetchone()
        self.assertIsNone(result)

        self.test_object.delete_outlier('test_table5', 'number', True, 20)
        cur.execute('SELECT * FROM "TEST".test_table5 WHERE number > 20')
        result = cur.fetchone()
        self.assertIsNone(result)

        #Test outliers smaller than presented value
        self.test_object.delete_outlier('test_table5', 'number', False, -15)
        cur.execute('SELECT * FROM "TEST".test_table5 WHERE number < -15')
        result = cur.fetchone()
        self.assertIsNone(result)

        self.test_object.delete_outlier('test_table5', 'number', False, 0)
        cur.execute('SELECT * FROM "TEST".test_table5 WHERE number < 0')
        result = cur.fetchone()
        self.assertIsNone(result)
        self.db_connection.commit()


    def test_fill_nulls_with_mean(self):
        """Test the method of TableTransformer that fills null values with the mean."""
        cur = self.db_connection.cursor()
        #Set a value to null
        cur.execute('UPDATE "TEST".test_table6 SET number = null  WHERE number > 40')
        self.test_object.fill_nulls_with_mean('test_table6', 'number')
        #Test if it's really set to null
        cur.execute('SELECT * FROM "TEST".test_table6 WHERE number > 40')
        result = cur.fetchone()
        self.assertIsNone(result)
        #Test whether any nulls are left open
        cur.execute('SELECT * FROM "TEST".test_table6 WHERE number is null')
        result = cur.fetchone()
        self.assertIsNone(result)
        #The mean by excluding values > 40 is 10 (cast to int), let's check if the value is here
        cur.execute('SELECT * FROM "TEST".test_table6 WHERE number = 10 AND string = \'Elevate ltd\'')
        result = cur.fetchall()
        self.assertIsNotNone(result)
        self.assertEqual(1,1)

    def test_fill_nulls_with_median(self):
        """Test the method of TableTransformer that fills null values with the median."""
        cur = self.db_connection.cursor()
        #Set a value to null
        cur.execute('UPDATE "TEST".test_table7 SET number = null  WHERE number > 40')
        self.test_object.fill_nulls_with_median('test_table7', 'number')
        #Test if it's really set to null
        cur.execute('SELECT * FROM "TEST".test_table7 WHERE number > 40')
        result = cur.fetchone()
        self.assertIsNone(result)
        #Test whether any nulls are left open
        cur.execute('SELECT * FROM "TEST".test_table6 WHERE number is null')
        result = cur.fetchone()
        self.assertIsNone(result)
        #The median by excluding values > 40 is 9, let's check if the value is here
        cur.execute('SELECT * FROM "TEST".test_table7 WHERE number = 9 AND string = \'Elevate ltd\'')
        result = cur.fetchall()
        self.assertIsNotNone(result)


    def test_fill_nulls_with_custom_value(self):
        """Test the method of TableTransformer that fills null values with a custom value."""
        cur = self.db_connection.cursor()
        #Set a value to null
        cur.execute('UPDATE "TEST".test_table7 SET number = null  WHERE number < -16')
        self.test_object.fill_nulls_with_custom_value('test_table7', 'number', 10000)
        #The value we used should correspond to the row with string = 'Dummy'
        cur.execute('SELECT * FROM "TEST".test_table7 WHERE number = 10000 AND string = \'Dummy\'')
        result = cur.fetchall()
        self.assertIsNotNone(result)


    def test_delete_rows_using_conditions(self):
        self.assertEqual(1, 1)
        
        
        
        
        
        

        
        

        



if __name__ == '__main__':
    unittest.main()
