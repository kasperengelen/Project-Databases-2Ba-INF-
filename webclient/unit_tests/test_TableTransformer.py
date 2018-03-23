import unittest
import sys, os
sys.path.append(os.path.join(sys.path[0],'..'))
import psycopg2
import TableTransformer as transformer

#This file contains tests for TableTransformer that specificaly overwrites tables.
#For the tests on TableTransformer that create new tables instead overwriting refer to "test_TableTransformerCopy.py"

class TestTableTransformer(unittest.TestCase):
    db_connection = None
    test_object = None

    
    @classmethod
    def setUpClass(cls):
        cls.db_connection = psycopg2.connect("dbname='hmtbpols' user='hmtbpols' host='baasu.db.elephantsql.com' password='yIje-2zT-zF0YyJywkAy57h6ob3ZnoV2'")
        #Fake userid = 0 and And setid is actually the TEST set
        cls.test_object = transformer.TableTransformer(0, 'TEST', cls.db_connection, None, True)
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
            
            
        values = [('C-Corp', 1, '08/08/1997'), ('Apple', 10, '01/04/1976'), ('Microsoft', 8, '04/04/1975') , ('Nokia', 3000, '12/05/1865') , ('Samsung', 7, '01/03/1938'),
                  ('Huawei', 4521, '15/09/1987'), ('Razer', 9000, '01/01/1998')]

        for v in values:
            cur.execute("INSERT INTO \"TEST\".test_table VALUES(%s, %s, %s)", v)

        
        cls.db_connection.commit()

    @classmethod
    def tearDownClass(cls):
        cls.db_connection.cursor().execute("DROP TABLE \"TEST\".test_table;")
        cls.db_connection.commit()
        #Close database connection
        cls.db_connection.close()
        

        
    def test_delete_attribute(self):
        #Let's test if we can delete the attribute named "garbage"
        self.test_object.delete_attribute('test_table', 'garbage')
        #Now the only attributes should be "string", "number", "date_time"
        cur = self.db_connection.cursor()
        cur.execute("""SELECT column_name FROM information_schema.columns WHERE table_schema = 'TEST'
                                              AND table_name   = 'test_table'""")
        #Fetched a list of tuples containing the tablenames
        tablenames = cur.fetchall()
        found = False
        for name in tablenames:
            if name[0] == 'garbage':
                found = True
        
                
        #Assert if it is in fact not found amongst remaining tablenames
        self.assertEqual(found, False)

    def test_find_and_replace(self):
        self.test_object.find_and_replace('test_table','string', 'C-Corp', 'Replacement')
        cur = self.db_connection.cursor()
        cur.execute("SELECT * FROM \"TEST\".test_table WHERE string = 'Replacement'")
        result = cur.fetchone()
        self.assertEqual(result[1], 1)
        self.assertEqual(result[2], '08/08/1997')

    #Test the conversion of numeric types (INTEGER, FLOAT)
    def test_numeric_conversion(self):
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

    #Test the conversion of character types
    def test_character_conversion(self):
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
        
        

        
        

        



if __name__ == '__main__':
    unittest.main()
