import unittest
import sys, os
sys.path.append(os.path.join(sys.path[0],'..', 'Controller'))
import psycopg2
import TableTransformer as transformer

#This file contains tests for TableTransformer that specifically creates new tables when transforming. This tests data manipulation methods of TableTransformer
#For the tests on data manipulation methods of TableTransformer that don't copy but overwrite the tables refer to "test_TableTransformer.py"

class TestTransformerCopy(unittest.TestCase):
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


    #Temporary dummy test
    def test_dummy(self):
        self.assertEqual(1, 1)




if __name__ == '__main__':
    unittest.main()
