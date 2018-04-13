import unittest
import sys, os
sys.path.append(os.path.join(sys.path[0],'..', 'Controller'))
sys.path.append(os.path.join(sys.path[0],'..', 'Model'))
import psycopg2
from sqlalchemy import create_engine
import TableViewer as tv
from DatabaseConfiguration import DatabaseConfiguration



class TestTableViewer(unittest.TestCase):
    db_connection = None
    engine = None
    test_object = None



    @classmethod
    def setUpClass(cls):
        #Make all the connections and objects needed
        cls.db_connection = DatabaseConfiguration().get_db()
        cls.engine = DatabaseConfiguration().get_engine()
        cls.test_object = tv.TableViewer('TEST', 'test_table', cls.engine)

        cur = cls.db_connection.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS \"TEST\"")
        cls.db_connection.commit()
        creation_query = """CREATE TABLE "TEST".test_table (
        string VARCHAR(255) NOT NULL,
        number INTEGER NOT NULL,
        date_time VARCHAR(255) NOT NULL);"""
        #In some cases the test fails in a way that tearDownClass is not called and the table still exists
        #Sadly we can't confirm if the table is still correct, so we better rebuild it.
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

        for i in range(10):
            for v in values:
                cur.execute("INSERT INTO \"TEST\".test_table VALUES(%s, %s, %s)", v)

        
        cls.db_connection.commit()


    @classmethod
    def tearDownClass(cls):
        cls.db_connection.cursor().execute("DROP SCHEMA \"TEST\" CASCADE")
        cls.db_connection.commit()
        #Close database connection
        cls.db_connection.close()




    def test_get_attributes(self):
        all_attributes = ['string', 'number', 'date_time']
        result = self.test_object.get_attributes()
        #Check if both lists are the same size
        self.assertEqual(len(all_attributes), len(result))
        #Check if every element of attributes is in the result
        self.assertEqual(all_attributes[0] in result, True)
        self.assertEqual(all_attributes[1] in result, True)
        self.assertEqual(all_attributes[2] in result, True)

    def test_is_in_range(self):
        #There are 70 rows in the table
        #Check if page one with 50 rows per page is in range
        self.assertEqual(self.test_object.is_in_range(1, 50), True)
        #Check if page 7 with 10 rows per page is in range
        self.assertEqual(self.test_object.is_in_range(7, 10), True)
        #Check if page 8 with 10 rows per page is in range
        self.assertEqual(self.test_object.is_in_range(8, 50), False)
        #Check if it fails for way big values
        self.assertEqual(self.test_object.is_in_range(72, 1), False)
        self.assertEqual(self.test_object.is_in_range(1001, 50), False)
        self.assertEqual(self.test_object.is_in_range(2458, 50), False)

    def test_get_page_indices(self):
        #We can set maxrows of our object since we don't explicitly check range
        self.test_object.maxrows = 50
        indices = self.test_object.get_page_indices(10, 1)
        self.assertEqual(indices, ['1', '2', '3', '4', '5'])
        indices = self.test_object.get_page_indices(10, 3)
        self.assertEqual(indices, ['1', '2', '3', '4', '5'])

        #Set the number of rows to 50 000
        self.test_object.maxrows = 50000
        #Get indices when being on page 1 when displaying 50 rows per page
        indices = self.test_object.get_page_indices(50, 1)
        self.assertEqual(indices, ['1', '2', '3', '4', '...', '1000'])
        #Get indices when being on page 2 when displaying 50 rows per page
        indices = self.test_object.get_page_indices(50, 2)
        self.assertEqual(indices, ['1', '2', '3', '4', '...', '1000'])
        #Get indices when being on page 72 when displaying 50 rows per page
        indices = self.test_object.get_page_indices(50, 72)
        self.assertEqual(indices, ['1', '...', '71', '72', '73', '...', '1000'])
        #Get indices when being on page 997 when displaying 50 rows per page
        indices = self.test_object.get_page_indices(50, 997)
        self.assertEqual(indices, ['1', '...', '996', '997', '998', '...', '1000'])
        #Get indices when being on page 1000 when displaying 50 rows per page
        indices = self.test_object.get_page_indices(50, 997)
        self.assertEqual(indices, ['1', '...', '997', '998', '999', '1000'])
        
        


if __name__ == '__main__':
    unittest.main()

    
