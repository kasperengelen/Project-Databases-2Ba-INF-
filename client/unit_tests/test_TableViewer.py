import unittest
import psycopg2
from sqlalchemy import create_engine
import Controller.TableViewer as tv
from Model.DatabaseConfiguration import TestConnection
import math



class TestTableViewer(unittest.TestCase):
    db_connection = None
    engine = None
    test_object = None
    test_object2 = None



    @classmethod
    def setUpClass(cls):
        #Make all the connections and objects needed
        cls.db_connection = TestConnection().get_db()
        cls.engine = TestConnection().get_engine()
        cur = cls.db_connection.cursor()
        cur.execute('CREATE SCHEMA IF NOT EXISTS "0"')
        cls.db_connection.commit()
        creation_query = """CREATE TABLE "0".test_table (
        string VARCHAR(255),
        number INTEGER,
        date_time VARCHAR(255));"""
        creation_query2 = creation_query.replace("test_table", "stat_table")
        #In some cases the test fails in a way that tearDownClass is not called and the table still exists
        #Sadly we can't confirm if the table is still correct, so we better rebuild it.
        try:
            # cur.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA \"TEST\" TO dbadmin;")
            # cur.execute("GRANT ALL PRIVILEGES ON SCHEMA \"TEST\" TO dbadmin;")
            # table 1
            cur.execute(creation_query)
            # table 2
            cur.execute(creation_query2)

        except psycopg2.ProgrammingError:
            #If it was still present in the database we better drop the schema and rebuild it
            cls.db_connection.rollback()
            cur.execute('DROP SCHEMA "0" CASCADE')
            cur.execute('CREATE SCHEMA "0"')
            cls.db_connection.commit()
            cur.execute(creation_query)
            cur.execute(creation_query2)
            cls.db_connection.commit()
            
            
            
        values = [('C-Corp', 1, '08/08/1997'), ('Apple', 10, '01/04/1976'), ('Microsoft', 8, '04/04/1975') , ('Nokia', 3000, '12/05/1865') , ('Samsung', 7, '01/03/1938'),
                  ('Huawei', 4521, '15/09/1987'), ('Razer', 9000, '01/01/1998')]

        values2 = [('haha', 1, '01/01/2001'), ('hihi', 2, '01/01/2001'), ('hoho', 3, '01/01/2001'), ('haha', 2, '20/05/2013'),
                   (None, 2, None), (None, 4, '05/12/2017')]

        for i in range(10):
            for v in values:
                cur.execute('INSERT INTO "0".test_table VALUES(%s, %s, %s)', v)

        for v in values2:
            cur.execute('INSERT INTO "0".stat_table VALUES(%s, %s, %s)', v)
        cls.db_connection.commit()

        #Make the objects after the tables actually exist and contain data.
        cls.test_object = tv.TableViewer(0, 'test_table', cls.engine, cls.db_connection)
        cls.test_object2 = tv.TableViewer(0, 'stat_table', cls.engine, cls.db_connection)


    @classmethod
    def tearDownClass(cls):
        cls.db_connection.cursor().execute('DROP SCHEMA "0" CASCADE')
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

    def test_get_most_frequent_value(self):
        most_frequent = self.test_object2.get_most_frequent_value("string")
        self.assertEqual(most_frequent, "haha")
        most_frequent = self.test_object2.get_most_frequent_value("number")
        self.assertEqual(most_frequent, 2)
        most_frequent = self.test_object2.get_most_frequent_value("date_time")
        self.assertEqual(most_frequent, '01/01/2001')

    def test_get_null_frequency(self):
        null_frequency = self.test_object2.get_null_frequency("string")
        self.assertEqual(null_frequency, 2)
        null_frequency = self.test_object2.get_null_frequency("number")
        self.assertEqual(null_frequency, 0)
        null_frequency = self.test_object2.get_null_frequency("date_time")
        self.assertEqual(null_frequency, 1)

    def test_get_max(self):
        max = self.test_object2.get_max("string")
        self.assertEqual(max, "N/A")
        max = self.test_object2.get_max("number")
        self.assertEqual(max, 4)
        max = self.test_object2.get_max("date_time")
        self.assertEqual(max, "N/A")

    def test_get_min(self):
        min = self.test_object2.get_min("string")
        self.assertEqual(min, "N/A")
        min = self.test_object2.get_min("number")
        self.assertEqual(min, 1)
        min = self.test_object2.get_min("date_time")
        self.assertEqual(min, "N/A")

    def test_get_avg(self):
        avg = self.test_object2.get_avg("string")
        self.assertEqual(avg, "N/A")
        avg = self.test_object2.get_avg("number")
        self.assertTrue(math.isclose(14/6, avg))
        avg = self.test_object2.get_avg("date_time")
        self.assertEqual(avg, "N/A")
        


if __name__ == '__main__':
    unittest.main()

    
