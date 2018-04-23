import unittest
import sys, os
sys.path.append(os.path.join(sys.path[0],'..', 'Controller'))
sys.path.append(os.path.join(sys.path[0],'..', 'Model'))
import psycopg2
import DataLoader as dl
from DatabaseConfiguration import DatabaseConfiguration


class TestDataLoader(unittest.TestCase):
    db_connection = None
    engine = None
    test_object = None


    def setUp(self):
        self.db_connection = DatabaseConfiguration().get_db()
        self.test_object = dl.DataLoader(0, self.db_connection)

        cur = self.db_connection.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS \"0\"")
        self.db_connection.commit()


    def tearDown(self):
        self.db_connection.cursor().execute("DROP SCHEMA \"0\" CASCADE")
        self.db_connection.commit()
        # Close database connection
        self.db_connection.close()

    def test_read_csv(self):
        # load file
        self.test_object.read_file(os.path.dirname(os.path.abspath(__file__)) + "src/departments.csv", True)
        # test if table exists
        self.db_connection.cursor().execute('SELECT table_name FROM information_schema.tables WHERE table_schema = "0", table_name = departments LIMIT 1;')
        self.assertEqual(self.db_connection.cursor().fetchone(), "departments")
        # test contents of table
        self.db_connection.cursor().execute('SELECT * FROM "0".departments WHERE "0".departments.dept_no = "d005"')
        self.assertEqual(self.db_connection.cursor().fetchone(), ("d005", "Development"))

        # test backup
        # test if table exists
        self.db_connection.cursor().execute('SELECT table_name FROM information_schema.tables WHERE table_schema = "original_0", table_name = departments LIMIT 1;')
        self.assertEqual(self.db_connection.cursor().fetchone(), "departments")
        # test contents of table
        self.db_connection.cursor().execute('SELECT * FROM "original_0".departments WHERE "original_0".departments.dept_no = "d005"')
        self.assertEqual(self.db_connection.cursor().fetchone(), ("d005", "Development"))


    def test_read_zip(self):
        # load file
        self.test_object.read_file(os.path.dirname(os.path.abspath(__file__)) + "src/test.zip", True)
        # test if table exists
        self.db_connection.cursor().execute('SELECT table_name FROM information_schema.tables WHERE table_schema = "0", table_name = test1 OR table_name = test2 OR table_name = test3 LIMIT 3;')
        self.assertEqual(self.db_connection.cursor().fetchone(), ("test1", "test2", "test3"))
        # test contents of table
        self.db_connection.cursor().execute('SELECT * FROM "0".test1 WHERE "0".test1.Col_2 = "3"')
        self.assertEqual(self.db_connection.cursor().fetchone(), ("2", "3", "4", "5"))
        self.db_connection.cursor().execute('SELECT * FROM "0".test2 WHERE "0".test2.Col_3 = "84"')
        self.assertEqual(self.db_connection.cursor().fetchone(), ("100", "97", "84", "65"))
        self.db_connection.cursor().execute('SELECT * FROM "0".test3 WHERE "0".test3.Col_1 = "1"')
        self.assertEqual(self.db_connection.cursor().fetchone(), ("1", "1", "1", "1"))

        # test backup
        # test if table exists
        self.db_connection.cursor().execute('SELECT table_name FROM information_schema.tables WHERE table_schema = "original_0", table_name = test1 OR table_name = test2 OR table_name = test3 LIMIT 3;')
        self.assertEqual(self.db_connection.cursor().fetchone(), ("test1", "test2", "test3"))
        # test contents of table
        self.db_connection.cursor().execute('SELECT * FROM "orignal_0".test1 WHERE "original_0".test1.Col_2 = "3"')
        self.assertEqual(self.db_connection.cursor().fetchone(), ("2", "3", "4", "5"))
        self.db_connection.cursor().execute('SELECT * FROM "original_0".test2 WHERE "original_0".test2.Col_3 = "84"')
        self.assertEqual(self.db_connection.cursor().fetchone(), ("100", "97", "84", "65"))
        self.db_connection.cursor().execute('SELECT * FROM "original_0".test3 WHERE "original_0".test3.Col_1 = "1"')
        self.assertEqual(self.db_connection.cursor().fetchone(), ("1", "1", "1", "1"))

    def test_read_dump(self):
        # load file
        self.test_object.read_file(os.path.dirname(os.path.abspath(__file__)) + "src/test.dump", True)
        # test if table exists
        self.db_connection.cursor().execute('SELECT table_name FROM information_schema.tables WHERE table_schema = "0", table_name = abc LIMIT 1;')
        self.assertEqual(self.db_connection.cursor().fetchone(), "abc")
        # test contents of table
        self.db_connection.cursor().execute('SELECT * FROM "0".abc WHERE "0".abc.userid = 1')
        self.assertEqual(self.db_connection.cursor().fetchone(), (1, "test", "01/01/2018"))

        # test backup
        # test if table exists
        self.db_connection.cursor().execute('SELECT table_name FROM information_schema.tables WHERE table_schema = "original_0", table_name = abc LIMIT 1;')
        self.assertEqual(self.db_connection.cursor().fetchone(), "abc")
        # test contents of table
        self.db_connection.cursor().execute('SELECT * FROM "original_0".abc WHERE "original_0".abc.userid = 1')
        self.assertEqual(self.db_connection.cursor().fetchone(), (1, "test", "01/01/2018"))

if __name__ == '__main__':

    unittest.main()
