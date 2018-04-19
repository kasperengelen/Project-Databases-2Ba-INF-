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


    @classmethod
    def setUpClass(cls):
        cls.db_connection = DatabaseConfiguration().get_db()
        cls.test_object = dl.DataLoader(0, cls.db_connection)

        cur = cls.db_connection.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS \"0\"")
        cls.db_connection.commit

    @classmethod
    def tearDownClass(cls):
        cls.db_connection.cursor().execute("DROP SCHEMA \"TEST\" CASCADE")
        cls.db_connection.commit()
        # Close database connection
        cls.db_connection.close()

    def test_read_csv(self):
        self.test_object.read_file(os.path.dirname(os.path.abspath(__file__)) + "src/departments.csv", True)
        self.db_connection.cursor().execute("SELECT table_name FROM information_schema.tables WHERE table_name = departments LIMIT 1;")
        self.assertEqual(self.db_connection.cursor().fetchone(), "departments")

    def test_read_zip(self):
        pass

    def test_read_dump(self):
        pass

if __name__ == '__main__':
    unittest.main()
