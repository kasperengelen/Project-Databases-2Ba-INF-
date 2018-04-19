import unittest
from UserInfo import UserInfo
from DatabaseConfiguration import DatabaseConfiguration

class TestUserInfo(unittest.TestCase):
    """Tests for the UserInfo class."""
    
    @classmethod
    def setUpClass(cls):
        """Set up the test environment."""
        db_conn = DatabaseConfiguration.get_db()
        engine = DatabaseConfiguration.get_engine()

        

    @classmethod
    def tearDownClass(cls):
        """Clean up the test environment."""
        cls.db_conn.close()
