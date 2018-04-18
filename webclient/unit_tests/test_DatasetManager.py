sys.path.append(os.path.join(sys.path[0],'..', 'Controller'))
sys.path.append(os.path.join(sys.path[0],'..', 'Model'))
import unittest
from DatasetInfo import DatasetInfo
from UserInfo import UserInfo
from DatasetManager import DatasetManager
from UserManager import UserManager
from DatabaseConfiguration import DatabaseConfiguration

class TestDatasetManager(unittest.TestCase):
    """Tests for the DatasetManager class."""
    
    @classmethod
    def setUpClass(cls):
        """Set up the test environment."""
        db_conn = DatabaseConfiguration.get_db()
        engine = DatabaseConfiguration.get_engine()

    @classmethod
    def tearDownClass(cls):
        """Clean up the test environment."""
        pass
