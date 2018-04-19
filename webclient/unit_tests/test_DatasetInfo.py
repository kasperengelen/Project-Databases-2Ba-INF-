import unittest
from Controller.DatasetInfo import DatasetInfo
from Model.DatabaseConfiguration import DatabaseConfiguration

class TestDatasetInfo(unittest.TestCase):
    """Tests for the DatasetInfo class."""
    
    @classmethod
    def setUpClass(cls):
        """Set up the test environment."""
        db_conn = DatabaseConfiguration.get_db()
        engine = DatabaseConfiguration.get_engine()

    @classmethod
    def tearDownClass(cls):
        """Clean up the test environment."""
        pass
