import unittest
from Controller.DatasetManager import DatasetManager
from Model.DatabaseConfiguration import DatabaseConfiguration

class TestDatasetManager(unittest.TestCase):
    """Tests for the DatasetManager class."""
    
    def __create_dataset_manually(name, desc):
        self.db_conn.cursor().execute("INSERT INTO SYSTEM.datasets(setname, description) VALUES (%s, %s) RETURNING setid;", [name, desc])
        setid = int(db_conn.cursor().fetchall()[0][0])

        # CREATE SCHEMA
        self.db_conn.cursor().execute("CREATE SCHEMA \"{}\";".format(int(setid)))
        self.db_conn.commit()

        # CREATE BACKUP SCHEMA
        self.db_conn.cursor().execute("CREATE SCHEMA \"original_{}\";".format(int(setid)))
        self.db_conn.commit()

        # create the history table
        self.db_conn.cursor().execute(open("Controller/dataset_history.sql", 'r').read())
        self.db_conn.cursor().execute("ALTER TABLE DATASET_HISTORY.temp RENAME TO \"{}\"".format(str(setid)))
        self.db_conn.commit()

        return setid

    @classmethod
    def setUpClass(cls):
        """Set up the test environment."""
        db_conn = DatabaseConfiguration.get_db()
        engine = DatabaseConfiguration.get_engine()


        # create 4 read-only datasets
        self.testset_1 = self.__create_dataset_manually('Set 1', '1st Test Set')
        self.testset_2 = self.__create_dataset_manually('Set 2', '2st Test Set')
        self.testset_3 = self.__create_dataset_manually('Set 3', '3st Test Set')
        self.testset_4 = self.__create_dataset_manually('Set 4', '4st Test Set')

        # create 1 dataset that will need to be modified
        self.testset_changeMeta = self.__create_dataset_manually('Set change metadata', 'DatasetManager.changeMetadata() test')

        # create 1 dataset that will be destroyed.
        self.testset_destroySet = self.__create_dataset_manually('Destroy dataset test', 'testset for DatasetManager.destroyDataset()')

        # create a user to test datasets

        


        # add user to dataset

    @classmethod
    def tearDownClass(cls):
        """Clean up the test environment."""
        cls.db_conn.close()
        cls.engine.dispose()

    def test_existsID(self):
        # check with ID that is specified

        # check that id - 1

        pass

    def test_getDataset(self):
        # check with ID that is specified

        pass

    def test_getDatasetsForUser(self):
        # specified user
        # a dataset that the user has access to
        # a dataset that the user does not has access to

        pass

    def test_createDataset(self):
        # create dataset

        # check that all the necessary DB-entries exist

        pass

    def test_destroyDataset(self):
        # create dataset

        # destroy dataset

        # check that the specified DB-entries do not exist

        pass

    def test_getAllDatasets(self):

        # datasets that exist need to be in the list

        # a nonexisting id must not be in the list

        pass

    def test_userHasAccessTo(self):
        # admin access
        # write access
        # read access

        # no access

        pass

    def test_changeMetaData(self):

        # change values for dataset

        # check that new values are present.

        pass