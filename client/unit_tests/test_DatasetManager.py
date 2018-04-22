import unittest
from Controller.DatasetManager import DatasetManager
from Model.DatabaseConfiguration import DatabaseConfiguration

from passlib.hash import sha256_crypt

class TestDatasetManager(unittest.TestCase):
    """Tests for the DatasetManager class."""
    
    def __create_dataset_manually(self, name, desc):
        self.db_conn.cursor().execute("INSERT INTO SYSTEM.datasets(setname, description) VALUES (%s, %s) RETURNING setid;", [name, desc])
        self.db_conn.commit()
        setid = int(self.db_conn.cursor().fetchall()[0][0])

        # CREATE SCHEMA
        self.db_conn.cursor().execute("CREATE SCHEMA \"{}\";".format(int(setid)))
        self.db_conn.commit()

        # CREATE BACKUP SCHEMA
        self.db_conn.cursor().execute("CREATE SCHEMA \"original_{}\";".format(int(setid)))
        self.db_conn.commit()

        return setid

    def __delete_dataset_manually(self, setid):
        self.db_conn.cursor().execute("DELETE FROM SYSTEM.datasets WHERE setid = %s;", [setid])
        self.db_conn.commit()

        self.db_conn.cursor().execute("DROP SCHEMA IF EXISTS \"{}\";".format(int(setid)))
        self.db_conn.commit()

        self.db_conn.cursor().execute("DROP SCHEMA IF EXISTS \"original_{}\";".format(int(setid)))
        self.db_conn.commit()

    def __dataset_exists(self, setid):
        self.db_conn.cursor().execute("SELECT * FROM SYSTEM.datasets WHERE setid = %s;", [setid])
        result = self.db_conn.cursor().fetchone()

        return result is not None

    @classmethod
    def setUpClass(cls):
        """Set up the test environment."""
        cls.db_conn = DatabaseConfiguration().get_db()
        cls.engine = DatabaseConfiguration().get_engine()

        cls.db_conn.cursor().execute("DELETE FROM SYSTEM.datasets WHERE TRUE;")
        cls.db_conn.commit()


        # create 4 read-only datasets
        cls.testset_1 = cls.__create_dataset_manually(cls, 'Set 1', '1st Test Set') # user has read access
        cls.testset_2 = cls.__create_dataset_manually(cls, 'Set 2', '2st Test Set') # user has write access
        cls.testset_3 = cls.__create_dataset_manually(cls, 'Set 3', '3st Test Set') # user has admin access
        cls.testset_4 = cls.__create_dataset_manually(cls, 'Set 4', '4st Test Set') # user has no access

        cls.deletedset = cls.__create_dataset_manually(cls, 'Deleted set', 'A set that will not exist')
        cls.__delete_dataset_manually(cls, cls.deletedset)

        # create 1 dataset that will need to be modified
        cls.testset_changeMeta = cls.__create_dataset_manually(cls, 'Set change metadata', 'DatasetManager.changeMetadata() test')

        # create 1 dataset that will be destroyed.
        cls.testset_destroySet = cls.__create_dataset_manually(cls, 'Destroy dataset test', 'testset for DatasetManager.destroyDataset()')

        # create a user to test datasets
        cls.db_conn.cursor().execute("INSERT INTO SYSTEM.user_accounts(fname, lname, email, passwd) VALUES ('Jan', 'Met de pet', 'Jan.met.de.pet@abc.com', %s) RETURNING userid;", [sha256_crypt.hash("password123")])
        cls.db_conn.commit()

        cls.user_1 = int(cls.db_conn.cursor().fetchone()[0])

        # add user to dataset
        cls.db_conn.cursor().execute("INSERT INTO SYSTEM.set_permissions(userid, setid, permission_type) VALUES (%s, %s, 'read');", [cls.user_1, cls.testset_1])
        cls.db_conn.cursor().execute("INSERT INTO SYSTEM.set_permissions(userid, setid, permission_type) VALUES (%s, %s, 'write');", [cls.user_1, cls.testset_2])
        cls.db_conn.cursor().execute("INSERT INTO SYSTEM.set_permissions(userid, setid, permission_type) VALUES (%s, %s, 'admin');", [cls.user_1, cls.testset_3])
        cls.db_conn.commit()

    @classmethod
    def tearDownClass(cls):
        """Clean up the test environment."""

        cls.__delete_dataset_manually(cls, cls.testset_1)
        cls.__delete_dataset_manually(cls, cls.testset_2)
        cls.__delete_dataset_manually(cls, cls.testset_3)
        cls.__delete_dataset_manually(cls, cls.testset_4)
        cls.__delete_dataset_manually(cls, cls.testset_changeMeta)
        cls.__delete_dataset_manually(cls, cls.testset_destroySet)

        cls.db_conn.cursor().execute("DELETE FROM SYSTEM.user_accounts WHERE userid = %s;", [cls.user_1])
        cls.db_conn.commit()

        cls.db_conn.close()
        cls.engine.dispose()

    def test_existsID(self):
        self.assertTrue(DatasetManager.existsID(self.testset_1, db_conn = self.db_conn))
        self.assertFalse(DatasetManager.existsID(self.deletedset, db_conn = self.db_conn))

    def test_getDataset(self):
        # check with ID that is specified

        dataset = DatasetManager.getDataset(self.testset_1, db_conn = self.db_conn)

        self.assertEqual(dataset.setid, self.testset_1)
        self.assertEqual(dataset.name, 'Set 1')
        self.assertEqual(dataset.desc, '1st Test Set')

    def test_getDatasetsForUser(self):
        # specified user
        # a dataset that the user has access to
        # a dataset that the user does not has access to

        setlist = DatasetManager.getDatasetsForUser(self.user_1, db_conn = self.db_conn)
        setid_list = [dataset.setid for dataset in setlist]

        self.assertTrue(self.testset_1 in setid_list)
        self.assertTrue(self.testset_2 in setid_list)
        self.assertTrue(self.testset_3 in setid_list)
        self.assertFalse(self.testset_4 in setid_list)

    def test_createDataset(self):
        # create dataset
        setid = DatasetManager.createDataset(name="abc", desc="def", db_conn = self.db_conn)

        # check that all the necessary DB-entries exist
        self.assertTrue(self.__dataset_exists(setid))


        self.__delete_dataset_manually(setid)

    def test_destroyDataset(self):

        # destroy dataset
        DatasetManager.destroyDataset(setid = self.testset_destroySet, db_conn = self.db_conn)

        # check that the specified DB-entries do not exist
        self.assertFalse(self.__dataset_exists(self.testset_destroySet))

    def test_getAllDatasets(self):

        setlist = DatasetManager.getAllDatasets(db_conn = self.db_conn)
        setid_list = [dataset.setid for dataset in setlist]

        

        # datasets that exist need to be in the list
        self.assertTrue(self.testset_1 in setid_list)

        # a nonexisting id must not be in the list
        self.assertFalse(self.deletedset in setid_list)

    def test_userHasAccessTo(self):
        # admin access
        self.assertFalse(DatasetManager.userHasAccessTo(self.testset_1, self.user_1, 'admin', db_conn = self.db_conn))
        self.assertFalse(DatasetManager.userHasAccessTo(self.testset_2, self.user_1, 'admin', db_conn = self.db_conn))
        self.assertTrue(DatasetManager.userHasAccessTo(self.testset_3, self.user_1, 'admin', db_conn = self.db_conn))
        self.assertFalse(DatasetManager.userHasAccessTo(self.testset_4, self.user_1, 'admin', db_conn = self.db_conn))

        # write access
        self.assertFalse(DatasetManager.userHasAccessTo(self.testset_1, self.user_1, 'write', db_conn = self.db_conn))
        self.assertTrue(DatasetManager.userHasAccessTo(self.testset_2, self.user_1, 'write', db_conn = self.db_conn))
        self.assertTrue(DatasetManager.userHasAccessTo(self.testset_3, self.user_1, 'write', db_conn = self.db_conn))
        self.assertFalse(DatasetManager.userHasAccessTo(self.testset_4, self.user_1, 'write', db_conn = self.db_conn))

        # read access
        self.assertTrue(DatasetManager.userHasAccessTo(self.testset_1, self.user_1, 'read', db_conn = self.db_conn))
        self.assertTrue(DatasetManager.userHasAccessTo(self.testset_2, self.user_1, 'read', db_conn = self.db_conn))
        self.assertTrue(DatasetManager.userHasAccessTo(self.testset_3, self.user_1, 'read', db_conn = self.db_conn))
        self.assertFalse(DatasetManager.userHasAccessTo(self.testset_4, self.user_1, 'read', db_conn = self.db_conn))

    def test_changeMetaData(self):
        # change values for dataset

        DatasetManager.changeMetadata(self.testset_changeMeta, 'New Name', 'New Desc', db_conn = self.db_conn)

        # check that new values are present.
        self.db_conn.cursor().execute("SELECT * FROM SYSTEM.datasets WHERE setid = %s;", [self.testset_changeMeta])
        result = self.db_conn.cursor().fetchone()

        self.assertEqual(result[1], 'New Name')
        self.assertEqual(result[2], 'New Desc')
