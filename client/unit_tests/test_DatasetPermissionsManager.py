import unittest
from Controller.DatasetPermissionsManager import DatasetPermissionsManager
from Controller.DatasetManager import DatasetManager
from Model.DatabaseConfiguration import TestConnection
from passlib.hash import sha256_crypt

class TestDatasetPermissionsManager(unittest.TestCase):
    """Tests for the DatasetPermissionManager class."""

    def __create_dataset_manually(self, name, desc):
        self.cur.execute("INSERT INTO system.datasets(setname, description) VALUES (%s, %s) RETURNING setid;", [name, desc])
        self.db_conn.commit()
        setid = int(self.cur.fetchone()[0])

        # CREATE SCHEMA
        self.cur.execute("CREATE SCHEMA \"{}\";".format(int(setid)))
        self.db_conn.commit()

        # CREATE BACKUP SCHEMA
        self.cur.execute("CREATE SCHEMA \"original_{}\";".format(int(setid)))
        self.db_conn.commit()

        return setid

    def __delete_dataset_manually(self, setid):
        self.cur.execute("DELETE FROM SYSTEM.datasets WHERE setid = %s;", [setid])
        self.db_conn.commit()

        self.cur.execute("DROP SCHEMA IF EXISTS \"{}\";".format(int(setid)))
        self.db_conn.commit()

        self.cur.execute("DROP SCHEMA IF EXISTS \"original_{}\";".format(int(setid)))
        self.db_conn.commit()

    def __create_user_manually(self, fname, lname, email):
        self.cur.execute("INSERT INTO SYSTEM.user_accounts(fname, lname, email, passwd) VALUES (%s, %s, %s, %s) RETURNING userid;", [fname, lname, email, sha256_crypt.hash("password123")])
        self.db_conn.commit()
        return int(self.cur.fetchone()[0])

    def __delete_user_manually(self, userid):
        self.cur.execute("DELETE FROM SYSTEM.user_accounts WHERE userid = %s;", [userid])
        self.db_conn.commit()

    def __add_perms_manually(self, setid, userid, permtype):
        self.cur.execute("INSERT INTO SYSTEM.set_permissions(userid, setid, permission_type) VALUES (%s, %s, %s);", [userid, setid, permtype])
        self.db_conn.commit()

    def __delete_perms_manually(self, setid, userid):
        self.cur.execute("DELETE FROM SYSTEM.set_permissions WHERE setid = %s AND userid = %s;", [setid, userid])
        self.db_conn.commit()

    @classmethod
    def setUpClass(cls):
        """Set up the test environment."""
        cls.db_conn = TestConnection().get_db()
        cls.cur = cls.db_conn.cursor()
        cls.engine = TestConnection().get_engine()

        cls.cur.execute("DELETE FROM SYSTEM.user_accounts WHERE TRUE;");
        cls.db_conn.commit()

        cls.cur.execute("DELETE FROM SYSTEM.datasets WHERE TRUE;");
        cls.db_conn.commit()

        cls.cur.execute("DELETE FROM SYSTEM.set_permissions WHERE TRUE;");
        cls.db_conn.commit()

        # create the dataset
        dataset_id = cls.__create_dataset_manually(cls, 'Dataset', 'Test Dataset')

        cls.dataset = DatasetManager.getDataset(dataset_id, db_conn = cls.db_conn)

        # 2 admin users
        cls.user_1 = cls.__create_user_manually(cls, 'User1', 'User1', 'user1@email.com')
        cls.user_2 = cls.__create_user_manually(cls, 'User2', 'User2', 'user2@email.com')

        cls.__add_perms_manually(cls, cls.dataset.setid, cls.user_1, 'admin')
        cls.__add_perms_manually(cls, cls.dataset.setid, cls.user_2, 'admin')

        # 1 write user
        cls.user_3 = cls.__create_user_manually(cls, 'User3', 'User3', 'user3@email.com')

        cls.__add_perms_manually(cls, cls.dataset.setid, cls.user_3, 'write')

        # 3 read users
        cls.user_4 = cls.__create_user_manually(cls, 'User4', 'User4', 'user4@email.com')
        cls.user_5 = cls.__create_user_manually(cls, 'User5', 'User5', 'user5@email.com')
        cls.user_6 = cls.__create_user_manually(cls, 'User6', 'User6', 'user6@email.com')

        cls.__add_perms_manually(cls, cls.dataset.setid, cls.user_4, 'read')
        cls.__add_perms_manually(cls, cls.dataset.setid, cls.user_5, 'read')
        cls.__add_perms_manually(cls, cls.dataset.setid, cls.user_6, 'read')

        # user to add and remove perms
        cls.user_7 = cls.__create_user_manually(cls, 'User7', 'User7', 'user7@email.com')

        # user that is not associated with any dataset.
        cls.user_8 = cls.__create_user_manually(cls, 'User8', 'User8', 'user8@gmail.com')
    # ENDMETHOD

    @classmethod
    def tearDownClass(cls):
        """Clean up the test environment."""

        cls.cur.execute("DELETE FROM SYSTEM.user_accounts WHERE TRUE;");
        cls.db_conn.commit()

        cls.cur.execute("DELETE FROM SYSTEM.datasets WHERE TRUE;");
        cls.db_conn.commit()

        cls.cur.execute("DELETE FROM SYSTEM.set_permissions WHERE TRUE;");
        cls.db_conn.commit()

        cls.db_conn.close()
    # ENDMETHOD

    def test_getAdminPerms(self):
        admin_ids = DatasetPermissionsManager.getAdminPerms(setid=self.dataset.setid, db_conn = self.db_conn)

        self.assertTrue(self.user_1 in admin_ids)
        self.assertTrue(self.user_2 in admin_ids)

        self.assertEqual(len(admin_ids), 2)
    # ENDTEST

    def test_getWritePerms(self):
        write_ids = DatasetPermissionsManager.getWritePerms(setid=self.dataset.setid, db_conn = self.db_conn)

        self.assertTrue(self.user_3 in write_ids)

        self.assertEqual(len(write_ids), 1)
    # ENDTEST

    def test_getReadPerms(self):
        read_ids = DatasetPermissionsManager.getReadPerms(setid=self.dataset.setid, db_conn = self.db_conn)

        self.assertTrue(self.user_4 in read_ids)
        self.assertTrue(self.user_5 in read_ids)
        self.assertTrue(self.user_6 in read_ids)

        self.assertEqual(len(read_ids), 3)
    # ENDTEST

    def test_addPerm(self):
        # add perm
        DatasetPermissionsManager.addPerm(setid=self.dataset.setid, userid=self.user_7, perm_type='admin', db_conn = self.db_conn)

        self.cur.execute("SELECT * FROM SYSTEM.set_permissions WHERE userid = %s and setid = %s;", [self.user_7, self.dataset.setid])
        result = self.cur.fetchone()

        self.assertTrue(result is not None)

        self.__delete_perms_manually(setid=self.dataset.setid, userid=self.user_7)
    # ENDTEST

    def test_removePerm(self):
        self.__add_perms_manually(setid=self.dataset.setid, userid=self.user_7, permtype='admin')

        self.cur.execute("SELECT * FROM SYSTEM.set_permissions WHERE userid = %s and setid = %s;", [self.user_7, self.dataset.setid])
        result = self.cur.fetchone()

        self.assertTrue(result is not None)

        # remove perm
        DatasetPermissionsManager.removePerm(setid=self.dataset.setid, userid=self.user_7, db_conn = self.db_conn)

        self.cur.execute("SELECT * FROM SYSTEM.set_permissions WHERE userid = %s and setid = %s;", [self.user_7, self.dataset.setid])
        result = self.cur.fetchone()

        self.assertTrue(result is None)

        self.__delete_perms_manually(setid=self.dataset.setid, userid=self.user_7)
    # ENDTEST

    def test_getPermForUserID(self):
        self.assertEqual(DatasetPermissionsManager.getPermForUserID(setid=self.dataset.setid, userid=self.user_1, db_conn = self.db_conn), 'admin')
        self.assertEqual(DatasetPermissionsManager.getPermForUserID(setid=self.dataset.setid, userid=self.user_2, db_conn = self.db_conn), 'admin')
        self.assertEqual(DatasetPermissionsManager.getPermForUserID(setid=self.dataset.setid, userid=self.user_3, db_conn = self.db_conn), 'write')
        self.assertEqual(DatasetPermissionsManager.getPermForUserID(setid=self.dataset.setid, userid=self.user_4, db_conn = self.db_conn), 'read')
        self.assertEqual(DatasetPermissionsManager.getPermForUserID(setid=self.dataset.setid, userid=self.user_5, db_conn = self.db_conn), 'read')
        self.assertEqual(DatasetPermissionsManager.getPermForUserID(setid=self.dataset.setid, userid=self.user_6, db_conn = self.db_conn), 'read')
        self.assertEqual(DatasetPermissionsManager.getPermForUserID(setid=self.dataset.setid, userid=self.user_8, db_conn = self.db_conn), None)
    # ENDTEST

    def test_userHasAccessTo(self):
        # admin access
        self.assertTrue(DatasetPermissionsManager.userHasSpecifiedAccessTo( setid=self.dataset.setid, userid=self.user_1, minimum_perm_type='admin', db_conn = self.db_conn))
        self.assertTrue(DatasetPermissionsManager.userHasSpecifiedAccessTo( setid=self.dataset.setid, userid=self.user_2, minimum_perm_type='admin', db_conn = self.db_conn))
        self.assertFalse(DatasetPermissionsManager.userHasSpecifiedAccessTo(setid=self.dataset.setid, userid=self.user_3, minimum_perm_type='admin', db_conn = self.db_conn))
        self.assertFalse(DatasetPermissionsManager.userHasSpecifiedAccessTo(setid=self.dataset.setid, userid=self.user_4, minimum_perm_type='admin', db_conn = self.db_conn))
        self.assertFalse(DatasetPermissionsManager.userHasSpecifiedAccessTo(setid=self.dataset.setid, userid=self.user_5, minimum_perm_type='admin', db_conn = self.db_conn))
        self.assertFalse(DatasetPermissionsManager.userHasSpecifiedAccessTo(setid=self.dataset.setid, userid=self.user_6, minimum_perm_type='admin', db_conn = self.db_conn))
        self.assertFalse(DatasetPermissionsManager.userHasSpecifiedAccessTo(setid=self.dataset.setid, userid=self.user_8, minimum_perm_type='admin', db_conn = self.db_conn))

        # write access
        self.assertTrue(DatasetPermissionsManager.userHasSpecifiedAccessTo( setid=self.dataset.setid, userid=self.user_1, minimum_perm_type='write', db_conn = self.db_conn))
        self.assertTrue(DatasetPermissionsManager.userHasSpecifiedAccessTo( setid=self.dataset.setid, userid=self.user_2, minimum_perm_type='write', db_conn = self.db_conn))
        self.assertTrue(DatasetPermissionsManager.userHasSpecifiedAccessTo( setid=self.dataset.setid, userid=self.user_3, minimum_perm_type='write', db_conn = self.db_conn))
        self.assertFalse(DatasetPermissionsManager.userHasSpecifiedAccessTo(setid=self.dataset.setid, userid=self.user_4, minimum_perm_type='write', db_conn = self.db_conn))
        self.assertFalse(DatasetPermissionsManager.userHasSpecifiedAccessTo(setid=self.dataset.setid, userid=self.user_5, minimum_perm_type='write', db_conn = self.db_conn))
        self.assertFalse(DatasetPermissionsManager.userHasSpecifiedAccessTo(setid=self.dataset.setid, userid=self.user_6, minimum_perm_type='write', db_conn = self.db_conn))
        self.assertFalse(DatasetPermissionsManager.userHasSpecifiedAccessTo(setid=self.dataset.setid, userid=self.user_8, minimum_perm_type='write', db_conn = self.db_conn))

        # read access
        self.assertTrue(DatasetPermissionsManager.userHasSpecifiedAccessTo( setid=self.dataset.setid, userid=self.user_1, minimum_perm_type='read', db_conn = self.db_conn))
        self.assertTrue(DatasetPermissionsManager.userHasSpecifiedAccessTo( setid=self.dataset.setid, userid=self.user_2, minimum_perm_type='read', db_conn = self.db_conn))
        self.assertTrue(DatasetPermissionsManager.userHasSpecifiedAccessTo( setid=self.dataset.setid, userid=self.user_3, minimum_perm_type='read', db_conn = self.db_conn))
        self.assertTrue(DatasetPermissionsManager.userHasSpecifiedAccessTo( setid=self.dataset.setid, userid=self.user_4, minimum_perm_type='read', db_conn = self.db_conn))
        self.assertTrue(DatasetPermissionsManager.userHasSpecifiedAccessTo( setid=self.dataset.setid, userid=self.user_5, minimum_perm_type='read', db_conn = self.db_conn))
        self.assertTrue(DatasetPermissionsManager.userHasSpecifiedAccessTo( setid=self.dataset.setid, userid=self.user_6, minimum_perm_type='read', db_conn = self.db_conn))
        self.assertFalse(DatasetPermissionsManager.userHasSpecifiedAccessTo(setid=self.dataset.setid, userid=self.user_8, minimum_perm_type='read', db_conn = self.db_conn))
    # ENDTEST
