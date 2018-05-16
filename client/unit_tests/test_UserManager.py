import unittest
from Controller.UserManager import UserManager
from Model.DatabaseConfiguration import TestConnection

from passlib.hash import sha256_crypt

class TestUserManager(unittest.TestCase):
    """Tests for the UserManager class."""

    @classmethod
    def setUpClass(cls):
        """Set up the test environment."""
        cls.db_conn = TestConnection().get_db()
        cls.engine = TestConnection().get_engine()
        cls.cur = cls.db_conn.cursor()
        
        cls.cur.execute("DELETE FROM SYSTEM.user_accounts WHERE TRUE;");
        cls.db_conn.commit()

        # user for querying information
        cls.cur.execute("INSERT INTO SYSTEM.user_accounts(fname, lname, email, passwd) VALUES ('Peter', 'Selie', 'peter.selie@abc.com', %s) RETURNING userid;", [sha256_crypt.hash("password123")])
        cls.db_conn.commit()

        cls.testuser1_id = int(cls.cur.fetchone()[0])

        # user for edit pass, edit info
        cls.cur.execute("INSERT INTO SYSTEM.user_accounts(fname, lname, email, passwd) VALUES ('Piet', 'Klaas', 'piet.klaas@abc.com', %s) RETURNING userid;", [sha256_crypt.hash("password123")])
        cls.db_conn.commit()

        cls.testuser2_id = int(cls.cur.fetchone()[0])

        # user to demote
        cls.cur.execute("INSERT INTO SYSTEM.user_accounts(fname, lname, email, passwd, admin) VALUES ('Jan', 'met de pet', 'jan.met.de.pet@abc.com', %s, FALSE) RETURNING userid;", [sha256_crypt.hash("password123")])
        cls.db_conn.commit()

        cls.testuser3_id = int(cls.cur.fetchone()[0])

        # user to promote
        cls.cur.execute("INSERT INTO SYSTEM.user_accounts(fname, lname, email, passwd, admin) VALUES ('Peter', 'Pan', 'peter.pan@abc.com', %s, TRUE) RETURNING userid;", [sha256_crypt.hash("password123")])
        cls.db_conn.commit()

        cls.testuser4_id = int(cls.cur.fetchone()[0])

    @classmethod
    def tearDownClass(cls):
        """Clean up the test environment."""
        cls.cur.execute("DELETE FROM SYSTEM.user_accounts WHERE TRUE;");
        cls.db_conn.commit()


        cls.db_conn.close()
        cls.engine.dispose()

    def test_validateLogin(self):
        ## do login that should succeed
        result = UserManager.validateLogin("peter.selie@abc.com", "password123", db_conn = self.db_conn)
        self.assertTrue(result)

        ## do login that should fail
        # password incorrect
        result = UserManager.validateLogin("peter.selie@abc.com", "jrgjeingerg454", db_conn = self.db_conn)
        self.assertFalse(result)

        # email not correct
        result = UserManager.validateLogin("peter.selie@def.com", "password123", db_conn = self.db_conn)
        self.assertFalse(result)

    def test_existsID(self):
        ## check for exist
        self.assertTrue(UserManager.existsID(self.testuser1_id, db_conn = self.db_conn))

        ## check for not exist
        self.assertFalse(UserManager.existsID(self.testuser1_id-1, db_conn = self.db_conn))

    def test_existsEmail(self):
        ## check for exist
        self.assertTrue(UserManager.existsEmail("peter.selie@abc.com", db_conn = self.db_conn))

        ## check for not exist
        self.assertFalse(UserManager.existsEmail("peter.selie@def.com", db_conn = self.db_conn))

    def test_getUserFromID(self):
        ## retrieve user
        user = UserManager.getUserFromID(self.testuser1_id, db_conn = self.db_conn)
        self.assertEqual(user.userid, self.testuser1_id)
        self.assertEqual(user.fname, 'Peter')
        self.assertEqual(user.lname, 'Selie')
        self.assertEqual(user.email, 'peter.selie@abc.com')

    def test_getUserFromEmail(self):
        ## retrieve user
        user = UserManager.getUserFromEmail('peter.selie@abc.com', db_conn = self.db_conn)
        self.assertEqual(user.userid, self.testuser1_id)
        self.assertEqual(user.fname, 'Peter')
        self.assertEqual(user.lname, 'Selie')
        self.assertEqual(user.email, 'peter.selie@abc.com')

    def test_createUser(self):
        ## create user
        userid = UserManager.createUser('abcdef123@xyz.com', 'password123', 'abc', 'def', False, db_conn = self.db_conn)

        ## check if user exists
        self.cur.execute("SELECT EXISTS (SELECT * FROM SYSTEM.user_accounts WHERE userid = %s AND fname = %s AND lname = %s AND email = %s);", [userid, 'abc', 'def', 'abcdef123@xyz.com'])
        result = self.cur.fetchone()[0]

        self.assertTrue(result)

        ## destroy user
        self.cur.execute("DELETE FROM SYSTEM.user_accounts WHERE userid = %s;", [userid])
        self.db_conn.commit()


    def test_destroyUser(self):
        self.cur.execute("INSERT INTO SYSTEM.user_accounts(fname, lname, email, passwd) VALUES ('Jan', 'Met de pet', 'Jan.met.de.pet@abc.com', %s) RETURNING userid;", [sha256_crypt.hash("password123")])
        self.db_conn.commit()

        userid = int(self.cur.fetchone()[0])

        UserManager.destroyUser(userid, db_conn = self.db_conn)

        self.cur.execute("SELECT EXISTS (SELECT * FROM SYSTEM.user_accounts WHERE userid = %s);", [userid])
        result = self.cur.fetchone()[0]

        self.assertFalse(result)

    def test_getAllUsers(self):

        users = UserManager.getAllUsers(db_conn = self.db_conn)
        
        userids = [user.userid for user in users]

        self.assertTrue(self.testuser1_id in userids)
        self.assertTrue(self.testuser2_id in userids)
        self.assertFalse((self.testuser1_id-1) in userids)

    def test_updateUserActivity(self):
        ## test if testuser1 is active
        # --> true
        self.cur.execute("SELECT active FROM SYSTEM.user_accounts WHERE userid = %s;", [self.testuser1_id])
        result = self.cur.fetchone()[0]
        self.assertTrue(result)

        ## set testuser1 to inactive
        UserManager.updateUserActivity(self.testuser1_id, False, db_conn = self.db_conn)

        ## test if testuser1 is active
        # --> false
        self.cur.execute("SELECT active FROM SYSTEM.user_accounts WHERE userid = %s;", [self.testuser1_id])
        result = self.cur.fetchone()[0]
        self.assertFalse(result)

        ## set testuser1 to active
        UserManager.updateUserActivity(self.testuser1_id, True, db_conn = self.db_conn)

        ## test if testuser1 is active
        # --> true
        self.cur.execute("SELECT active FROM SYSTEM.user_accounts WHERE userid = %s;", [self.testuser1_id])
        result = self.cur.fetchone()[0]
        self.assertTrue(result)

    def test_editUserInfo(self):
        ## edit info of testuser2

        UserManager.editUserInfo(self.testuser2_id, "Luke", "Skywalker", "luke.skywalker@holo.net", db_conn = self.db_conn)

        ## retrieve information
        self.cur.execute("SELECT * FROM SYSTEM.user_accounts WHERE userid = %s;", [self.testuser2_id])
        result = self.cur.fetchone()

        # Format: (userid, fname, lname, email, passwd, registerdate, admin, active)

        self.assertEqual(result[1], "Luke")
        self.assertEqual(result[2], "Skywalker")
        self.assertEqual(result[3], "luke.skywalker@holo.net")

    def test_editUserPass(self):
        ## set new password
        new_pass = "123abcd456"

        UserManager.editUserPass(self.testuser2_id, new_pass, db_conn = self.db_conn)

        ## retrieve password
        self.cur.execute("SELECT * FROM SYSTEM.user_accounts WHERE userid = %s;", [self.testuser2_id])
        result = self.cur.fetchone()

        ## check if the passwords match
        retrieved_passwd_hash = result[4]

        self.assertTrue(sha256_crypt.verify(new_pass, retrieved_passwd_hash))

    def test_editAdminStatus_promote(self):
        # update status
        UserManager.editAdminStatus(cls.testuser3_id, True, db_conn = self.db_conn)

        self.cur.execute("SELECT admin FROM SYSTEM.user_accounts WHERE userid = %s;", [self.testuser3_id])
        result = self.cur.fetchone()[0]

        self.assertTrue(result)

    def test_editAdminStatus_demote(self):
        UserManager.editAdminStatus(cls.testuser4_id, True, db_conn = self.db_conn)

        self.cur.execute("SELECT * FROM SYSTEM.user_accounts WHERE userid = %s;", [self.testuser4_id])
        result = self.cur.fetchone()[0]

        self.assertFalse(result)
# ENDCLASS

