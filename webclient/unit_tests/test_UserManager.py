import unittest
from Controller.UserManager import UserManager
from Model.DatabaseConfiguration import DatabaseConfiguration

from passlib.hash import sha256_crypt

class TestUserManager(unittest.TestCase):
    """Tests for the UserManager class."""

    @classmethod
    def setUpClass(cls):
        """Set up the test environment."""
        cls.db_conn = DatabaseConfiguration().get_db()
        cls.engine = DatabaseConfiguration().get_engine()

        cls.db_conn.cursor().execute("DELETE FROM SYSTEM.user_accounts WHERE TRUE;");
        cls.db_conn.commit()

        cls.db_conn.cursor().execute("INSERT INTO SYSTEM.user_accounts(fname, lname, email, passwd) VALUES ('Peter', 'Selie', 'peter.selie@abc.com', %s) RETURNING userid;", [sha256_crypt.hash("password123")])
        cls.db_conn.commit()

        cls.testuser1_id = int(cls.db_conn.cursor().fetchall()[0][0])

        cls.db_conn.cursor().execute("INSERT INTO SYSTEM.user_accounts(fname, lname, email, passwd) VALUES ('Piet', 'Klaas', 'piet.klaas@abc.com', %s) RETURNING userid;", [sha256_crypt.hash("password123")])
        cls.db_conn.commit()

        cls.testuser2_id = int(cls.db_conn.cursor().fetchall()[0][0])

    @classmethod
    def tearDownClass(cls):
        """Clean up the test environment."""
        cls.db_conn.cursor().execute("DELETE FROM SYSTEM.user_accounts WHERE email='peter.selie@abc.com';")
        cls.db_conn.commit()
        cls.db_conn.cursor().execute("DELETE FROM SYSTEM.user_accounts WHERE TRUE;");
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
        self.db_conn.cursor().execute("SELECT EXISTS (SELECT * FROM SYSTEM.user_accounts WHERE userid = %s AND fname = %s AND lname = %s AND email = %s);", [userid, 'abc', 'def', 'abcdef123@xyz.com'])
        result = self.db_conn.cursor().fetchone()[0]

        self.assertTrue(result)

        ## destroy user
        self.db_conn.cursor().execute("DELETE FROM SYSTEM.user_accounts WHERE userid = %s;", [userid])
        self.db_conn.commit()


    def test_destroyUser(self):
        self.db_conn.cursor().execute("INSERT INTO SYSTEM.user_accounts(fname, lname, email, passwd) VALUES ('Jan', 'Met de pet', 'Jan.met.de.pet@abc.com', %s) RETURNING userid;", [sha256_crypt.hash("password123")])
        self.db_conn.commit()

        userid = int(self.db_conn.cursor().fetchall()[0][0])

        UserManager.destroyUser(userid, db_conn = self.db_conn)

        self.db_conn.cursor().execute("SELECT EXISTS (SELECT * FROM SYSTEM.user_accounts WHERE userid = %s);", [userid])
        result = self.db_conn.cursor().fetchone()[0]

        self.assertFalse(result)

    def test_getAllUsers(self):

        users = UserManager.getAllUsers(db_conn = self.db_conn)
        
        userids = [user.userid for user in users]

        self.assertTrue(self.testuser1_id in userids)
        self.assertTrue(self.testuser2_id in userids)
        self.assertFalse((self.testuser1_id-1) in userids)

    def test_updateUserActivity(self):
      pass
