import sys, os
sys.path.insert(0, "../Controller/")
sys.path.insert(0, "../Model/")
sys.path.insert(0, "../View")
sys.path.insert(0, "..")
import unittest
from UserManager import UserManager
from DatabaseConfiguration import DatabaseConfiguration

from passlib.hash import sha256_crypt

class TestUserManager(unittest.TestCase):
    """Tests for the UserManager class."""

    @classmethod
    def setUpClass(cls):
        """Set up the test environment."""
        db_conn = DatabaseConfiguration.get_db()
        engine = DatabaseConfiguration.get_engine()

    @classmethod
    def tearDownClass(cls):
        """Clean up the test environment."""
        db_conn.close()

    def test_validateLogin(self):
        ## insert user
        # determine password hash
        passwd_hash = sha256_crypt.hash("password123")
        db_conn.cursor().execute("INSERT INTO SYSTEM.user_accounts(fname, lname, email, passwd) VALUES ('Peter', 'Selie', 'peter.selie@abc.com', %s);", [passwd_hash])
        db_conn.commit()

        ## do login that should succeed
        result = UserManager.validateLogin("peter.selie@abc.com", "password123")
        self.assertTrue(result)

        ## do login that should fail
        # password incorrect
        result = UserManager.validateLogin("peter.selie@abc.com", "jrgjeingerg454")
        self.assertFalse(result)

        # email not correct
        result = UserManager.validateLogin("peter.selie@def.com", "password123")
        self.assertFalse(result)

        ## cleanup
        db_conn.cursor().execute("DELETE FROM SYSTEM.user_accounts WHERE email='peter.selie@abc.com';")
        db_conn.commit()

    def test_existID(self):
        pass

    def test_existsEmail(self):
        pass

    def test_getUserFromID(self):
        pass

    def test_getUserFromEmail(self):
        pass

    def test_createUser(self):
        pass

    def test_destroyUser(self):
        pass

    def test_getAllUsers(self):
        pass

    def test_updateUserActivity(self):
        pass
