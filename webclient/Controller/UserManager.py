
from utils import get_db
from passlib.hash import sha256_crypt
from UserInfo import UserInfo
from utils import sql_time_to_dict

class UserManager:
    """Class that provides facilities to manage the userbase."""

    @staticmethod
    def validateLogin(email, password_candidate):
        """Determine if the specified email and password combination
        corresponds to a valid user."""
        
        get_db().cursor().execute("SELECT * FROM SYSTEM.user_accounts WHERE email=%s;", [email])
        result = get_db().cursor().fetchone()

        if result is None:
            return False # email does not exist

        correct_password = result[4]

        if not sha256_crypt.verify(password_candidate, correct_password):
            return False # password does not match

        return True
    # ENDMETHOD

    @staticmethod
    def existsID(userid):
        """Determine if there exists a user with the specified
        user ID."""
        
        get_db().cursor().execute("SELECT * FROM SYSTEM.user_accounts WHERE userid=%s;", [userid])
        result = get_db().cursor().fetchone()

        return result is not None # an entry must exist
    # ENDMETHOD

    @staticmethod
    def existsEmail(email):
        """Determine if there exists a user with the specified
        email."""
        
        get_db().cursor().execute("SELECT * FROM SYSTEM.user_accounts WHERE email=%s;", [email])
        result = get_db().cursor().fetchone()

        return result is not None # an entry must exist
    # ENDMETHOD

    @staticmethod
    def getUserFromID(userid):
        """Retrieve a UserInfo object that contains
        information about the user with the specified id."""
        
        get_db().cursor().execute("SELECT * FROM SYSTEM.user_accounts WHERE userid=%s;", [userid])
        result = get_db().cursor().fetchone()

        return UserInfo.fromSqlTuple(result)
    # ENDMETHOD

    @staticmethod
    def getUserFromEmail(email):
        """Retrieve a UserInfo object that contains
        information about the user with the specified email."""
        
        get_db().cursor().execute("SELECT * FROM SYSTEM.user_accounts WHERE email=%s;", [email])
        result = get_db().cursor().fetchone()

        return UserInfo.fromSqlTuple(result)
    # ENDMETHOD

    @staticmethod
    def createUser(email, password, fname, lname, admin):
        """Create a user with the specified parameters. The user
        will be inserted into the database. If a user with the
        specified email already exists, RuntimeError will be raised.

        The userid of the created user will be returned."""
        
        if UserManager.existsEmail(email):
            raise RuntimeError("User already exists.")

        password_hash = sha256_crypt.hash(password)

        get_db().cursor().execute("INSERT INTO SYSTEM.user_accounts(fname, lname, email, passwd) VALUES (%s, %s, %s, %s) RETURNING userid;", [fname, lname, email, password_hash])
        get_db().commit()

        userid = get_db().cursor().fetchone()
        return userid
    # ENDMETHOD

    @staticmethod
    def destroyUser(userid):
        """Removes the user with the specified user id from the system."""

        if not UserManager.existsID(userid):
            raise RuntimeError("User with specified userid does not exists.")

        get_db().cursor().execute("DELETE FROM SYSTEM.user_accounts WHERE userid=%s CASCADE;", [userid])
        get_db().commit()
    # ENDMETHOD

    @staticmethod
    def getAllUsers():
        """Retrieve a list of UserInfo objects representing all users."""

        get_db().cursor().execute("SELECT * FROM SYSTEM.user_accounts;")
        results = get_db().cursor().fetchall()

        retval = []

        for result in results:
            retval.append(UserInfo.fromSqlTuple(result))

        return retval
    # ENDMETHOD
# ENDCLASS