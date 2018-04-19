
from utils import get_db
from passlib.hash import sha256_crypt
from Controller.UserInfo import UserInfo

class UserManager:
    """Class that provides facilities to manage the userbase."""

    @staticmethod
    def validateLogin(email, password_candidate, db_conn = None):
        """Determine if the specified email and password combination
        corresponds to a valid user."""
        
        if db_conn is None:
            db_conn = get_db()

        db_conn.cursor().execute("SELECT * FROM SYSTEM.user_accounts WHERE email=%s;", [email])
        result = db_conn.cursor().fetchone()

        if result is None:
            return False # email does not exist

        correct_password = result[4]

        if not sha256_crypt.verify(password_candidate, correct_password):
            return False # password does not match

        return True
    # ENDMETHOD

    @staticmethod
    def existsID(userid, db_conn = None):
        """Determine if there exists a user with the specified
        user ID."""

        if db_conn is None:
            db_conn = get_db()
        
        db_conn.cursor().execute("SELECT * FROM SYSTEM.user_accounts WHERE userid=%s;", [userid])
        result = db_conn.cursor().fetchone()

        return result is not None # an entry must exist
    # ENDMETHOD

    @staticmethod
    def existsEmail(email, db_conn = None):
        """Determine if there exists a user with the specified
        email."""
        
        if db_conn is None:
            db_conn = get_db()

        db_conn.cursor().execute("SELECT * FROM SYSTEM.user_accounts WHERE email=%s;", [email])
        result = db_conn.cursor().fetchone()

        return result is not None # an entry must exist
    # ENDMETHOD

    @staticmethod
    def getUserFromID(userid, db_conn = None):
        """Retrieve a UserInfo object that contains
        information about the user with the specified id."""
        
        if db_conn is None:
            db_conn = get_db()

        if not UserManager.existsID(userid, db_conn = db_conn):
            raise RuntimeError("User with specified does not exist.")

        db_conn.cursor().execute("SELECT * FROM SYSTEM.user_accounts WHERE userid=%s;", [userid])
        result = db_conn.cursor().fetchone()

        return UserInfo.fromSqlTuple(result)
    # ENDMETHOD

    @staticmethod
    def getUserFromEmail(email, db_conn = None):
        """Retrieve a UserInfo object that contains
        information about the user with the specified email."""
        
        if db_conn is None:
            db_conn = get_db()

        db_conn.cursor().execute("SELECT * FROM SYSTEM.user_accounts WHERE email=%s;", [email])
        result = db_conn.cursor().fetchone()

        return UserInfo.fromSqlTuple(result)
    # ENDMETHOD

    @staticmethod
    def createUser(email, password, fname, lname, admin, db_conn = None):
        """Create a user with the specified parameters. The user
        will be inserted into the database. If a user with the
        specified email already exists, RuntimeError will be raised.

        The userid of the created user will be returned."""
        
        if db_conn is None:
            db_conn = get_db()

        if UserManager.existsEmail(email, db_conn = db_conn):
            raise RuntimeError("User already exists.")

        password_hash = sha256_crypt.hash(password)

        db_conn.cursor().execute("INSERT INTO SYSTEM.user_accounts(fname, lname, email, passwd) VALUES (%s, %s, %s, %s) RETURNING userid;", [fname, lname, email, password_hash])
        db_conn.commit()

        userid = db_conn.cursor().fetchone()
        return userid
    # ENDMETHOD

    @staticmethod
    def destroyUser(userid, db_conn = None):
        """Removes the user with the specified user id from the system."""

        if db_conn is None:
            db_conn = get_db()

        if not UserManager.existsID(userid, db_conn = db_conn):
            raise RuntimeError("User with specified userid does not exists.")

        db_conn.cursor().execute("DELETE FROM SYSTEM.user_accounts WHERE userid=%s;", [userid])
        db_conn.commit()
    # ENDMETHOD

    @staticmethod
    def getAllUsers(db_conn = None):
        """Retrieve a list of UserInfo objects representing all users."""

        if db_conn is None:
            db_conn = get_db()

        db_conn.cursor().execute("SELECT * FROM SYSTEM.user_accounts;")
        results = db_conn.cursor().fetchall()

        retval = []

        for result in results:
            retval.append(UserInfo.fromSqlTuple(result))

        return retval
    # ENDMETHOD

    @staticmethod
    def updateUserActivity(userid, active, db_conn = None):
        """Set the user as active/inactive."""

        if db_conn is None:
            db_conn = get_db()

        if not UserManager.existsID(userid, db_conn = db_conn):
            raise RuntimeError("User with specified userid does not exists.")

        db_conn.cursor().execute("UPDATE SYSTEM.user_accounts SET active=%s WHERE userid=%s", [bool(active), int(userid)])
        db_conn.commit()
    # ENDMETHOD

    @staticmethod
    def editUserInfo(userid, new_fname, new_lname, new_email, db_conn = None):
        """Set the information about the specified user to the new
        specified values."""

        if db_conn is None:
            db_conn = get_db()

        if not UserManager.existsID(userid, db_conn = db_conn):
            raise RuntimeError("User with specified userid does not exists.")
        
        db_conn.cursor().execute("UPDATE SYSTEM.user_accounts SET fname = %s, lname = %s, email=%s WHERE userid=%s;", 
                                                                                                [new_fname, new_lname, new_email, int(userid)])
        db_conn.commit()
    # ENDMETHOD

    @staticmethod
    def editUserPass(userid, new_pass, db_conn = None):
        """Given the user-inputted password (not yet hashed!), this will update the user's
        password to the specified password."""

        if db_conn is None:
            db_conn = get_db()

        passwd_hash = sha256_crypt.hash(new_pass)

        db_conn.cursor().execute("UPDATE SYSTEM.user_accounts SET passwd = %s WHERE userid=%s;", [passwd_hash, int(userid)])
        db_conn.commit()
    # ENDMETHOD

# ENDCLASS