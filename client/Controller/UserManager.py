
from Model.db_access import get_db
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

        cur = db_conn.cursor()
        cur.execute("SELECT * FROM SYSTEM.user_accounts WHERE email=%s;", [email])
        result = cur.fetchone()

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
        
        cur = db_conn.cursor()
        cur.execute("SELECT * FROM SYSTEM.user_accounts WHERE userid=%s;", [userid])
        result = cur.fetchone()

        return result is not None # an entry must exist
    # ENDMETHOD

    @staticmethod
    def existsEmail(email, db_conn = None):
        """Determine if there exists a user with the specified
        email."""
        
        if db_conn is None:
            db_conn = get_db()

        cur = db_conn.cursor()
        cur.execute("SELECT * FROM SYSTEM.user_accounts WHERE email=%s;", [email])
        result = cur.fetchone()

        return result is not None # an entry must exist
    # ENDMETHOD

    @staticmethod
    def getUserFromID(userid, db_conn = None):
        """Retrieve a UserInfo object that contains
        information about the user with the specified id.
        If there is no user with the specified ID, None will be returned."""
        
        if db_conn is None:
            db_conn = get_db()

        cur = db_conn.cursor()
        cur.execute("SELECT * FROM SYSTEM.user_accounts WHERE userid=%s;", [userid])
        result = cur.fetchone()

        if result is None:
            return None

        return UserInfo.fromSqlTuple(result)
    # ENDMETHOD

    @staticmethod
    def getUserFromEmail(email, db_conn = None):
        """Retrieve a UserInfo object that contains
        information about the user with the specified email.
        If there is no user with the specified email, None will be returned."""
        
        if db_conn is None:
            db_conn = get_db()

        cur = db_conn.cursor()
        cur.execute("SELECT * FROM SYSTEM.user_accounts WHERE email=%s;", [email])
        result = cur.fetchone()

        if result is None:
            return None

        return UserInfo.fromSqlTuple(result)
    # ENDMETHOD

    @staticmethod
    def createUser(email, password, fname, lname, admin, db_conn = None):
        """Create a user with the specified parameters. The user
        will be inserted into the database.

        The userid of the created user will be returned."""
        
        if db_conn is None:
            db_conn = get_db()

        password_hash = sha256_crypt.hash(password)

        cur = db_conn.cursor()
        cur.execute("INSERT INTO SYSTEM.user_accounts(fname, lname, email, passwd) VALUES (%s, %s, %s, %s) RETURNING userid;", [fname, lname, email, password_hash])
        db_conn.commit()

        userid = cur.fetchone()
        return userid
    # ENDMETHOD

    @staticmethod
    def destroyUser(userid, db_conn = None):
        """Removes the user with the specified user id from the system."""

        if db_conn is None:
            db_conn = get_db()

        db_conn.cursor().execute("DELETE FROM SYSTEM.user_accounts WHERE userid=%s;", [userid])
        db_conn.commit()
    # ENDMETHOD

    @staticmethod
    def getAllUsers(db_conn = None):
        """Retrieve a list of UserInfo objects representing all users."""

        if db_conn is None:
            db_conn = get_db()

        cur = db_conn.cursor()
        cur.execute("SELECT * FROM SYSTEM.user_accounts;")
        results = cur.fetchall()

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

        db_conn.cursor().execute("UPDATE SYSTEM.user_accounts SET active=%s WHERE userid=%s", [bool(active), int(userid)])
        db_conn.commit()
    # ENDMETHOD

    @staticmethod
    def editUserInfo(userid, new_fname, new_lname, new_email, db_conn = None):
        """Set the information about the specified user to the new
        specified values."""

        if db_conn is None:
            db_conn = get_db()
        
        db_conn.cursor().execute("UPDATE SYSTEM.user_accounts SET fname = %s, lname = %s, email=%s WHERE userid=%s;", [new_fname, new_lname, new_email, int(userid)])
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

    @staticmethod
    def editAdminStatus(userid, admin_status, db_conn = None):
        """Given True of False as the new admin status, this will update the current status to the new status."""

        if db_conn is None:
            db_conn = get_db()

        db_conn.cursor().execute("UPDATE SYSTEM.user_accounts SET admin = %s WHERE userid = %s", [admin_status])
        db_conn.commit()
    # ENDMETHOD
# ENDCLASS