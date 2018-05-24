
from Model.db_access import get_db
from passlib.hash import sha256_crypt
from Controller.UserInfo import UserInfo
from Model.QueryManager import QueryManager

class UserManager:
    """Class that provides facilities to manage the userbase."""

    @staticmethod
    def validateLogin(email, password_candidate, db_conn = None):
        """Determine if the specified email and password combination
        corresponds to a valid user."""
        
        if db_conn is None:
            db_conn = get_db()

        qm = QueryManager(db_conn = db_conn, engine = None)
        result = qm.getUser(email=email)

        if not result:
            return False # email does not exist

        correct_password = result[0]['passwd']

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

        qm = QueryManager(db_conn = db_conn, engine=None)
        result = qm.getUser(userid=userid)

        return len(result) > 0
    # ENDMETHOD

    @staticmethod
    def existsEmail(email, db_conn = None):
        """Determine if there exists a user with the specified
        email."""
        
        if db_conn is None:
            db_conn = get_db()

        qm = QueryManager(db_conn = db_conn, engine=None)
        result = qm.getUser(email=email)

        return len(result) > 0
    # ENDMETHOD

    @staticmethod
    def getUserFromID(userid, db_conn = None):
        """Retrieve a UserInfo object that contains
        information about the user with the specified id.
        If there is no user with the specified ID, None will be returned."""
        
        if db_conn is None:
            db_conn = get_db()

        qm = QueryManager(db_conn = db_conn, engine=None)
        result = qm.getUser(userid=userid)

        if not result:
            return None

        return UserInfo(userid        = result[0]['userid'],
                        fname         = result[0]['fname'],
                        lname         = result[0]['lname'],
                        email         = result[0]['email'],
                        register_date = result[0]['register_date'],
                        admin         = result[0]['admin'],
                        active        = result[0]['active'])
    # ENDMETHOD

    @staticmethod
    def getUserFromEmail(email, db_conn = None):
        """Retrieve a UserInfo object that contains
        information about the user with the specified email.
        If there is no user with the specified email, None will be returned."""
        
        if db_conn is None:
            db_conn = get_db()

        qm = QueryManager(db_conn = db_conn, engine=None)
        result = qm.getUser(email=email)

        if not result:
            return None

        return UserInfo(userid        = result[0]['userid'],
                        fname         = result[0]['fname'],
                        lname         = result[0]['lname'],
                        email         = result[0]['email'],
                        register_date = result[0]['register_date'],
                        admin         = result[0]['admin'],
                        active        = result[0]['active'])
    # ENDMETHOD

    @staticmethod
    def createUser(email, password, fname, lname, admin, db_conn = None):
        """Create a user with the specified parameters. The user
        will be inserted into the database.

        The userid of the created user will be returned."""
        
        if db_conn is None:
            db_conn = get_db()

        password_hash = sha256_crypt.hash(password)

        qm = QueryManager(db_conn = db_conn, engine = None)
        userid = qm.insertUser(email = email, passwd = password_hash, fname = fname, lname = lname, admin = admin, returning = 'userid')

        return userid
    # ENDMETHOD

    @staticmethod
    def destroyUser(userid, db_conn = None):
        """Removes the user with the specified user id from the system."""

        if db_conn is None:
            db_conn = get_db()

        qm = QueryManager(db_conn = db_conn, engine = None)
        qm.deleteUser(userid=userid)

    # ENDMETHOD

    @staticmethod
    def getAllUsers(db_conn = None):
        """Retrieve a list of UserInfo objects representing all users."""

        if db_conn is None:
            db_conn = get_db()
            
        qm = QueryManager(db_conn = db_conn, engine = None)
        results = qm.getUser() 

        retval = []

        for result in results:
            retval.append(UserInfo(userid        = result['userid'],
                                   fname         = result['fname'],
                                   lname         = result['lname'],
                                   email         = result['email'],
                                   register_date = result['register_date'],
                                   admin         = result['admin'],
                                   active        = result['active']))
        return retval
    # ENDMETHOD

    @staticmethod
    def updateUserActivity(userid, active, db_conn = None):
        """Set the user as active/inactive."""

        if db_conn is None:
            db_conn = get_db()

        qm = QueryManager(db_conn = db_conn, engine = None)
        qm.updateUser(reqs={'userid': userid}, sets={'active': active})
    # ENDMETHOD

    @staticmethod
    def editUserInfo(userid, new_fname, new_lname, new_email, db_conn = None):
        """Set the information about the specified user to the new
        specified values."""

        if db_conn is None:
            db_conn = get_db()

        qm = QueryManager(db_conn = db_conn, engine = None)
        qm.updateUser(reqs={'userid': userid}, sets={'fname': new_fname, 'lname': new_lname, 'email': new_email})
    # ENDMETHOD

    @staticmethod
    def editUserPass(userid, new_pass, db_conn = None):
        """Given the user-inputted password (not yet hashed!), this will update the user's
        password to the specified password."""

        if db_conn is None:
            db_conn = get_db()

        passwd_hash = sha256_crypt.hash(new_pass)

        qm = QueryManager(db_conn = db_conn, engine = None)
        qm.updateUser(reqs={'userid': userid}, sets={'passwd': passwd_hash})
    # ENDMETHOD

    @staticmethod
    def editAdminStatus(userid, admin_status, db_conn = None):
        """Given True of False as the new admin status, this will update the current status to the new status."""

        if db_conn is None:
            db_conn = get_db()

        qm = QueryManager(db_conn = db_conn, engine = None)
        qm.updateUser(reqs={'userid': userid}, sets={'admin': admin_status})
    # ENDMETHOD
# ENDCLASS