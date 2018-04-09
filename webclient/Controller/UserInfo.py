from utils import sql_time_to_dict
from utils import get_db
from passlib.hash import sha256_crypt

class UserInfo:
    """Class that represents a user."""
    
    @staticmethod
    def fromSqlTuple(tupl):
        """Convert a SQL-tuple containing information about a user
        to a UserInfo object."""

        userid = int(tupl[0])
        fname = str(tupl[1])
        lname = str(tupl[2])
        email = str(tupl[3])
        register_date = sql_time_to_dict(tupl[5])
        admin = bool(tupl[6])
        active = bool(tupl[7])

        return UserInfo(userid, fname, lname, email, register_date, admin, active)
    # ENDMETHOD

    def __init__(self, userid, fname, lname, email, register_date, admin, active):
        """Construct an object that represents a user with the
        specified parameters.

        The register_date parameter is a dict with the following entries:
            'Y': year
            'M': month
            'D': day
            'hr': hour
            'min': minutes
            'sec': An integer representing the seconds.
            'sec_full': The floating point number representing the seconds.
        """
        
        self.userid = userid
        self.fname = fname
        self.lname = lname
        self.email = email
        self.register_date = register_date
        self.admin = admin
        self.active = active
    # ENDMETHOD

    def editInfo(self, new_email, new_fname, new_lname, new_pass):
        """Edit the user information. The information will also be updated in the database."""

        passwd_hash = sha256_crypt.hash(new_pass)

        # update DB
        get_db().cursor().execute("UPDATE SYSTEM.user_accounts SET fname=%s, lname=%s, email=%s, passwd=%s WHERE userid=%s", 
                                                                        [new_fname, new_lname, new_email, passwd_hash, self.userid])
        get_db().commit()

         # update values
        new = UserManager.UserManager.getUserFromID(self.userid)
        self.fname = new.fname
        self.lname = new.lname
        self.email = new.email
    # ENDMETHOD

    def editInfoNoPass(self, new_email, new_fname, new_lname):
        """Edit the user information except the password."""

        get_db().cursor().execute("UPDATE SYSTEM.user_accounts SET fname=%s, lname=%s, email=%s WHERE userid=%s", 
                                                                        [new_fname, new_lname, new_email, self.userid])
        get_db().commit()

        # update values
        new = UserManager.UserManager.getUserFromID(self.userid)
        self.fname = new.fname
        self.lname = new.lname
        self.email = new.email

    # ENDMETHOD

    def toDict(self):
        """Retrieve a JSON-compatible dict
        that contains information about the user."""
        
        return {
            "userid": self.userid,
            "email": self.email,
            "firstname": self.fname,
            "lastname": self.lname,
            "register_date": self.register_date,
            "admin": self.admin,
            "active" : self.active
        }
    # ENDMETHOD

import UserManager