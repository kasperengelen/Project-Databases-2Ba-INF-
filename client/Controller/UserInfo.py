from Model.db_access import get_db
from Model.SQLTypeHandler import SQLTypeHandler
import datetime

class UserInfo:
    """Class that represents a user."""
    
    def __init__(self, userid, fname, lname, email, register_date, admin, active):
        """Construct an object that represents a user with the
        specified parameters.

        The register_date parameter is a datetime.datetime object.
        """
        
        self.userid = userid
        self.fname = fname
        self.lname = lname
        self.email = email
        self.register_date = register_date
        self.admin = admin
        self.active = active
    # ENDMETHOD

    def toDict(self):
        """Retrieve a JSON-compatible dict
        that contains information about the user.
        
        Note: The register date is specified as a dd-mm-yy string. The register
        time is specified as an hr:min:sec string.
        """
        
        return {
            "userid": self.userid,
            "email": self.email,
            "firstname": self.fname,
            "lastname": self.lname,
            "register_date": self.register_date.strftime("%d-%m-%Y"),
            "register_time": self.register_date.strftime("%H:%M:%S"),
            "admin": self.admin,
            "active" : self.active
        }
    # ENDMETHOD
# ENDFUNCTION