from Model.db_access import get_db

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

def sql_time_to_dict(sql_date_string):
    """Given a string of the format "YYYY:MM:DD HH:MM:SS.SSSSSS" this
    returns a dict containing the same data under the keys 'Y', 'M', 'D', 'hr', 'min', 'sec', 'sec_full'.
    With 'sec' containing the seconds rounded to an integer, and 'sec_full' containing the full original value."""

    sql_date_string = str(sql_date_string)

    date = sql_date_string.split(' ')[0] # split on space between date and time
    time = sql_date_string.split(' ')[1]

    year = int(date.split('-')[0])
    month = int(date.split('-')[1])
    day = int(date.split('-')[2])

    hour = int(time.split(':')[0])
    minute = int(time.split(':')[1])
    sec_full = float(time.split(':')[2])
    sec = int(sec_full)

    return {
        "Y": year,
        "M": month,
        "D": day,

        "hr": hour,
        "min": minute,
        "sec": sec,
        "sec_full": sec_full
    }
# ENDFUNCTION