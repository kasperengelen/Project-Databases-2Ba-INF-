# file for various utility functions

from wtforms.validators import ValidationError
from flask import g, abort, session
import psycopg2
from functools import wraps
import re

def get_db():
    """Retrieve the DB connection."""
    return g.db_conn

def get_sqla_eng():
    """Retrieve the SQLA engine."""
    return g.sqla_engine

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

def sync_user_info():
    userid = session['userdata']['userid']
    print(userid)

    session['userdata'] = UserManager.getUserFromID(userid).toDict()


class EnumCheck:
    def __init__(self, message="", choises=[]):
        self.message = message
        self.choises = choises

    def __call__(self, form, field):
        if field.data not in self.choises:
            raise ValidationError(self.message)

class FilenameCheck:
    def __init__(self, message="", regex=""):
        self.message=message
        self.regex = re.compile(regex)

    def __call__(self, form, field):
        if not self.regex.fullmatch(field.data.filename):
            raise ValidationError(self.message)

class Logger:
    @staticmethod
    def log(message):
        print("[ERROR] " + message)


class LoginManager:
    @staticmethod
    def setLoggedIn(user):
        session['loggedin'] = True
        session['userdata'] = user.toDict()
    # ENDFUNCTION

    @staticmethod
    def setLoggedOut():
        session['loggedin'] = False
        session['userdata'] = None
    # ENDFUNCTION

# SOURCE: https://docs.python.org/2/library/functools.html
def require_login(func):
    """Wrapper that makes functions only accessible if
    the user is loggedn in."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        if 'loggedin' in session and session['loggedin']:
            return func(*args, **kwargs)
        else:
            abort(403)
    return wrapper
# ENDFUNCTION

def require_admin(func):
    """Wrapper that makes functions only accessible if 
    the user is logged in and if the user is a site-admin."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        if 'userdata' in session and session['userdata'] is not None and session['userdata']['admin']:
            return func(*args, **kwargs)
        else:
            abort(403)

    return wrapper
# ENDFUNCTION

from UserManager import UserManager
