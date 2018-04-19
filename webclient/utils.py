# file for various utility functions

from wtforms.validators import ValidationError
from flask import g
import re

def get_db():
    """Retrieve the DB connection."""
    return g.db_conn

def get_sqla_eng():
    """Retrieve the SQLA engine."""
    return g.sqla_engine

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
