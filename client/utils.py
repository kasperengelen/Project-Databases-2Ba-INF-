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