# this file contains the code for the database wrapper.
import psycopg2

class DBWrapper:
    """Raw wrapper class that encapsulates the connection and provides a cursor."""

    def __init__(self, dbname, username, hostname, password):
        self.conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(dbname, username, hostname, password))
        self.cur = self.conn.cursor()

    def commit(self):
        """Commit to the connection."""
        self.conn.commit()

    def cursor(self, name=None, cursor_factory=None, scrollable=None, withhold=False):
        """Retrieve the cursor. This is guaranteed to always return the
        same cursor object."""
        return self.cur

    def dict_cursor(self):
        """Return a python dictionary style cursor."""
        return self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def rollback(self):
        """Perform a rollback on the internal database connection"""
        self.conn.rollback()

    def close(self):
        """Close the connection."""
        self.cur.close()
        self.conn.close()
