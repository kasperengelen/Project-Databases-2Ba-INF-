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

    def cursor(self):
        """Retrieve the cursor. This is guaranteed to always return the
        same cursor object."""
        return self.cur

    def rollback(self):
        """Perform a rollback on the internal database connection"""
        self.conn.rollback()

    def close(self):
        """Close the connection."""
        self.cur.close()
        self.conn.close()
