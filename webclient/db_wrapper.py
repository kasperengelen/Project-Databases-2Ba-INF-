# this file contains the code for the database wrapper.
import psycopg2


class DBConnection:
    """Class that represents a DB connection. The class implements
    a context manager that automatically opens and closes a DB connection.
    USAGE: just do "with DBConnection() as db_connection:" and you can use the 
    connection in the enclosing scope."""

    def __enter__(self):
        # open connection
        self.__connection = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format("projectdb18", "dbadmin", "localhost", "AdminPass123"))

        return DBConnectionWrapper(self.__connection, self.__connection.cursor())

    def __exit__(self, exc_type, exc_value, traceback):
        # terminate connection
        self.__connection.cursor().close()
        self.__connection.close()


class DBConnectionWrapper:
    """Internal wrapper class that wraps a cursor and a connection into one."""

    def __init__(self, connection, cursor):
        self.__cursor = cursor
        self.__connection = connection

    def cursor(self):
        return self.__cursor

    def commit(self):
        """Commit changes, for the syntax please refer to the psycopg2 docs."""
        self.__connection.commit()

    # add extra wrapper methods here

class DBWrapper:
    """Raw wrapper class that encapsulates the connection and provides a cursor."""

    def __init__(self):
        self.conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format("projectdb18", "dbadmin", "localhost", "AdminPass123"))
        self.cur = self.conn.cursor()

    def commit(self):
        """Commit to the connection."""
        self.conn.commit()

    def cursor(self):
        """Retrieve the cursor. This is guaranteed to always return the
        same cursor object."""
        return self.cur

    def close(self):
        """Close the connection."""
        self.cur.close()
        self.conn.close()
