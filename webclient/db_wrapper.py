# this file contains the code for the database wrapper.
import psycopg2


class DBConnection:
    """Class that represents a DB connection. The class implements
    a context manager that automatically opens and closes a DB connection.
    USAGE: just do "with DBConnection() as db_connection:" and you can use the 
    connection in the enclosing scope."""

    def __enter__(self):
        # open connection
        self.__connection = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format("ProjectDB18",
                                                                                                       "dbadmin",
                                                                                                       "localhost",
                                                                                                       "AdminPass123"))

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

    def commit():
        """Commit changes, for the syntax please refer to the psycopg2 docs."""
        self.__connection.commit()

    # add extra wrapper methods here
