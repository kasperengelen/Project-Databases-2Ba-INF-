# this file contains the code for the database wrapper.
import psycopg2


class DBConnection:
    """Class that represents a DB connection. The class implements
    a context manager that automatically opens and closes a DB connection."""

    def __enter__(self):
        # open connection
        self.__connection = psycopg2.connect("dbname='{}', user='{}', host='{}', password='{}'".format("ProjectDB18",
                                                                                                       "DBAdmin",
                                                                                                       "localhost",
                                                                                                       "AdminPass123"))

        return DBConnectionWrapper(self.__connection, self.__connection.cursor)

    def __exit__(self):
        # terminate connection
        self.__connection.close()
        self.__connection.cursor.close()


class DBConnectionWrapper:
    """Internal wrapper class that wraps a cursor and a connection into one."""

    def __init__(self, cursor, connection):
        self.__cursor = cursor
        self.__connection = connection

    def execute(self, query, args=None):
        """Execute a query, for the syntax please refer to the psycopg2 docs."""
        self.__cursor.execute(query, args)

    def commit():
        """Commit changes, for the syntax please refer to the psycopg2 docs."""
        self.__connection.commit()

    # add extra wrapper methods here
