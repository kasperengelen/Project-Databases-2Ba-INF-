import configparser
import io
import os
import psycopg2
import psycopg2.pool
from sqlalchemy import create_engine


class _DatabaseLoader:
    """Internal class that loads information about a database connection from a config file and creates all
    the necessary resources to make use of the DB."""


    class __InnerClass:
        def __init__(self, dbname, user, host, password, release):
            self.dbname = dbname
            self.user = user
            self.host = host
            self.password = password
            self.release = release
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(5, 20, dbname=dbname, user=user, password=password, host=host)
            self.engine = create_engine("postgresql://{}:{}@{}/{}".format(self.user, self.password, self.host, self.dbname),
                                         pool_size=20, max_overflow=0)

        def get_dbname(self):
            """Returns the database name as specified in the config file"""
            return self.dbname

        def get_user(self):
            """Returns the user of the database as specified in the config file"""
            return self.user

        def get_host(self):
            """Returns the host of the database as specified in the config file"""
            return self.host

        def get_password(self):
            """Returns the password to access the database as specified in the config file"""
            return self.password

        def get_packed_values(self):
            """Returns all the fields necessary to create a database connection in the form of a python list.
            This is done in this order= [database name, user, host, password]. This method might be extra helpful
            if the caller makes use of the unpacking operator "*".
            """
            return [self.dbname, self.user, self.host, self.password]

        def is_test_connection(self):
            """Returns a boolean indicating whether you're connected to the testing database."""
            return not self.release
        
        def get_db2(self):
            """Returns a psycopg2 database connection from the connection pool."""
            return self.connection_pool.getconn()

        def get_db(self):
            return psycopg2.connect(user=self.user, password=self.password, host=self.host, dbname=self.dbname)

        def get_engine(self):
            """Returns an SQL Alchemy engine."""
            return self.engine

        def close_connection(self, db_conn):
            """'Closes' a psycopg2 db connection, but in reality returns it to the connection pool.

            Parameters:
                db_conn: A reference to the connection you're trying to close (return to the pool).
            """
            self.connection_pool.putconn(db_conn)
            
            

    def __init__(self):
        if type(self) == _DatabaseLoader:
            raise Exception('DatabaseLoader should not be directly constructed. '
                            'Use DatabaseConfiguration or TestConnection instead.')

    def load_db(self, release):
        """Read database details and create the internal object and return it."""
        try: #Try opening the file and loading it
            #Get path without worrying about current working directory.
            if release is False:
                path = os.path.dirname(os.path.abspath(__file__)) + '/../test_config.ini'
            else:
                path = os.path.dirname(os.path.abspath(__file__)) + '/../config.ini'
                
            with open(path) as conf:
                db_config = conf.read()
        
        except(FileNotFoundError):
            raise FileNotFoundError("The config file config.ini was not found!"
                                    " Please add this file to allow proper configuration"
                                    " of the database connection.")
        
        config = configparser.RawConfigParser(allow_no_value=True)
        config.read_file(io.StringIO(db_config))

        #Extract content to store it the datamembers
        db_info = config.sections()[0] #The only relevant section is the first one
        values =  {}
        fields = ['db_name', 'user', 'host', 'password']
        conf_fields = config.options(db_info)
        
        for e in fields: #Let's check if all the needed fields are present in the config file
            if e not in conf_fields:
                error_msg = 'Error! The config file is missing the field "' + e + '"'
                raise ValueError(error_msg)

            values[e] = config.get(db_info, e)
            if values[e] == "": #We can't have empty values in our config file so an exception will be raised
                error_msg = 'Error! The value for the field "' + e + '"can not be empty!'
                raise ValueError(error_msg)
        
        new_obj = self.__InnerClass(values['db_name'], values['user'], values['host'], values['password'], release)
        return new_obj


class DatabaseConfiguration(_DatabaseLoader):
    """Class following the singleton pattern that reads and stores the database configuration of the project.
    The data of the configuration can be safely accessed through the public methods of this class.
    """

    __instance = None

    def __init__(self):
        if self.__instance is None:
            fresh_obj = super().load_db(True)
            DatabaseConfiguration.__instance = fresh_obj

    def __getattr__(self, name):
        return getattr(self.__instance, name)

class TestConnection(_DatabaseLoader):
    """Class following the singleton pattern that reads and stores the database configuration of the project.
    The data of the configuration can be safely accessed through the public methods of this class.
    This class is used to connect to the testing database for testing purposes.
    """

    __instance = None

    def __init__(self):
        if self.__instance is None:
            fresh_obj = super().load_db(False)
            TestConnection.__instance = fresh_obj

    def __getattr__(self, name):
        return getattr(self.__instance, name)
        
    


