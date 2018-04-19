import configparser
import io
import os
import Model.db_wrapper
from sqlalchemy import create_engine


class DatabaseConfiguration:
    """Class following the singleton pattern that reads and stores the database configuration of the project.
    The data of the configuration can be safely accessed through the public methods of this class.
    """

    __instance = None

    class __InnerClass:
        def __init__(self, dbname, user, host, password):
            self.dbname = dbname
            self.user = user
            self.host = host
            self.password = password
            self.db_connection = db_wrapper.DBWrapper(dbname, user, host, password)
            self.engine = create_engine("postgresql://{}:{}@{}/{}".format(self.get_dbname(), self.get_password(), self.get_host(), self.get_dbname()))
            pass

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
        
        def get_db(self):
            """Returns a DBWrapper object that represents the DB connection to PSQL"""
            #return self.db_connection
            return db_wrapper.DBWrapper(self.dbname, self.user, self.host, self.password)

        def get_engine(self):
            """Returns an SQL Alchemy engine."""
            #return self.engine
            return create_engine("postgresql://{}:{}@{}/{}".format(self.user, self.password, self.host, self.dbname))

    def __init__(self):
        if DatabaseConfiguration.__instance is None:
            try: #Try opening the file and loading it
                #Get path without worrying about current working directory.
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
            
            DatabaseConfiguration.__instance = self.__InnerClass(values['db_name'], values['user'], values['host'], values['password'])



    def __getattr__(self, name):
        return getattr(self.__instance, name)
    


if __name__ == '__main__':
    DatabaseConfiguration()
