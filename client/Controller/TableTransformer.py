import sys
import os
import math

import psycopg2
from psycopg2 import sql
import sqlalchemy
import pandas as pd

from Controller.DatasetHistoryManager import DatasetHistoryManager
from Model.SQLTypeHandler import SQLTypeHandler

class TableTransformer:
    """Class that performs transformations and various actions on SQL tables to support the data cleaning process.

    Attributes:
        setid: The id of the dataset that the user wants to modify
        replace: A boolean indicating whether the modification of the data should overwrite the table or create a new table
                 True overwrites data, False creates a new table (every transformation method has a new_name parameter that is the name of the new table)
        db_connection: psycopg2 database connection to execute SQL queries
        engine: SQLalchemy engine to use pandas functionality
        track_history: A boolean indicating whether the transformations performed should be tracked in the history table.
    """

    def __init__(self, setid, db_conn, engine, replace=True, track_history=True):
        """Inits the TableTransformer with provided values"""
        self.setid = setid
        self.replace = replace
        self.db_connection = db_conn
        self.engine = engine
        self.history_manager = DatasetHistoryManager(setid, db_conn, track_history)
        
    class AttrTypeError(Exception):
        """
        This exception is raised whenever an user attempts to perform a transformation on an attribute
        whose type is not supported by the called transformation.
        """

    class ConversionError(Exception):
        """
        This exception is raised whenever an implicit type conversion of an attribute failed because of
        values that aren't possible to convert.
        """

    class ValueError(Exception):
        """
        This exception is raised whenever an operation is provided with an inappropiate value causing
        the operation to fail.
        """
        
    def is_nullable(self, tablename, attribute):
        """Extra option to check whether an attribute is nullable or not. If deleting something is impossible due to a not null
        constraint, the row will be deleted as a whole.
        """
        internal_ref = self.get_internal_reference(tablename)
        cur = self.db_connection.cursor()
        query = ("SELECT is_nullable FROM information_schema.columns  WHERE table_schema = %s"
                " AND table_name =  %s AND column_name = %s LIMIT 1")
        cur.execute(sql.SQL(query), [internal_ref[0], tablename, attribute])
        result = cur.fetchone()[0]
        if result == 'YES':
            return True
        elif result == 'NO':
            return False
        raise self.ValueError("Error: is_nullabe returned something else than YES or NO.")
    
    def set_to_overwrite(self):
        """Method that changes the behavior of the TableTransforler to overwrite the table when performing modifcations."""
        self.replace = True
        
    def set_to_copy(self):
        """Method that changes the behavior of the TableTransforler to create a new table when performing modifcations."""
        self.replace = False
        
    def get_internal_reference(self, tablename):
        """Get the internal reference for the table of (setid) and (tablename). This returns a pair (id, name)."""
        return (str(self.setid), tablename)
    
    def copy_table(self, internal_ref, new_name):
        """In case the transformation has to result in a new table, we copy the existing one and perform modifications on this copy.

        Parameters:
           internal_ref: A tuple containing information to identify the table in our system. This is returned by get_internal_reference().
           new_name: A string representing the name of the new table constructed after performing a transformation.
        """
        if new_name == "":
            raise self.ValueError('No tablename given to the new table resulting from this operation. Please assign a valid tablename.')

        new_name = self.__get_unique_name(new_name, new_name, False)
        #Execute with the dynamic SQL module of psycopg2 to avoid SQL injecitons
        self.db_connection.cursor().execute(sql.SQL("CREATE TABLE {}.{} AS SELECT * FROM {}.{}").format(sql.Identifier(internal_ref[0]),sql.Identifier(new_name),
                                                                                          sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1])))
        self.db_connection.commit()
        return (internal_ref[0], new_name)

    def delete_rows_using_predicate_logic(self, tablename, arg_list, new_name=""):
        """Method to delete rows by using provided predicates like "attribute > x AND attribute != y".

        Parameters:
            arg_list: A list of strings containing the strings representing the predicates (Identifiers, logical operators).
        """
        #NEED TO REVIEW THIS METHOD AFTER NEW FRONT-END
        """internal_ref = self.get_internal_reference(tablename)
        if self.replace is False:
            internal_ref = self.copy_table(internal_ref, new_name)
        #List of length 4 is of type ['ATTRIBUTE' '=' 'X' 'END]
        list_size = len(arg_list)
        if list_size  in [3, 7, 11]:
            predicate = ''
            part1 = ''
            part2 = ''
            part3 = ''
            if list_size >= 3:
                part1 += '"{}"'.format(arg_list[0])
                part1 += ' ' + arg_list[1]
                attr_type = self.get_attribute_type(tablename, arg_list[0])[0]
                if SQLTypeHandler().is_numerical(attr_type) is not True:
                    part1 += ' ' + "'{}'".format(arg_list[2])                   
                else:
                    part1 += ' ' + arg_list[2]

                if arg_list[2] == 'None':#Add a NULL check in case user meant to delete NULLS
                    if attr_type not in ['character', 'character varying']:
                        part1 = ''
                    else:
                        part1 += ' OR '
                    if arg_list[1] == '=':
                        part1 = ' (' + part1
                        part1 += '"{}" IS NULL )'.format(arg_list[0])
                    elif arg_list[1] == '!=':
                        part1 = ' (' + part1
                        part1 += '"{}" IS NOT NULL )'.format(arg_list[0])
                    
                    
            
            if list_size >= 7:
                
                part1 += ' ' + arg_list[3]
                part2 += ' ' +'"{}"'.format(arg_list[4])
                part2 += ' ' + arg_list[5]
                attr_type = self.get_attribute_type(tablename, arg_list[4])[0]
                if SQLTypeHandler().is_numerical(attr_type) is not True:
                    part2 += ' ' + "'{}'".format(arg_list[6])                   
                else:
                    part2 += ' ' + arg_list[6]

                if arg_list[6] == 'None':#Add a NULL check in case user meant to delete NULLS
                    if attr_type not in ['character', 'character varying']:
                        part2 = ''
                    else:
                        part2 += ' OR '
                    if arg_list[5] == '=':
                        part2 = ' (' + part2
                        part2 += '"{}" IS NULL )'.format(arg_list[4])
                    elif arg_list[5] == '!=':
                        part2 = ' (' + part2
                        part2 += '"{}" IS NOT NULL )'.format(arg_list[4])

            if list_size == 11:
                part2 += ' ' + arg_list[7]
                part3 += ' ' + '"{}"'.format(arg_list[8])
                part3 += ' ' + arg_list[9]
                attr_type = self.get_attribute_type(tablename, arg_list[8])[0]
                if SQLTypeHandler().is_numerical(attr_type):
                    part3 += ' ' + "'{}'".format(arg_list[10])                   
                else:
                    part3 += ' ' + arg_list[10]

                if arg_list[10] == 'None':#Add a NULL check in case user meant to delete NULLS
                    if SQLTypeHandler().is_string(attr_type) is False:
                        part3 = ''
                    else:
                        part3 += ' OR '
                    if arg_list[9] == '=':
                        part3 = ' (' + part3
                        part3 += '"{}" IS NULL )'.format(arg_list[8])
                    elif arg_list[1] == '!=':
                        part3 = ' (' + part3
                        part3 += '"{}" IS NOT NULL )'.format(arg_list[8])

            predicate = part1 + part2 + part3

        else:
            raise self.ValueError('Can not delete rows because an invalid predicate has been provided.')

        query = "DELETE FROM {}.{} WHERE %s" % predicate
        try:
            self.db_connection.cursor().execute(sql.SQL(query).format(sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1])))
        except psycopg2.DataError:
            raise self.ValueError('Could not delete rows using this predicate since it contains invalid input value(s) for the attributes.')
        self.db_connection.commit()
        clean_predicate = predicate.replace('"', '\\"') #Escape double quotes to pass it in postgres array
        self.history_manager.write_to_history(internal_ref[1], tablename, 'None', [clean_predicate], 15)"""
    
    def delete_attribute(self, tablename, attribute, new_name=""):
        """Delete an attribute of a table"""
        internal_ref = self.get_internal_reference(tablename)
        if self.replace is False:
            internal_ref = self.copy_table(internal_ref, new_name)
        self.db_connection.cursor().execute(sql.SQL("ALTER TABLE {}.{} DROP COLUMN {}").format(sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1]),
                                                                                    sql.Identifier(attribute)))
        self.db_connection.commit()
        self.history_manager.write_to_history(internal_ref[1], tablename, attribute, [], 2)

    def get_supported_types(self):
        """Quick method that returns all the types supported by the TableTransformer for conversion purposes"""
        return ['VARCHAR(255)', 'CHAR(255)', 'INTEGER', 'FLOAT', 'DATE', 'TIME', 'TIMESTAMP']

    def get_conversion_options(self, tablename, attribute):
        """Returns a list of supported types that the given attribute can be converted to."""
        data_type = self.get_attribute_type(tablename, attribute)[0]
        options = { 'character varying' : ['INTEGER', 'FLOAT', 'DATE', 'TIME', 'TIMESTAMP',  'CHAR(n)'],
                    'character'         : ['VARCHAR(255)', 'VARCHAR(n)', 'INTEGER', 'FLOAT', 'DATE', 'TIME', 'TIMESTAMP'],
                    'integer'           : ['FLOAT', 'VARCHAR(255)', 'VARCHAR(n)','CHAR(n)'],
                    'double precision'  : ['INTEGER', 'VARCHAR(255)','CHAR(n)'] ,
                    'date'              : ['VARCHAR(255)', 'VARCHAR(n)', 'CHAR(n)'],
                    'time without time zone' : ['VARCHAR(255)', 'VARCHAR(n)', 'CHAR(n)'],
                    'timestamp without time zone' : ['VARCHAR(255)', 'VARCHAR(n)', 'CHAR(n)']
                    }
        #In case it's a data type unknown to this class, we can almost certainly convert to varchar(255)
        return options.setdefault(data_type, ['VARCHAR(255)'])

    def get_datetime_formats(self, data_type):
        """Returns a list of supported formats for the conversion of a character type to a date/time type."""
        formats = { 'DATE'      : ['DD/MM/YYYY', 'DD-MM-YYYY', 'MM/DD/YYYY', 'MM-DD-YYYY', 'YYYY/MM/DD', 'YYYY-MM-DD'],
                    'TIME'      : ['HH24:MI:SS', 'HH12:MI:SS AM/PM', 'HH12:MI AM/PM', 'HH12 AM/PM'],
                    'TIMESTAMP' : ['DD/MM/YYYY TIME', 'DD-MM-YYYY TIME', 'MM/DD/YYYY TIME', 'MM-DD-YYYY TIME', 'YYYY/MM/DD TIME', 'YYYY-MM-DD TIME']
                    }
        return formats.setdefault(data_type, [])

    def get_extraction_options(self, data_type):
        """Returns a list of supported options for the date extraction method."""
        if data_type in ['date', 'timestamp without time zone']:
            return ['YEAR', 'MONTH + YEAR', 'MONTH', 'DAY OF THE WEEK']
        else:
            return []

    def __readable_format_to_postgres(self, attr_type, readable_format):
        """Modifies the readable formats we use for the front-end to correct postgres formats in the back-end."""
        if attr_type == 'DATE':
            return readable_format
        
        elif attr_type == 'TIME':
            ps_format = readable_format.replace('/PM', '')
            return ps_format

        elif attr_type == 'TIMESTAMP':
            ps_format = readable_format.replace('TIME', 'HH24:MI:SS')
            return ps_format

        error_msg = "This method should only be called when converting to date/time."
        raise ValueError(error_msg)

    def __get_datetime_regex(self, d_type, d_format):
        """Method that returns the regex matching the type (DATE, TIMESTAMP, TIME)."""
        pattern = d_format
        if d_type in ['DATE', 'TIMESTAMP']:
            pattern = pattern.replace('DD', '[0-9]{1,2}')
            pattern = pattern.replace('MM', '[0-9]{1,2}')
            pattern = pattern.replace('YYYY', '[0-9]{4}')
        if d_type in ['TIMESTAMP', 'TIME']:
            pattern = pattern.replace('HH24', '[0-9]{1,2}')
            #pattern = pattern.replace('HH24', '[0-9]{1,2}')
            pattern = pattern.replace('MI', '(:[0-9]{1-2})?')
            pattern = pattern.replace('SS', '(:[0-9]{1-2})?')
            pattern = pattern.replace('AM', '')
        return pattern

    def get_attribute_type(self, tablename, attribute, detailed=False):
        """Return the postgres data type of an attribute.
        Parameters:
            detailed: A boolean indicating if the method should return a detailed description of the type (include size limit).
        """
        internal_ref = self.get_internal_reference(tablename)
        cur = self.db_connection.cursor()
        query = ("SELECT data_type, character_maximum_length FROM information_schema.columns"
                 " WHERE table_schema = %s AND table_name =  %s AND column_name = %s LIMIT 1")
        cur.execute(sql.SQL(query), [internal_ref[0], tablename, attribute])

        row = cur.fetchone()
        self.db_connection.commit()
        if row is None: #Nothing fetched? Return None.
            error_msg = 'Error occured! Column "' + attribute + '" of table "' + tablename + '" does not exist.'
            raise self.ValueError(error_msg)
        if detailed is False:
            return (row[0], internal_ref)
        else:
            return (row[0], row[1])


    def __convert_numeric(self, internal_ref, attribute, to_type, length):
        """Conversion of "numeric" things (INTEGER and FLOAT, DATE, TIME, TIMESTAMP)"""
        if to_type in ['VARCHAR(n)', 'CHAR(n)']:
            to_type = to_type.replace('n', str(length))
        sql_query = "ALTER TABLE {}.{} ALTER COLUMN  {} TYPE %s" % to_type
        self.db_connection.cursor().execute(sql.SQL(sql_query).format(sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1]),
                                                                                              sql.Identifier(attribute)), [to_type])
        self.db_connection.commit()
        return to_type

    
    def __convert_character(self, internal_ref, attribute, to_type, data_format, length):
        """Conversion of a character type (VARCHAR and CHAR)"""
        patterns = { 
                     'INTEGER'      : 'INTEGER USING {}::integer{}',
                     'FLOAT'        : 'FLOAT USING {}::float{}',
                     'DATE'         : 'DATE USING to_date({} , \'{}\')',
                     'TIME'         : 'TIME USING to_timestamp({}, \'{}\')::time',
                     'TIMESTAMP'    : 'TIMESTAMP USING to_timestamp({}, \'{}\')',
                     'VARCHAR(255)' : 'VARCHAR',
                     'VARCHAR(n)'   : 'VARCHAR',
                     'CHAR(n)'      : 'CHAR',

                     }
        temp = patterns.setdefault(to_type, '')
        if temp == '':
            error_msg = "Converting to the type " + to_type + "is not supported!"
            raise self.ValueError(error_msg)
        if to_type not in ['DATE', 'TIME', 'TIMESTAMP']: #Make sure no accidental data_format is provided
            data_format = ''

        else:
            data_format = self.__readable_format_to_postgres(to_type, data_format)
        #Maybe sanity check on these parameters? I'll look into it, note: 32
        ident_attr = '"{}"'.format(attribute)
        casting_var = temp.format(ident_attr, data_format)

        if temp in ['CHAR' , 'VARCHAR']: #Char and varchar don't need special parameters
            to_type = to_type.replace('n', str(length))
            casting_var = to_type
            
        sql_query = "ALTER TABLE {}.{} ALTER COLUMN {} TYPE " + casting_var
        cur = self.db_connection.cursor()
        try:
            cur.execute(sql.SQL(sql_query).format(sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1]),
                                                  sql.Identifier(attribute)))
        except psycopg2.DataError as e:
            if 'value too long' in str(e):
                error_msg = "Conversion failed due to character values longer than {} in the attribute.".format(length)
                raise self.ValueError(error_msg)

            else:
                error_msg = "Conversion failed due to values in attribute that can't correctly be converted to {}.".format(to_type)
                raise self.ValueError(error_msg)
        self.db_connection.commit()
        return to_type



    def change_attribute_type(self, tablename, attribute, to_type, data_format="", length='1', new_name=""):
        """Change the type of a table attribute.

        Parameters:
            to_type: A string representing the PostreSQL type we want to convert to, like INTEGER or VARCHAR(255)
            data_format: Optional parameter for when the attribute has to follow a specific format, like a DATE with format 'DD/MM/YYYY'
            length: This specifies the size of a char or varchar if it the user wants to place a specific limit.
            new_name: The name for the new table constructed from this operation if the TableTransformer is not set to overwrite
        """
        cur_type, internal_ref  = self.get_attribute_type(tablename, attribute)
        if self.replace is False:
            internal_ref = self.copy_table(internal_ref, new_name)
        
        if (cur_type == 'character varying') or (cur_type == 'character'):
            to_type = self.__convert_character(internal_ref, attribute, to_type, data_format, length)
        else:
            to_type = self.__convert_numeric(internal_ref, attribute, to_type, length)
            
        self.history_manager.write_to_history(internal_ref[1], tablename, attribute, [to_type, data_format, length], 1)

    def force_attribute_type(self, tablename, attribute, to_type, data_format="", new_name=""):
        """In case that change_attribute_type fails due to elements that can't be converted
        this method will force the conversion by deleting the row containing the bad element.
        The parameters are identical to change_attribute_type().
        """
        if self.replace is True:
            internal_ref = self.get_internal_reference(tablename)
        else:
            internal_ref = self.get_internal_reference(new_name)
        
        pattern = ""
        if to_type == 'INTEGER':
            pattern = '^[-+]?[0-9]+$'
            
        elif to_type == 'FLOAT':
            pattern = '^[-+.]?[0-9]+[.]?[e]?[-+]?[0-9]*$'

        elif to_type in ['DATE', 'TIMESTAMP', 'TIME']:
            data_format = self.__readable_format_to_postgres(to_type, data_format)
            pattern = self.__get_datetime_regex(to_type, data_format)
            

        else:
            # This is going to be very tough to express....
            #Know the formats we need to support for date, time, timestamp
            return None

        self.db_connection.cursor().execute(sql.SQL("DELETE FROM {}.{} WHERE ({} !~ %s )").format(sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1]),
                                                                                         sql.Identifier(attribute)), [pattern])
        self.db_connection.commit()

        if self.replace is True:
            self.change_attribute_type(tablename, attribute, to_type, data_format, new_name)
        else:
            # The first call of change_attribute_type already created a new table which is a copy of (tablename)
            # But we don't want to copy this table once again, only overwrite it
            self.replace = True
            self.change_attribute_type(new_name, attribute, to_type, data_format, new_name)

    def find_and_replace(self, tablename, attribute, value, replacement, exact=True, replace_all=True, new_name=""):
        """Method that finds values and replaces them with the provided argument.

        Parameters:
            value: The value that needs to found so it can be replaced.
            replacement: The value that will replace the values that were found.
            exact : A boolean indicating if we want to match whole-words only or allowing substring matching
            replace_all : A boolean indicating whether a found substring should be replaced or the string should be replaced.
                          True replaces the whole string, False replaces the found substring with the replacement.
        """
        cur_type, internal_ref  = self.get_attribute_type(tablename, attribute)
        if self.replace is False:
            internal_ref = self.copy_table(internal_ref, new_name)

        original_value = value            
        if exact is True:
            sql_query = "UPDATE {0}.{1} SET {2} = %s WHERE {2} = %s"
        elif exact is False:
            if value.isalnum() is not True: #Only alphanumerical substrings are supported
                raise self.ValueError("Values not containing alphanumerical characters can not be used for substring matching. "
                                      "Please use whole-word matching to find and replace the values.")

            attr_type = self.get_attribute_type(tablename, attribute)[0]
            if SQLTypeHandler().is_string(attr_type) is False:
                raise self.AttrTypeError("Substring matching is only possible with character strings. "
                                         "Please convert the attribute to a character string type.")
                
            sql_query = "UPDATE {0}.{1} SET {2} = %s WHERE {2} LIKE %s"

            value = '%{}%'.format(value)

            if replace_all is False: #We have to replace the substring and this is done with a different query.
                sql_query = "UPDATE {0}.{1} SET {2} = replace({2}, %s, %s) WHERE {2} LIKE %s"
                cur = self.db_connection.cursor()
                cur.execute(sql.SQL(sql_query).format(sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1]),
                                                                              sql.Identifier(attribute)), (original_value, replacement, value))
                self.db_connection.commit()
                
        if replace_all is True: #If replace_all was False the operation was already performed by the query above.           
            try:
                self.db_connection.cursor().execute(sql.SQL(sql_query).format(sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1]),
                                                                          sql.Identifier(attribute)), (replacement, value))
            except psycopg2.DataError:
                raise self.ValueError("Could not perform find-and-replace due to an invalid input value for this attribute.")
            self.db_connection.commit()
        self.history_manager.write_to_history(internal_ref[1], tablename, attribute, [original_value, replacement, exact, replace_all], 8)

    def regex_find_and_replace(self, tablename, attribute, regex, replacement, case_sens=False, new_name=""):
        """Method that finds values with a provided regex and replaces them with a provided replacement.

        Parameters:
            regex: A string that is a POSIX compliant regex to match all the searched values.
            replacement: A replacement for the found values.
            case_sens: A boolean indicating whether the regex is case sensitive. True for sensitive, False for insensitive.
            new_name: The name of the new table if the TableTransformer is not set to overwrite.
        """
        cur_type, internal_ref = self.get_attribute_type(tablename, attribute)
        if self.replace is False:
            internal_ref = self.copy_table(internal_ref, new_name)

        if SQLTypeHandler().is_string(cur_type) is False:
            raise self.AttrTypeError("Find-and-replace using regular epxressions is only possible with character type attributes. "
                                     "Please convert the needed attribute to VARCHAR or CHAR.")
        
        if case_sens is False:
            sql_query = "UPDATE {0}.{1} SET {2} = %s WHERE {2} ~* %s"
        elif case_sens is True:
            sql_query = "UPDATE {0}.{1} SET {2} = %s WHERE {2} ~ %s"

        try:
            self.db_connection.cursor().execute(sql.SQL(sql_query).format(sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1]),
                                                                      sql.Identifier(attribute)), (replacement, regex))
        except psycopg2.DataError as e:
            error_msg = str(e)  + ". Please refer to the PostgreSQL documentation on regular expressions for more information."
            raise self.ValueError(error_msg)

        self.db_connection.commit()
        self.history_manager.write_to_history(internal_ref[1], tablename, attribute, [regex, replacement, case_sens], 9)
        
    def __get_simplified_types(self, tablename, data_frame):
        """Method that makes sure pandas dataframe uses correct datatypes when writing to SQL."""
        new_attributes = data_frame.columns.values.tolist()
        new_types = {}
        sqla_type = None
        for  elem in new_attributes:
            try:
                psql_type = self.get_attribute_type(tablename, elem, True)
                sqla_type = SQLTypeHandler().to_sqla_object(psql_type[0])
                if sqla_type is None:
                    if psql_type[0] == 'character varying':
                        sqla_type = sqlalchemy.types.VARCHAR(psql_type[1])
                    elif psql_type[0] == 'character':
                        sqla_type = sqlalchemy.types.CHAR(psql_type[1])
                    else:
                        continue
            except self.ValueError:
                pd_type = str(data_frame[elem].dtype)
                sqla_type = SQLTypeHandler().to_sqla_object(pd_type)
                
            new_types[elem] = sqla_type
        return new_types
    
    def one_hot_encode(self, tablename, attribute, new_name=""):
        """Method that performs one hot encoding given an attribute"""
        internal_ref = self.get_internal_reference(tablename)
        sql_query = "SELECT * FROM \"{}\".\"{}\"".format(*internal_ref)
        df = pd.read_sql(sql_query, self.engine)
        encoded = pd.get_dummies(df[attribute]) #Perfom one-hot-encoding
        df = df.drop(attribute, axis=1) #Drop the attribute used for encoding
        df = df.join(encoded) #Join the original attributes with the encoded table
        new_dtypes = self.__get_simplified_types(tablename, df)
        eventual_table = "" #This will be the name of table containing the changes.
        if self.replace is True:
            #If the table should be replaced, drop it and recreate it.
            df.to_sql(tablename, self.engine, None, internal_ref[0], 'replace', index = False, dtype = new_dtypes)
            eventual_table = tablename
        elif self.replace is False:
            #We need to create a new table and leave the original untouched
            new_name = self.__get_unique_name("", new_name, False)
            df.to_sql(new_name, self.engine, None, internal_ref[0], 'fail', index = False, dtype = new_dtypes)
            eventual_table = new_name
            
        self.history_manager.write_to_history(eventual_table, tablename, attribute, [], 14)
        
    def __calculate_zscore(self, mean, standard_dev, value):
        """Method to quickly calculate z-scores"""
        zscore = (value - mean) / standard_dev
        return zscore
    
    def normalize_using_zscore(self, tablename, attribute, new_name = ""):
        """Method that normalizes the values of an attribute using the z-score.
        This will normalize everything in a 1-point range, thus [0-1].
        """
        #Let's check if the attribute is a numeric type, this should not be performed on non-numeric types
        attr_type = self.get_attribute_type(tablename, attribute)[0]
        if SQLTypeHandler().is_numerical(attr_type) is False:
            raise self.AttrTypeError("Normalization failed due attribute not being of numeric type (neither integer or float)")
        
        internal_ref = self.get_internal_reference(tablename)
        sql_query = "SELECT * FROM \"{}\".\"{}\"".format(*internal_ref)
        df = pd.read_sql(sql_query, self.engine)
        mean = df[attribute].mean()
        #Calculate the standard deviation, for this method we consider the data as the population
        #and not a sample so we don't use Bessel's correction
        standard_deviation = df[attribute].std(ddof=0)
        #Get the series containing values and convert them to float so that they can be assigned with their z-scores
        column = df[attribute].astype(float)
        #Calculate all the z-scores
        for i in range(column.size):
            zscore = self.__calculate_zscore(mean, standard_deviation, column[i])
            #limit the extremes to -2 and +2
            if zscore < -2:
                zscore = 2
            elif zscore > 2:
                zscore = 2
            column[i] = zscore
        
        column = column.divide(4) #Divide all the values by 4 to get a 1-point range
        column = column.add(0.5) #Now the mean is 0, so add 0.5 to have the mean at 0.5
        df.update(column)
        new_dtypes = self.__get_simplified_types(tablename, df)
        #Casting the attribute back to int would be problematic so make sure it's float
        new_dtypes[attribute] = sqlalchemy.types.Float(precision=25, asdecimal=True)
        eventual_table = ""

        if self.replace is True:
            #If the table should be replaced, drop it and recreate it.
            df.to_sql(tablename, self.engine, None, internal_ref[0], 'replace', index = False, dtype = new_dtypes)
            eventual_table = tablename
        elif self.replace is False:
            #We need to create a new table and leave the original untouched
            new_name = self.__get_unique_name("", new_name, False)
            df.to_sql(new_name, self.engine, None, internal_ref[0], 'fail', index = False, dtype = new_dtypes)
            eventual_table = new_name

        self.history_manager.write_to_history(internal_ref[1], tablename, attribute, [mean], 10)
        
    def __get_unique_name(self, tablename, name, is_attribute=True):
        """Method that makes sure an attribute name or table name given the name
        is unique or else make an unique variation of it by appending a number.
        """
        cur = self.db_connection.cursor()
        if is_attribute is True:
            query = ("SELECT column_name FROM information_schema.columns WHERE table_schema = %s "
                     "AND table_name   = '{}'").format(tablename)
        elif is_attribute is False:
            query = "SELECT table_name FROM information_schema.columns WHERE table_schema = %s{}".format("")
        
            
        cur.execute(sql.SQL(query), [str(self.setid)])
        query_result = cur.fetchall()
        name_list = [x[0] for x in query_result] #List of all the attribute/table names in the table.
        name_size = len(name)
        count = 0

        for elem in name_list:
            if name in elem:
                if len(elem) == name_size: #If it's the same size, it's the string itself
                    count += 1
                    continue #Looking for the next characters is pointless. So continue
                if (elem[name_size] == '_') and (str.isdigit(elem[name_size+1:])):
                    count += 1

        if count == 0:
            return name
        else:
            new_name = name + "_" + str(count)
            return new_name
        
    def __make_binlabels(self, bins):
        binlabels = []
        for i in range(1, len(bins)):
            label = "[" + str(bins[i-1]) + " , " + str(bins[i]) + "["                                
            binlabels.append(label)
        return binlabels

    def __truncate_float(self, fp_num):
        truncated = "%.3f" % fp_num
        return float(truncated)
        
    def discretize_using_equal_width(self, tablename, attribute, nr_bins, new_name=""):
        """Method that calulates the bins for an equi-distant discretization and performs it"""
        internal_ref = self.get_internal_reference(tablename)
        attr_type = self.get_attribute_type(tablename, attribute)[0]
        if SQLTypeHandler().is_numerical(attr_type) is False:
            raise self.AttrTypeError("Discretization failed due attribute not being of numeric type (neither integer or float)")

        integer_bool = SQLTypeHandler().is_integer(attr_type)
        sql_query = 'SELECT * FROM "{}"."{}"'.format(*internal_ref)
        df = pd.read_sql(sql_query, self.engine)
        minimum = df[attribute].min()
        maximum = df[attribute].max() + 0.0001 #Add a little bit to make sure the max element is included
        leftmost_edge = (math.floor(minimum * 1000) / 1000)
        rightmost_edge = (math.ceil(maximum * 1000) / 1000)
        value_range = rightmost_edge - leftmost_edge

        #Calculate the width of the bins
        bin_width = (math.ceil((value_range / nr_bins) * 1000)) / 1000
        if integer_bool is True:
            bin_width = math.ceil(bin_width)
        else:
            bin_width = self.__truncate_float(bin_width)
        bins = [leftmost_edge if integer_bool is False else math.ceil(leftmost_edge)]
        #Get all the intervals in list form
        for i in range(nr_bins):
            if integer_bool is True:
                next_interval = math.ceil(bins[i]) + bin_width
            else:
                next_interval = self.__truncate_float(bins[i] + bin_width)
            bins.append(next_interval)

        #Make labels for these intervals using the list of bin values
        binlabels = self.__make_binlabels(bins)
        column_name = attribute + "_categorical"
        category = self.__get_unique_name(tablename, column_name)
        df[category] = pd.cut(df[attribute], bins, right=False, labels = binlabels, include_lowest=True)
        new_dtypes = self.__get_simplified_types(tablename, df)
        eventual_table = "" #This the table that eventually becomes the table with the resulting changes

        if self.replace is True:
            #If the table should be replaced, drop it and recreate it.
            df.to_sql(tablename, self.engine, None, internal_ref[0], 'replace', index = False, dtype = new_dtypes)
            eventual_table = tablename
        elif self.replace is False:
            #We need to create a new table and leave the original untouched
            new_name = self.__get_unique_name("", new_name, False)
            df.to_sql(new_name, self.engine, None, internal_ref[0], 'fail', index = False, dtype = new_dtypes)
            eventual_table = new_name
        
        self.history_manager.write_to_history(eventual_table, tablename, attribute, [], 6)

    def __calculate_equifrequent_indices(self, width, remainder, nr_bins):
        """Calculates the indices of the list that represent bin edges for discretize_using_equal_frequency."""
        index_bins = [-1] #Offset all the indices because Nth element is at index N-1
        for i in range(nr_bins):
            index_value = index_bins[i] + width
            if remainder > 0:
                index_value += 1
                remainder -= 1
            index_bins.append(index_value)# minus because Nth element is at index N-1

        index_bins[0] = 0 #The first index is always zero.
        return index_bins
            
    def discretize_using_equal_frequency(self, tablename, attribute, new_name=""):
        """Method that calulates the bins for an equi-frequent discretization and performs it"""
        #The initial steps are similar to equi-distant discretization
        attr_type = self.get_attribute_type(tablename, attribute)[0]
        if SQLTypeHandler().is_numerical(attr_type) is False:
            raise self.AttrTypeError("Discretization failed due attribute not being of numeric type (neither integer or float)")

        round_to_int = True
        internal_ref = self.get_internal_reference(tablename)
        sql_query = "SELECT * FROM \"{}\".\"{}\"".format(*internal_ref)
        df = pd.read_sql(sql_query, self.engine)
        if attr_type != 'integer':
            if df[attribute].max() < 2: #Probably working with values [0-1]
                round_to_int = False
        nr_values = df[attribute].size
        nr_bins = math.floor(math.sqrt(nr_values))
        if nr_bins < 2:
            nr_bins = 2
        elif nr_bins > 20:
            nr_bins = 20
        
        elements = df[attribute].tolist() #Load all the elements in a python list
        elements.sort()
        list_size = len(elements)
        indices = []

        #Let's first try the naïve approach
        bin_width = math.floor(list_size / nr_bins)
        remainder = list_size % nr_bins
        indices   = self.__calculate_equifrequent_indices(bin_width, remainder, nr_bins)

        #Calculate actual values for the bins
        bins = [elements[0]]
        for i in range(1, len(indices)):
            index = indices[i]
            if round_to_int is True:
                value = math.ceil(elements[index] + 0.0001)
            else:
                value = round((elements[index] + 0.05), 1)
            bins.append(value)

        bins = list(set(bins))
        bins.sort()

        #Make binlabels to represent the bins.
        binlabels = self.__make_binlabels(bins)
        column_name = attribute + "_categorical"
        category = self.__get_unique_name(tablename, column_name)
        df[category] = pd.cut(df[attribute], bins, right=False, labels = binlabels, include_lowest=True)
        new_dtypes = self.__get_simplified_types(tablename, df)
        eventual_table = "" #This the table that eventually becomes the table with the resulting changes
        
        if self.replace is True:
            #If the table should be replaced, drop it and recreate it.
            df.to_sql(tablename, self.engine, None, internal_ref[0], 'replace', index = False, dtype = new_dtypes)
            eventual_table = tablename
        elif self.replace is False:
            #We need to create a new table and leave the original untouched
            new_name = self.__get_unique_name("", new_name, False)
            df.to_sql(new_name, self.engine, None, internal_ref[0], 'fail', index = False, dtype = new_dtypes)
            eventual_table = new_name
            
        self.history_manager.write_to_history(eventual_table, tablename, attribute, [], 5)
        
    def discretize_using_custom_ranges(self, tablename, attribute, ranges, exclude_right=True, new_name=""):
        """Method that discretizes given a a list representing the bins.

        Parameters:
            ranges: A python list that represents the bins that the user has provided
            exclude_right: A boolean indicating whether the rightmost edge should be included
                           True if the rightmost edge is excluded [X - Y[, False if rightmost edge is included ]X - Y]
        """
        attr_type = self.get_attribute_type(tablename, attribute)[0]
        if SQLTypeHandler().is_numerical(attr_type) is False:
            raise self.AttrTypeError("Discretization failed due attribute not being of numeric type (neither integer or float)")

        internal_ref = self.get_internal_reference(tablename)
        sql_query = "SELECT * FROM \"{}\".\"{}\"".format(*internal_ref)
        df = pd.read_sql(sql_query, self.engine)
        bracket = ""
        bins = ranges

        if exclude_right is True:
            bracket = "["
        else:
            bracket = "]"

        binlabels = []
        for i in range(1, len(ranges)):
            label = bracket + str(bins[i-1]) + " , " + str(bins[i]) + bracket                           
            binlabels.append(label)

        if exclude_right is True:
            param_a = False
            param_b = True
        else:
            param_a = True
            param_b = False

        column_name = attribute + "_categorical"
        category = self.__get_unique_name(tablename, column_name)
        df[category] = pd.cut(df[attribute], ranges, right=param_a, labels = binlabels, include_lowest=param_b)
        new_dtypes = self.__get_simplified_types(tablename, df)
        eventual_table = "" #This the table that eventually becomes the table with the resulting changes

        if self.replace is True:
            #If the table should be replaced, drop it and recreate it.
            df.to_sql(tablename, self.engine, None, internal_ref[0], 'replace', index = False, dtype = new_dtypes)
            eventual_table = tablename
        elif self.replace is False:
            #We need to create a new table and leave the original untouched
            new_name = self.__get_unique_name("", new_name, False)
            df.to_sql(new_name, self.engine, None, internal_ref[0], 'fail', index = False, dtype = new_dtypes)
            eventual_table = new_name

        self.history_manager.write_to_history(eventual_table, tablename, attribute, [ranges, exclude_right], 4)
        
    def delete_outliers(self, tablename, attribute, larger, value, replacement, new_name=""):
        """Method that gets rid of outliers of an attribute by setting them to null.

        Parameters:
            larger: A boolean indicating if we need to delete values larger or smaller than the provided value
                    True deletes all the values larger, False deletes all the values smaller.
            value: The value used to determine whether an element is an outlier.
            replacement: The value that will be used instead of the outliers
        """
        internal_ref = self.get_internal_reference(tablename)
        if self.replace is False:
            internal_ref = self.copy_table(internal_ref, new_name)

        #Let's check if the attribute is a numeric type, this should not be performed on non-numeric types
        attr_type = self.get_attribute_type(tablename, attribute)[0]
        if SQLTypeHandler().is_numerical(attr_type) is False:
            raise self.AttrTypeError("Deleting outliers failed due attribute not being of numeric type (neither integer or float)")
            
        if larger is True:
            comparator = '>'
        else:
            comparator = '<'
        #Create query for larger/smaller deletion of outlier
        sql_query = "UPDATE {0}.{1} SET {2} = %s  WHERE {2} cmp %s".replace('cmp', comparator)
        self.db_connection.cursor().execute(sql.SQL(sql_query).format(sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1]),
                                                                                    sql.Identifier(attribute)), (value, replacement))
        self.db_connection.commit()
        self.history_manager.write_to_history(internal_ref[1], tablename, attribute, [larger, value, replacement], 3)
        

    def __fill_nulls_with_x(self, attribute, internal_ref,  x):
        """Method that fills null values of an attribute with a provided value.

        Parameter:
            x: The value that is going to be used to fill all the nulls with.
        """
        self.db_connection.cursor().execute(sql.SQL("UPDATE {0}.{1} SET {2} = %s  WHERE {2} is null").format(sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1]),
                                                                                    sql.Identifier(attribute)), (x,))
        self.db_connection.commit()
        

    def fill_nulls_with_mean(self, tablename, attribute, new_name=""):
        """Method that fills null values of an attribute with the mean."""
        internal_ref = self.get_internal_reference(tablename)
        if self.replace is False:
            internal_ref = self.copy_table(internal_ref, new_name)

        #Let's check if the attribute is a numeric type, this should not be performed on non-numeric types
        attr_type = self.get_attribute_type(tablename, attribute)[0]
        if SQLTypeHandler().is_numerical(attr_type) is False:
            raise self.AttrTypeError("Filling nulls failed due attribute not being of numeric type (neither integer or float)")

        sql_query = "SELECT * FROM \"{}\".\"{}\"".format(*internal_ref)
        df = pd.read_sql(sql_query, self.engine)
        mean = df[attribute].mean()

        self.__fill_nulls_with_x(attribute, internal_ref, mean)
        self.history_manager.write_to_history(internal_ref[1], tablename, attribute, [mean], 10)


    def fill_nulls_with_median(self, tablename, attribute, new_name=""):
        """Method that fills null values of an attribute with the median."""
        internal_ref = self.get_internal_reference(tablename)
        if self.replace is False:
            internal_ref = self.copy_table(internal_ref, new_name)

        #Let's check if the attribute is a numeric type, this should not be performed on non-numeric types
        attr_type = self.get_attribute_type(tablename, attribute)[0]
        if SQLTypeHandler().is_numerical(attr_type) is False:
            raise self.AttrTypeError("Filling nulls failed due attribute not being of numeric type (neither integer or float)")

        sql_query = "SELECT * FROM \"{}\".\"{}\"".format(*internal_ref)
        df = pd.read_sql(sql_query, self.engine)
        median = df[attribute].median()

        self.__fill_nulls_with_x(attribute, internal_ref, median)
        self.history_manager.write_to_history(internal_ref[1], tablename, attribute, [median], 11)


    def fill_nulls_with_custom_value(self, tablename, attribute, value, new_name=""):
        """Method that fills null values of an attribute with a custom value"""
        internal_ref = self.get_internal_reference(tablename)
        if self.replace is False:
            internal_ref = self.copy_table(internal_ref, new_name)

        #Let's check if the attribute is a numeric type, this should not be performed on non-numeric types
        attr_type = self.get_attribute_type(tablename, attribute)[0]
        if SQLTypeHandler().is_numerical(attr_type) is False:
            raise self.AttrTypeError("Filling nulls failed due attribute not being of numeric type (neither integer or float)")

        #Perhaps do a sanity check here? I'll see later on.
        self.__fill_nulls_with_x(attribute, internal_ref, value)
        self.history_manager.write_to_history(internal_ref[1], tablename, attribute, [value], 12)

    
    def extract_part_of_date(self, tablename, attribute, extraction_arg, new_name=""):
        """Method that extracts part of a date, time, or datetime"""
        internal_ref = self.get_internal_reference(tablename)
        if self.replace is False:
            internal_ref = self.copy_table(internal_ref, new_name)

        attr_type = self.get_attribute_type(tablename, attribute)[0]
        if SQLTypeHandler().is_date_type(attr_type) is False:
            raise self.AttrTypeError("Extraction of date part failed due attribute not being a date type (neither DATE or TIMESTAMP).")

        if extraction_arg not in ['YEAR', 'MONTH + YEAR', 'MONTH', 'DAY OF THE WEEK']:
            raise self.ValueError('Extraction argument is not supported by TableTransformer')

        attr_name = attribute + '_part'
        attr_name = self.__get_unique_name(tablename, attr_name, True)
        #Let's first create this new column that will contain the extracted part of the date
        self.db_connection.cursor().execute(sql.SQL(
                    "ALTER TABlE {}.{} ADD COLUMN  {} VARCHAR(255)").format(sql.Identifier(internal_ref[0]),
                                                               sql.Identifier(internal_ref[1]),
                                                               sql.Identifier(attr_name)))

        cur = self.db_connection.cursor()
        if extraction_arg == 'YEAR':
            query = "UPDATE {}.{} SET {} = EXTRACT(YEAR from {})::VARCHAR(255)"

        elif extraction_arg == 'MONTH + YEAR':
            query = (
                """UPDATE {0}.{1} SET {2} = concat_ws( ' ', CASE (EXTRACT(MONTH from {3}))
                WHEN 1 THEN 'January'
                WHEN 2 THEN 'February'
                WHEN 3 THEN 'March'
                WHEN 4 THEN 'April'
                WHEN 5 THEN 'May'
                WHEN 6 THEN 'June'
                WHEN 7 THEN 'July'
                WHEN 8 THEN 'August'
                WHEN 9 THEN 'September'
                WHEN 10 THEN 'October'
                WHEN 11 THEN 'November'
                WHEN 12 THEN 'December'
                END, EXTRACT(YEAR from {3})::VARCHAR(255))""")
            
        elif extraction_arg == 'MONTH':
            query = (
                """UPDATE {}.{} SET {} =  CASE (EXTRACT(MONTH from {}))
                WHEN 1 THEN 'January'
                WHEN 2 THEN 'February'
                WHEN 3 THEN 'March'
                WHEN 4 THEN 'April'
                WHEN 5 THEN 'May'
                WHEN 6 THEN 'June'
                WHEN 7 THEN 'July'
                WHEN 8 THEN 'August'
                WHEN 9 THEN 'September'
                WHEN 10 THEN 'October'
                WHEN 11 THEN 'November'
                WHEN 12 THEN 'December'
                END""")

        elif extraction_arg == 'DAY OF THE WEEK':
            query = (
                """UPDATE {}.{} SET {} =  CASE (EXTRACT(DOW from {}))
                WHEN 0 THEN 'Sunday'
                WHEN 1 THEN 'Monday'
                WHEN 2 THEN 'Tuesday'
                WHEN 3 THEN 'Wednesday'
                WHEN 4 THEN 'Thursday'
                WHEN 5 THEN 'Friday'
                WHEN 6 THEN 'Saturday'
                END""")

        cur.execute(sql.SQL(query).format(sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1]),
                                          sql.Identifier(attr_name), sql.Identifier(attribute)))
        self.db_connection.commit()
        self.history_manager.write_to_history(internal_ref[1], tablename, attribute, [extraction_arg], 7)


    def undo_transformation(self, t_id):
        """Method to undo changes brought by an operation of TableTransformer."""
        pass


    def recreate_table_from_history(self, t_id, new_name):
        """Method that recreates a previous table described in the transformation history.

        Parameters:
            t_id: The internal id of the transformation as used in the DatasetHistory table.
            new_name: A string representing the name of the recreated table.
        """
        pass
                

    def change_column_name(self, tablename, old_name, new_name):
        self.db_connection.cursor().execute(sql.SQL(
            "ALTER TABlE {}.{} RENAME COLUMN {} TO {}").format(sql.Identifier(str(self.setid)),
                                                               sql.Identifier(tablename),
                                                               sql.Identifier(old_name),
                                                               sql.Identifier(new_name)))
        self.db_connection.commit()


    def join_tables(self, table1, table2, table1_columns, table2_columns, new_table):
        # not complete

        query = sql.SQL("CREATE TABLE {} AS (SELECT * FROM {} t1 JOIN {} t2 ON ").format(sql.Identifier(new_table),
                                                                                         sql.Identifier(table1),
                                                                                         sql.Identifier(table2))

        for i in range(len(table1_columns) - 1):
            query += sql.SQL("t1.{} = t2.{} AND ").format(sql.Identifier(table1_columns[i]),
                                                          sql.Identifier(table2_columns[i]))

        query += sql.SQL("t1.{} = t2.{})").format(sql.Identifier(table1_columns[-1]),
                                                  sql.Identifier(table2_columns[-1]))

        self.db_connection.cursor().execute("SET search_path TO {};".format(self.setid))
        self.db_connection.cursor().execute(query)
        self.db_connection.commit()
