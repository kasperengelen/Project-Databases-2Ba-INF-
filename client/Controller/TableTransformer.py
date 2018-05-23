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
    """

    def __init__(self, setid, db_conn, engine, replace=True):
        self.setid = setid
        self.schema = str(setid)
        self.replace = replace
        self.db_connection = db_conn
        self.engine = engine
        self.history_manager = DatasetHistoryManager(setid, db_conn)

    class TTError(Exception):
        """
        Base exception for all TableTransformer exceptions used to reference to the class exceptions.
        """
        
    class AttrTypeError(TTError):
        """
        This exception is raised whenever an user attempts to perform a transformation on an attribute
        whose type is not supported by the called transformation.
        """

    class ConversionError(TTError):
        """
        This exception is raised whenever an implicit type conversion of an attribute failed because of
        values that aren't possible to convert.
        """

    class ValueError(TTError):
        """
        This exception is raised whenever an operation is provided with an inappropiate value causing
        the operation to fail.
        """
    def get_attribute_type(self, table, attribute):
        """Execute query that returns the type of the attribute of an SQL table in the dataset schema."""
        cur = self.db_connection.cursor()
        cur.execute(("SELECT data_type FROM information_schema.columns WHERE table_schema = %s "
                     "AND table_name = %s AND column_name = %s LIMIT 1"), (self.schema, table, attribute))
        self.db_connection.commit()
        return cur.fetchone()[0]

    def get_attribute_type_with_length(self, table, attribute):
        """Execute query that returns a tuple of the attribute type and length limit of that type if specified.
        For varchar(255) this will yield (character varying, 255), in case of no length limit the value is None."""
        cur = self.db_connection.cursor()
        cur.execute(("SELECT data_type, character_maximum_length FROM information_schema.columns"
                    " WHERE table_schema = %s AND table_name =  %s AND column_name = %s LIMIT 1"),
                    (self.schema, table, attribute))
        self.db_connection.commit()
        return cur.fetchone()

    def drop_attribute(self, schema, table, attribute):
        """Execute query that drops the attribute of an SQL table."""
        cur = self.db_connection.cursor()
        query_args = [sql.Identifier(schema),sql.Identifier(table), sql.Identifier(attribute)]
        cur.execute(sql.SQL("ALTER TABLE {}.{} DROP COLUMN IF EXISTS {}").format(*query_args))
        self.db_connection.commit()

    def rename_attribute(self, table, old, new):
        """Execute auery that renames the column of an SQL table."""
        cur = self.db_connection.cursor()
        query_args = [sql.Identifier(self.schema), sql.Identifier(table),
                      sql.Identifier(old), sql.Identifier(new)]
        cur.execute(sql.SQL('ALTER TABLE {}.{} RENAME {} TO {}').format(*query_args))
        self.db_connection.commit()

    def drop_table(self, table):
        """Execute query that drops an SQL table of the dataset."""
        cur = self.db_connection.cursor()
        query_args = [sql.Identifier(self.schema), sql.Identifier(table)]
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}.{}").format(*query_args))
        self.db_connection.commit()
        
    def create_copy_of_table(self, schema1, tablename1, schema2, tablename2):
        """Execute query that copies a whole table to another table with name new_name."""
        cur = self.db_connection.cursor()
        query_args = [sql.Identifier(schema2), sql.Identifier(tablename2),
                     sql.Identifier(schema1), sql.Identifier(tablename1)]
        cur.execute(sql.SQL("CREATE TABLE {}.{} AS SELECT * FROM {}.{}").format(*query_args))
        self.db_connection.commit()

    def nullify_column(self, tablename, attribute):
        """Execute query that sets empty strings in text columns to NULL in an SQL table."""
        cur = self.db_connection.cursor()
        query_args = [sql.Identifier(self.schema), sql.Identifier(tablename), sql.Identifier(attribute)]
        cur.execute(sql.SQL("UPDATE {0}.{1} SET {2} = NULL WHERE length({2}) = 0").format(*query_args)) 
        self.db_connection.commit()

    def load_table_in_dataframe(self, table):
        """Load all the data of a SQL table in a pandas dataframe."""
        cur = self.db_connection.cursor()
        cur.execute(sql.SQL("SELECT * FROM {}.{}").format(sql.Identifier(self.schema), sql.Identifier(table)))
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=cols)
        return df
    
    def set_to_overwrite(self):
        """Method that changes the behavior of the TableTransformer to overwrite the table when performing modifcations."""
        self.replace = True
        
    def set_to_copy(self):
        """Method that changes the behavior of the TableTransformer to create a new table when performing modifcations."""
        self.replace = False
        
    def get_resulting_table(self, tablename, new_name):
        """Method that returns the name of the table where TableTransformer will operate on. In case of overwrtie it's just tablename,
        but in case it's an operation that needs to result in a new table, we create table 'new_name' and return that name.
        """
        if self.replace is True:
            return tablename
        else:
            resulting_t = self.copy_table(tablename, new_name)
            return resulting_t
            
    def copy_table(self, old, new):
        """In case the transformation has to result in a new table, we copy the existing one to a new table in this dataset
        and perform the operation on this newly created copy.

        Parameters:
           old: A string representing the name of the old table we're constructing the new one from.
           new: A string representing the name of the new table constructed after performing a transformation.
        """
        if new == "":
            raise self.ValueError('No tablename given to the new table resulting from this operation. Please assign a valid tablename.')

        new_name = self.__get_unique_name(new, new, False)
        query_args = [self.schema, old, self.schema, new_name]
        self.create_copy_of_table(*query_args)
        self.history_manager.write_to_history(resulting_table, old, attribute, [], 0)
        return new_name

    def change_attribute_name(self, table, attribute, new_name):
        """Transformation that changes the name of a table attribute."""
        new_name = self.__get_unique_name(table, new_name)
        self.rename_attribute(self, table, attribute, new_name)
        self.history_manager.write_to_history(table, table, attribute, [new_name], 17)

    def delete_attribute(self, tablename, attribute, new_name=""):
        """Transformation that deletes an attribute of a table."""
        resulting_table = self.get_resulting_table(tablename, new_name)
        self.drop_attribute(self.schema, resulting_table, attribute)
        self.history_manager.write_to_history(resulting_table, resulting_table, attribute, [], 2)

    def __create_predicate_piece(self, plist):
        predicate = ''
        if plist[1] in ['=', '!='] and plist[2] == '"null"':
            if plist[1] == '=':
                plist[1] = 'is'
            else:
                plist[1] = 'is not'
            plist[2] = 'NULL'

        return [sql.Identifier(plist[0]), plist[1], plist[2]]
    
    def delete_rows_using_predicate_logic(self, tablename, arg_list, new_name=""):
        """Method to delete rows by using provided predicates like "attribute > x AND attribute != y".

        Parameters:
            arg_list: A list of strings containing the strings representing the predicates (Identifiers, logical operators).
        """
        resulting_table  = self.get_resulting_table(tablename, new_name)
        list_size = len(arg_list)
        identifiers = [sql.Identifier(self.schema), sql.Identifier(resulting_table)]
        values = []
        delete_query = 'DELETE FROM {}.{} WHERE'
        # We get list of form ['x', '<', 'y'] or  ['x', '<', 'y', 'AND', 'x', '>','z'] et cetera
        if ((list_size - 3) % 4) == 0:
            i = 0
            while i < list_size:
                cur_list = self.__create_predicate_piece(arg_list[i:i+3])
                delete_query += ' {} ' + cur_list[1] + ' ' + '%s'
                identifiers.append(cur_list[0])
                values.append(cur_list[2])
                if list_size > 3 and (i + 4) < list_size: #There should be an AND / OR following
                    i += 4
                    delete_query += ' ' +  arg_list[i-1] + ' '
                else: #Only one clause specified or no more clauses following current clause
                    break

        else:
            raise self.ValueError('Can not delete rows because an invalid predicate has been provided.')

        try:
            self.db_connection.cursor().execute(sql.SQL(delete_query).format(*identifiers), values)
        except:
            if resulting_table != tablename:
                self.db_connection.commit()
                self.drop_table(resulting_table)
            raise self.ValueError('Could not delete rows using this predicate since it contains invalid input value(s) for the attributes.')
        
        self.db_connection.commit()
        #clean_predicate = predicate.replace('"', '\\"') #Escape double quotes to pass it in postgres array
        self.history_manager.write_to_history(resulting_table, resulting_table, 'None', ['temp'], 15)

    def get_conversion_options(self, tablename, attribute):
        """Returns a list of supported types that the given attribute can be converted to."""
        data_type = self.get_attribute_type(tablename, attribute)
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
    
    def __convert_character(self, tablename, attribute, to_type, data_format, length):
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

        if temp in ['CHAR' , 'VARCHAR'] and length is not None: #Char and varchar don't need special parameters
            to_type = to_type.replace('n', str(length))
            casting_var = to_type
        else:
            #The empty strings should become nulls for conversion purposes
            self.nullify_column(tablename, attribute)
            
        sql_query = "ALTER TABLE {}.{} ALTER COLUMN {} TYPE " + casting_var
        cur = self.db_connection.cursor()
        try:
            cur.execute(sql.SQL(sql_query).format(sql.Identifier(self.schema), sql.Identifier(tablename),
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

    def __convert_numeric(self, tablename, attribute, to_type, length):
        """Conversion of "numeric" things (INTEGER and FLOAT, DATE, TIME, TIMESTAMP)"""
        if to_type in ['VARCHAR(n)', 'CHAR(n)']:
            to_type = to_type.replace('n', str(length))
        sql_query = "ALTER TABLE {}.{} ALTER COLUMN  {} TYPE %s" % to_type
        self.db_connection.cursor().execute(sql.SQL(sql_query).format(sql.Identifier(self.schema), sql.Identifier(tablename),
                                                                                              sql.Identifier(attribute)), [to_type])
        self.db_connection.commit()
        return to_type
    
    def change_attribute_type(self, tablename, attribute, to_type, data_format="", length=None, new_name=""):
        """Change the type of a table attribute.

        Parameters:
            to_type: A string representing the PostreSQL type we want to convert to, like INTEGER or VARCHAR(255)
            data_format: Optional parameter for when the attribute has to follow a specific format, like a DATE with format 'DD/MM/YYYY'
            length: This specifies the size of a char or varchar if it the user wants to place a specific limit.
            new_name: The name for the new table constructed from this operation if the TableTransformer is not set to overwrite
        """
        cur_type = self.get_attribute_type(tablename, attribute)
        resulting_table = self.get_resulting_table(tablename, new_name)
        try:
            if SQLTypeHandler().is_string(cur_type):
                to_type = self.__convert_character(resulting_table, attribute, to_type, data_format, length)
            else:
                to_type = self.__convert_numeric(resulting_table, attribute, to_type, length)
        except:
            self.db_connection.commit()
            #If anything went wrong in the transformation, the new table has to be dropped.
            if resulting_table != tablename:
                self.drop_table(resulting_table)
            #Reraise the exception for the higher level caller
            raise
        
        self.history_manager.write_to_history(resulting_table, resulting_table, attribute, [to_type, data_format, length], 1)

    def force_attribute_type(self, tablename, attribute, to_type, data_format="", length=None, force_mode=True, new_name=""):
        """In case that change_attribute_type fails due to elements that can't be converted
        this method will force the conversion by deleting the row containing the bad element.
        The parameters are identical to change_attribute_type().
        """
        attr_type = self.get_attribute_type(tablename, attribute)
        resulting_table = self.get_resulting_table(tablename, new_name)
        if resulting_table != tablename:
            self.set_to_overwrite() #The next call should overwrite the newly created table.
        #If the attribute is not of string type then forcing will have no effect, so we can proceed as normal.
        if SQLTypeHandler().is_string(attr_type) is False:
            self.change_attribute_type(resulting_table, attribute, to_type, data_format, length, new_name)
            return
        
        pattern = ""
        if to_type == 'INTEGER':
            pattern = '^[-+]?[0-9]+$'
            
        elif to_type == 'FLOAT':
            pattern = '^[-+.]?[0-9]+[.]?[e]?[-+]?[0-9]*$'

        elif to_type in ['DATE', 'TIMESTAMP', 'TIME']:
            data_format = self.__readable_format_to_postgres(to_type, data_format)
            pattern = self.__get_datetime_regex(to_type, data_format)

        cur = self.db_connection.cursor()
        query_args = [sql.Identifier(self.schema), sql.Identifier(resulting_table), sql.Identifier(attribute)]

        if force_mode is True:
            query1 = "DELETE FROM {}.{} WHERE ({} !~ %s )"
            query2 = "DELETE FROM {}.{} WHERE char_length({}) < %s"

        else:
            query1 = "UPDATE {0}.{1} SET {2} = NULL WHERE ({2} !~ %s )"
            query2 = "UPDATE {0}.{1} SET {2} = NULL WHERE char_length({}) < %s"
            

        if pattern != "":
            cur.execute(sql.SQL(query1).format(*query_args), [pattern])
            self.db_connection.commit()

        if length is not None:
            cur.execute(sql.SQL(query2).format(*query_args), [length])
            self.db_connection.commit()
            
        #If we were to create a new table for this operation, this already happened, so overwrite the newly created table.
        if self.replace is False:
            self.set_to_overwrite
        self.change_attribute_type(resulting_table, attribute, to_type,  data_format, length, new_name)

    def find_and_replace(self, tablename, attribute, value, replacement, exact=True, replace_all=True, new_name=""):
        """Method that finds values and replaces them with the provided argument. This wraps around an internal
        method that does the heavy work.

        Parameters:
            value: The value that needs to found so it can be replaced.
            replacement: The value that will replace the values that were found.
            exact : A boolean indicating if we want to match whole-words only or allowing substring matching
            replace_all : A boolean indicating whether a found substring should be replaced or the string should be replaced.
                          True replaces the whole string, False replaces the found substring with the replacement.
        """
        resulting_table = self.get_resulting_table(tablename, new_name)
        args = locals()
        params = {k:v for k,v in args.items() if k not in ['self', 'tablename', 'new_name']}
        try:
            self.__execute_normal_find_and_replace(**params)

        except:
            if resulting_table != tablename:
                self.db_connection.commit()
                self.drop_table(resulting_table)
            raise
            
    def __execute_normal_find_and_replace(self, resulting_table, attribute, value, replacement, exact, replace_all):
        """Internal method that executes the wanted behavior specified in find_and_replace."""
        cur_type = self.get_attribute_type(resulting_table, attribute)
        
        original_value = value            
        if exact is True:
            sql_query = "UPDATE {0}.{1} SET {2} = %s WHERE {2} = %s"
        elif exact is False:
            if value.isalnum() is not True: #Only alphanumerical substrings are supported
                raise self.ValueError("Values not containing alphanumerical characters can not be used for substring matching. "
                                      "Please use whole-word matching to find and replace the values.")

            if SQLTypeHandler().is_string(cur_type) is False:
                raise self.AttrTypeError("Substring matching is only possible with character strings. "
                                         "Please convert the attribute to a character string type.")
                
            sql_query = "UPDATE {0}.{1} SET {2} = %s WHERE {2} LIKE %s"
            value = '%{}%'.format(value)

            if replace_all is False: #We have to replace the substring and this is done with a different query.
                sql_query = "UPDATE {0}.{1} SET {2} = replace({2}, %s, %s) WHERE {2} LIKE %s"
                cur = self.db_connection.cursor()
                cur.execute(sql.SQL(sql_query).format(sql.Identifier(self.schema), sql.Identifier(resulting_table),
                                                      sql.Identifier(attribute)), (original_value, replacement, value))
                self.db_connection.commit()
                
        if replace_all is True: #If replace_all was False the operation was already performed by the query above.           
            try:
                self.db_connection.cursor().execute(sql.SQL(sql_query).format(sql.Identifier(self.schema), sql.Identifier(resulting_table),
                                                                              sql.Identifier(attribute)), (replacement, value))
            except psycopg2.DataError:
                raise self.ValueError("Could not perform find-and-replace due to an invalid input value for this attribute.")
            self.db_connection.commit()
        self.history_manager.write_to_history(resulting_table, resulting_table, attribute, [original_value, replacement, exact, replace_all], 8)
        
    def regex_find_and_replace(self, tablename, attribute, regex, replacement, case_sens=False, new_name=""):
        """Method that finds values with a provided regex and replaces them with a provided replacement.

        Parameters:
            regex: A string that is a POSIX compliant regex to match all the searched values.
            replacement: A replacement for the found values.
            case_sens: A boolean indicating whether the regex is case sensitive. True for sensitive, False for insensitive.
            new_name: The name of the new table if the TableTransformer is not set to overwrite.
        """
        resulting_table = self.get_resulting_table(tablename, new_name)
        args = locals()
        params = {k:v for k,v in args.items() if k not in ['self', 'tablename', 'new_name']}
        try:
            self.__execute_regex_find_and_replace(**params)

        except:
            if resulting_table != tablename:
                self.db_connection.commit()
                self.drop_table(resulting_table)
            raise

    def __execute_regex_find_and_replace(self, resulting_table, attribute, regex, replacement, case_sens):
        """Internal method that executes the wanted behavior specified in regex_find_and_replace."""
        cur_type = self.get_attribute_type(resulting_table, attribute)
        if SQLTypeHandler().is_string(cur_type) is False:
            raise self.AttrTypeError("Find-and-replace using regular epxressions is only possible with character type attributes. "
                                     "Please convert the needed attribute to VARCHAR or CHAR.")
        
        if case_sens is False:
            sql_query = "UPDATE {0}.{1} SET {2} = %s WHERE {2} ~* %s"
        elif case_sens is True:
            sql_query = "UPDATE {0}.{1} SET {2} = %s WHERE {2} ~ %s"

        try:
            self.db_connection.cursor().execute(sql.SQL(sql_query).format(sql.Identifier(self.schema), sql.Identifier(resulting_table),
                                                                          sql.Identifier(attribute)), (replacement, regex))
        except psycopg2.DataError as e:
            error_msg = str(e)  + ". Please refer to the PostgreSQL documentation on regular expressions for more information."
            raise self.ValueError(error_msg)

        self.db_connection.commit()
        self.history_manager.write_to_history(resulting_table, resulting_table, attribute, [regex, replacement, case_sens], 9)
        
    def __get_simplified_types(self, tablename, data_frame):
        """Method that makes sure pandas dataframe uses correct datatypes when writing to SQL."""
        new_attributes = data_frame.columns.values.tolist()
        new_types = {}
        sqla_type = None
        for  elem in new_attributes:
            psql_type = self.get_attribute_type_with_length(tablename, str(elem))
            if psql_type is not None:
                sqla_type = SQLTypeHandler().to_sqla_object(psql_type[0])
                if sqla_type is None:
                    if psql_type[0] == 'character varying':
                        sqla_type = sqlalchemy.types.VARCHAR(psql_type[1])
                    elif psql_type[0] == 'character':
                        sqla_type = sqlalchemy.types.CHAR(psql_type[1])
                    else:
                        #Then this is another type that won't bring us trouble.
                        continue
            else:
                pd_type = str(data_frame[elem].dtype)
                sqla_type = SQLTypeHandler().to_sqla_object(pd_type)
                
            new_types[elem] = sqla_type
        return new_types
    
    def one_hot_encode(self, tablename, attribute, new_name=""):
        """Method that performs one hot encoding given an attribute"""
        df = self.load_table_in_dataframe(tablename)
        encoded = pd.get_dummies(df[attribute]) #Perfom one-hot-encoding
        df = df.drop(attribute, axis=1) #Drop the attribute used for encoding
        df = df.join(encoded) #Join the original attributes with the encoded table
        new_dtypes = self.__get_simplified_types(tablename, df)
        eventual_table = "" #This will be the name of table containing the changes.
        if self.replace is True:
            #If the table should be replaced, drop it and recreate it.
            df.to_sql(tablename, self.engine, self.schema, if_exists='replace', index = False, dtype = new_dtypes)
            eventual_table = tablename
        elif self.replace is False:
            #We need to create a new table and leave the original untouched
            new_name = self.__get_unique_name("", new_name, False)
            df.to_sql(new_name, self.engine, self.schema, if_exists='fail', index = False, dtype = new_dtypes)
            eventual_table = new_name
            
        self.history_manager.write_to_history(eventual_table, eventual_table, attribute, [], 14)
        
    def __calculate_zscore(self, mean, standard_dev, value):
        """Method to quickly calculate z-scores"""
        zscore = (value - mean) / standard_dev
        return zscore
    
    def normalize_using_zscore(self, tablename, attribute, overwrite=True, new_name = ""):
        """Method that normalizes the values of an attribute using the z-score.
        This will normalize everything in a 1-point range, thus [0-1].

        Parameter:
            overwrite: Boolean indicating whether the normalization should overwrite the column
                       True overwrites the column with normalized data, False creates a new column
        """
        #Let's check if the attribute is a numeric type, this should not be performed on non-numeric types
        attr_type = self.get_attribute_type(tablename, attribute)
        if SQLTypeHandler().is_numerical(attr_type) is False:
            raise self.AttrTypeError("Normalization failed due attribute not being of numeric type (neither integer or float)")
        
        df = self.load_table_in_dataframe(tablename)
        og_attribute = attribute
        if overwrite is False:
            new_col = attribute + '_normalized'
            new_col = self.__get_unique_name(tablename, new_col)
            df[new_col] = df[attribute]
            # Now reference new_col as the attribute used in this method
            attribute = new_col
            
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
            df.to_sql(tablename, self.engine, self.schema, 'replace', index = False, dtype = new_dtypes)
            eventual_table = tablename
        elif self.replace is False:
            #We need to create a new table and leave the original untouched
            new_name = self.__get_unique_name("", new_name, False)
            df.to_sql(new_name, self.engine, self.schema, 'fail', index = False, dtype = new_dtypes)
            eventual_table = new_name

        self.history_manager.write_to_history(eventual_table, eventual_table, attribute, [overwrite, og_attribute], 13)
        
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
        max_nr = 0

        for elem in name_list:
            if name in elem:
                if len(elem) == name_size: #If it's the same size, it's the string itself
                    count += 1
                    continue #Looking for the next characters is pointless. So continue
                if (elem[name_size] == '_') and (str.isdigit(elem[name_size+1:])):
                    count += 1
                    number = int(elem[name_size+1:])
                    if  number > max_nr:
                        max_nr = number

        if count == 0:
            return name
        else:
            new_name = name + "_" + str(max_nr + 1)
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
        attr_type = self.get_attribute_type(tablename, attribute)
        if SQLTypeHandler().is_numerical(attr_type) is False:
            raise self.AttrTypeError("Discretization failed due attribute not being of numeric type (neither integer or float)")

        integer_bool = SQLTypeHandler().is_integer(attr_type)
        df = self.load_table_in_dataframe(tablename)
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
            df.to_sql(tablename, self.engine, self.schema, 'replace', index = False, dtype = new_dtypes)
            eventual_table = tablename
        elif self.replace is False:
            #We need to create a new table and leave the original untouched
            new_name = self.__get_unique_name("", new_name, False)
            df.to_sql(new_name, self.engine, self.schema, 'fail', index = False, dtype = new_dtypes)
            eventual_table = new_name
        
        self.history_manager.write_to_history(eventual_table, eventual_table, attribute, [], 6)

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
        attr_type = self.get_attribute_type(tablename, attribute)
        if SQLTypeHandler().is_numerical(attr_type) is False:
            raise self.AttrTypeError("Discretization failed due attribute not being of numeric type (neither integer or float)")

        round_to_int = True
        df = self.load_table_in_dataframe(tablename)
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
            df.to_sql(tablename, self.engine, self.schema, 'replace', index = False, dtype = new_dtypes)
            eventual_table = tablename
        elif self.replace is False:
            #We need to create a new table and leave the original untouched
            new_name = self.__get_unique_name("", new_name, False)
            df.to_sql(new_name, self.engine, self.schema, 'fail', index = False, dtype = new_dtypes)
            eventual_table = new_name
            
        self.history_manager.write_to_history(eventual_table, eventual_table, attribute, [], 5)
        
    def discretize_using_custom_ranges(self, tablename, attribute, ranges, exclude_right=True, new_name=""):
        """Method that discretizes given a a list representing the bins.

        Parameters:
            ranges: A python list that represents the bins that the user has provided
            exclude_right: A boolean indicating whether the rightmost edge should be included
                           True if the rightmost edge is excluded [X - Y[, False if rightmost edge is included ]X - Y]
        """
        attr_type = self.get_attribute_type(tablename, attribute)
        if SQLTypeHandler().is_numerical(attr_type) is False:
            raise self.AttrTypeError("Discretization failed due attribute not being of numeric type (neither integer or float)")

        df = self.load_table_in_dataframe(tablename)
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
            df.to_sql(tablename, self.engine, self.schema, 'replace', index = False, dtype = new_dtypes)
            eventual_table = tablename
        elif self.replace is False:
            #We need to create a new table and leave the original untouched
            new_name = self.__get_unique_name("", new_name, False)
            df.to_sql(new_name, self.engine, self.schema, 'fail', index = False, dtype = new_dtypes)
            eventual_table = new_name

        self.history_manager.write_to_history(eventual_table, eventual_table, attribute, [ranges, exclude_right], 4)
        
    def delete_outliers(self, tablename, attribute, larger, value, replacement, new_name=""):
        """Method that gets rid of outliers of an attribute by setting them to null.

        Parameters:
            larger: A boolean indicating if we need to delete values larger or smaller than the provided value
                    True deletes all the values larger, False deletes all the values smaller.
            value: The value used to determine whether an element is an outlier.
            replacement: The value that will be used instead of the outliers
        """
        resulting_table = self.get_resulting_table(tablename, new_name)
        #Let's check if the attribute is a numeric type, this should not be performed on non-numeric types
        attr_type = self.get_attribute_type(tablename, attribute)
        if SQLTypeHandler().is_numerical(attr_type) is False:
            raise self.AttrTypeError("Deleting outliers failed due attribute not being of numeric type (neither integer or float)")
            
        if larger is True:
            comparator = '>'
        else:
            comparator = '<'
        #Create query for larger/smaller deletion of outlier
        sql_query = "UPDATE {0}.{1} SET {2} = %s  WHERE {2} cmp %s".replace('cmp', comparator)
        cur = self.db_connection.cursor()
        cur.execute(sql.SQL(sql_query).format(sql.Identifier(self.schema), sql.Identifier(resulting_table),
                                              sql.Identifier(attribute)), (replacement, value))
        self.db_connection.commit()
        self.history_manager.write_to_history(resulting_table, resulting_table, attribute, [larger, value, replacement], 3)
        
    def __fill_nulls_with_x(self, attribute, table,  x):
        """Method that fills null values of an attribute with a provided value.

        Parameter:
            x: The value that is going to be used to fill all the nulls with.
        """
        query_args = [sql.Identifier(self.schema), sql.Identifier(table), sql.Identifier(attribute)]
        self.db_connection.cursor().execute(sql.SQL("UPDATE {0}.{1} SET {2} = %s  WHERE {2} is null").format(*query_args), [x])
        self.db_connection.commit()
        
    def fill_nulls_with_mean(self, tablename, attribute, new_name=""):
        """Method that fills null values of an attribute with the mean."""
        resulting_table = self.get_resulting_table(tablename, new_name)
        #Let's check if the attribute is a numeric type, this should not be performed on non-numeric types
        attr_type = self.get_attribute_type(tablename, attribute)
        if SQLTypeHandler().is_numerical(attr_type) is False:
            raise self.AttrTypeError("Filling nulls failed due attribute not being of numeric type (neither integer or float)")

        df = self.load_table_in_dataframe(tablename)
        mean = df[attribute].mean()

        self.__fill_nulls_with_x(attribute, resulting_table, mean)
        self.history_manager.write_to_history(resulting_table, resulting_table, attribute, [mean], 10)


    def fill_nulls_with_median(self, tablename, attribute, new_name=""):
        """Method that fills null values of an attribute with the median."""
        resulting_table = self.get_resulting_table(tablename, new_name)
        #Let's check if the attribute is a numeric type, this should not be performed on non-numeric types
        attr_type = self.get_attribute_type(tablename, attribute)
        if SQLTypeHandler().is_numerical(attr_type) is False:
            raise self.AttrTypeError("Filling nulls failed due attribute not being of numeric type (neither integer or float)")

        df = self.load_table_in_dataframe(tablename)
        median = df[attribute].median()

        self.__fill_nulls_with_x(attribute, resulting_table, median)
        self.history_manager.write_to_history(resulting_table, resulting_table, attribute, [median], 11)


    def fill_nulls_with_custom_value(self, tablename, attribute, value, new_name=""):
        """Method that fills null values of an attribute with a custom value"""
        resulting_table = self.get_resulting_table(tablename, new_name)
        #Let's check if the attribute is a numeric type, this should not be performed on non-numeric types
        attr_type = self.get_attribute_type(tablename, attribute)
        if SQLTypeHandler().is_numerical(attr_type) is False:
            raise self.AttrTypeError("Filling nulls failed due attribute not being of numeric type (neither integer or float)")

        #Perhaps do a sanity check here? I'll see later on.
        self.__fill_nulls_with_x(attribute, resulting_table, value)
        self.history_manager.write_to_history(resulting_table, resulting_table, attribute, [value], 12)

    
    def extract_part_of_date(self, tablename, attribute, extraction_arg, new_name=""):
        """Method that extracts part of a date, time, or datetime"""
        raise self.AttrTypeError(attr_type)
        resulting_table = self.get_resulting_table(tablename, new_name)
        attr_type = self.get_attribute_type(tablename, attribute)
        if SQLTypeHandler().is_date_type(attr_type) is False:
            raise self.AttrTypeError("Extraction of date part failed due attribute not being a date type (neither DATE or TIMESTAMP).")

        if extraction_arg not in ['YEAR', 'MONTH + YEAR', 'MONTH', 'DAY OF THE WEEK']:
            raise self.ValueError('Extraction argument is not supported by TableTransformer')

        attr_name = attribute + '_part'
        attr_name = self.__get_unique_name(tablename, attr_name, True)
        #Let's first create this new column that will contain the extracted part of the date
        cur = self.db_connection.cursor()
        query_args = [sql.Identifier(self.schema), sql.Identifier(resulting_table), sql.Identifier(attr_name)]
        cur.execute(sql.SQL("ALTER TABlE {}.{} ADD COLUMN  {} VARCHAR(255)").format(*query_args))

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

        cur.execute(sql.SQL(query).format(sql.Identifier(self.schema), sql.Identifier(resulting_table),
                                          sql.Identifier(attr_name), sql.Identifier(attribute)))
        self.db_connection.commit()
        self.history_manager.write_to_history(resulting_table, resulting_table, attribute, [extraction_arg], 7)
