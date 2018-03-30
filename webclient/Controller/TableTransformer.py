import pandas as pd
from psycopg2 import sql
from sqlalchemy import create_engine

class TableTransformer:

    def __init__(self, userid, setid, db_conn, engine, replace=True):
        self.userid = userid
        self.setid = setid
        self.replace = replace
        #Get the psycopg2 database connection to execute SQL queries
        self.db_connection = db_conn
        #Get the SQLalchemy engine to use pandas functionality
        self.engine = engine


    
    class AttrTypeError(Exception):
        """
        This exception is raised whenever an user attempts to perform a transformation on an attribute
        whose type is not supported by the called transformation
        """

    class ConversionError(Exception):
        """
        This exception is raised whenever an implicit type conversion of an attribute failed because of
        values that aren't possible to convert
        """
        
    

    """Extra option to check whether deleting the attribute will destroy integrity constraints.
    Checks for other constraints is possible as well"""
    def __integrity_check(self, tablename, setid):
        #implemented in secret file
        pass

    #Method to change the transformation behavior to overwrite the table with the changes
    def set_to_overwrite(self):
        self.replace = True

    #Method to change the transformation behavior to create a new table with the changes
    def set_to_copy(self):
        self.replace = False


    # Get the internal reference for the table of (setid) and (tablename). This returns a pair (id, name)
    def get_internal_reference(self, tablename):
            return (str(self.setid), tablename)

    # In case the transformation has to result in a new table, we copy the existing one
    def copy_table(self, internal_ref, new_name):
        #Execute the query this way to avoid SQL injections
        self.db_connection.cursor().execute(sql.SQL("CREATE TABLE {}.{} AS SELECT * FROM {}.{}").format(sql.Identifier(internal_ref[0]),sql.Identifier(new_name),
                                                                                          sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1])))
        self.db_connection.commit()
        return (internal_ref[0], new_name)

        
    # Delete an attribute of a table
    def delete_attribute(self, tablename, attribute, new_name=""):
        internal_ref = self.get_internal_reference(tablename)
        if self.replace is False:
            internal_ref = self.copy_table(internal_ref, new_name)
        self.db_connection.cursor().execute(sql.SQL("ALTER TABLE {}.{} DROP COLUMN {}").format(sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1]),
                                                                                    sql.Identifier(attribute)))
        self.db_connection.commit()


    # Returns a list of supported types to convert to given a data_type
    def get_conversion_options(self, tablename, attribute):
        data_type = self.get_attribute_type(tablename, attribute)[0]
        options = { 'character varying' : ['CHAR(255)', 'INTEGER', 'FLOAT', 'DATE', 'TIME', 'TIMESTAMP'],
                    'character'         : ['VARCHAR(255)', 'INTEGER', 'FLOAT', 'DATE', 'TIME', 'TIMESTAMP'],
                    'integer'           : ['CHAR(255)', 'VARCHAR(255)', 'FLOAT'],
                    'double precision'  : ['CHAR(255)', 'VARCHAR(255)', 'INTEGER'] ,
                    'date'              : ['CHAR(255)', 'VARCHAR(255)'],
                    'time without time zone' : ['CHAR(255)', 'VARCHAR(255)'],
                    'timestamp without time zone' : ['CHAR(255)', 'VARCHAR(255)']
                    }
        #In case it's a data type unknown to this class, we can almost certainly convert to varchar(255)
        return options.setdefault(data_type, ['VARCHAR(255)'])

    # Return the postgres data type of an attribute
    def get_attribute_type(self, tablename, attribute):
        internal_ref = self.get_internal_reference(tablename)
        cur = self.db_connection.cursor()
        cur.execute(sql.SQL("SELECT pg_typeof({}) FROM {}.{}").format(sql.Identifier(attribute), sql.Identifier(internal_ref[0]),
                                                                                   sql.Identifier(internal_ref[1])))
        return (cur.fetchone()[0], internal_ref)


    # Conversion of "numeric" things (INTEGER and FLOAT, DATE, TIME, TIMESTAMP)
    def __convert_numeric(self, internal_ref, attribute, to_type):
        sql_query = "ALTER TABLE {}.{} ALTER COLUMN  {} TYPE %s" % to_type
        self.db_connection.cursor().execute(sql.SQL(sql_query).format(sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1]),
                                                                                              sql.Identifier(attribute)), [to_type])
        self.db_connection.commit()
        

    # Conversion of a character type (VARCHAR and CHAR)
    def __convert_character(self, internal_ref, attribute, to_type, data_format):
        patterns = { 
                     'INTEGER'      : 'INTEGER USING %s::integer%s',
                     'FLOAT'        : 'FLOAT USING %s::float%s',
                     'DATE'         : 'DATE USING to_date(\"%s\" , %s)',
                     'TIME'         : 'TIME USING to_timestamp(\"%s\", %s)::time',
                     'TIMESTAMP'    : 'TIMESTAMP USING to_timestamp(\"%s\", %s)'
                     }
        temp = patterns.setdefault(to_type, '')
        if temp == '':
            casting_var = to_type
        else:
            casting_var = temp % (attribute, data_format)
            
        sql_query = "ALTER TABLE \"%s\".\"%s\" ALTER COLUMN \"%s\" TYPE " % (internal_ref[0], internal_ref[1], attribute)
        sql_query += casting_var
        self.db_connection.cursor().execute(sql_query)
        self.db_connection.commit()



    # Change the attribute type, if the data follows a specific format like TIMESTAMP provide it as well.
    def change_attribute_type(self, tablename, attribute, to_type, data_format="", new_name=""):
        cur_type, internal_ref  = self.get_attribute_type(tablename, attribute)
        if self.replace is False:
            internal_ref = self.copy_table(internal_ref, new_name)
        
        if (cur_type == 'character varying') or (cur_type == 'character'):
            self.__convert_character(internal_ref, attribute, to_type, data_format)

        else:
            self.__convert_numeric(internal_ref, attribute, to_type)


    # In case that change_attribute_type fails due to elements that can't be converted
    # this method will force the conversion by a) setting
    def force_attribute_type(self, tablename, attribute, to_type, data_format="", new_name=""):
        if self.replace is True:
            internal_ref = self.get_internal_reference(tablename)
        else:
            internal_ref = self.get_internal_reference(new_name)
        
        pattern = ""
        if to_type == 'INTEGER':
            pattern = '^[-+]?[0-9]+$'
            
        elif to_type == 'FLOAT':
            pattern = '^[-+.]?[0-9]+[.]?[e]?[-+]?[0-9]*$'

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




    #Method that finds all the exact matches (value = value) and replaces it with the provided value
    def find_and_replace(self, tablename, attribute, value, replacement, new_name=""):
        cur_type, internal_ref  = self.get_attribute_type(tablename, attribute)
        if self.replace is False:
            internal_ref = self.copy_table(internal_ref, new_name)

        # Possible danger of SQL injections, so we pass value and replacement like this
        self.db_connection.cursor().execute(sql.SQL("UPDATE {}.{} SET  {} = %s WHERE {} = %s").format(sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1]),
                                                                                           sql.Identifier(attribute), sql.Identifier(attribute)), (replacement, value))
        self.db_connection.commit()

        
    #Method that performs one hot encoding given an attribute
    def one_hot_encode(self, tablename, attribute, new_name=""):
        internal_ref = self.get_internal_reference(tablename)
        #Read the table in a dataframe
        sql_query = "SELECT * FROM \"{}\".\"{}\"".format(*internal_ref)
        df = pd.read_sql(sql_query, self.engine)
        #Perfom one-hot-encoding
        encoded = pd.get_dummies(df[attribute])
        #Drop the attribute used for encoding
        df = df.drop(attribute, axis=1)
        #Join the original attributes with the encoded table
        df = df.join(encoded)
        if self.replace is True:
            #If the table should be replaced, drop it and recreate it.
            df.to_sql(tablename, self.engine, None, internal_ref[0], 'replace', index = False)
        elif self.replace is False:
            #We need to create a new table and leave the original untouched
            df.to_sql(new_name, self.engine, None, internal_ref[0], 'fail', index = False)

    #Method to quickly calculate z-scores
    def __calculate_zscore(self, mean, standard_dev, value):
        zscore = (value - mean) / standard_dev
        return zscore
            

    #Method that normalizes the values of an attribute using the z-score.
    #This will normalize everything in a 1-point range, thus [0-1]
    def normalize_using_zscore(self, tablename, attribute, new_name = ""):
        copy_made = False #If we are still
        #Let's check if the attribute is a numeric type, this should not be performed on non-numeric types
        attr_type = self.get_attribute_type(tablename, attribute)
        if attr_type not in ['integer', 'double precision']:
            raise self.AttrTypeError("Normalization failed due attribute not being of numeric type (neither integer or float)")
        internal_ref = self.get_internal_reference(tablename)
        #Read the table in a dataframe
        sql_query = "SELECT * FROM \"{}\".\"{}\"".format(*internal_ref)
        df = pd.read_sql(sql_query, self.engine)
        #Calculate the mean
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
        #Divide all the values by 4 to get a 1-point range
        column = column.divide(4)
        #Now the mean is 0, so add 0.5 to have the mean at 0.5
        column = column.add(0.5)
        #Update these changes to the dataframe
        df.update(column)

        if self.replace is True:
            #If the table should be replaced, drop it and recreate it.
            df.to_sql(tablename, self.engine, None, internal_ref[0], 'replace', index = False)
        elif self.replace is False:
            #We need to create a new table and leave the original untouched
            df.to_sql(new_name, self.engine, None, internal_ref[0], 'fail', index = False)
        return column
        
        
        

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

        print(query)

        self.db_connection.cursor().execute("SET search_path TO {};".format(self.setid))
        self.db_connection.cursor().execute(query)
        self.db_connection.commit()


if __name__ == '__main__':
    tt = TableTransformer(1, 1, None, None)
    
