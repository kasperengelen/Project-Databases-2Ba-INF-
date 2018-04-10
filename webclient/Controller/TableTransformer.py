import pandas as pd
import math
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine

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
        """Inits the TableTransformer with provided values"""
        self.setid = setid
        self.replace = replace
        self.db_connection = db_conn
        self.engine = engine


    
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



    def __integrity_check(self, tablename, setid):
        """Extra option to check whether deleting the attribute will destroy integrity constraints.
        Checks for other constraints is possible as well
        """
        pass

    
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
        #Execute with the dynamic SQL module of psycopg2 to avoid SQL injecitons
        self.db_connection.cursor().execute(sql.SQL("CREATE TABLE {}.{} AS SELECT * FROM {}.{}").format(sql.Identifier(internal_ref[0]),sql.Identifier(new_name),
                                                                                          sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1])))
        self.db_connection.commit()
        return (internal_ref[0], new_name)

        
    
    def delete_attribute(self, tablename, attribute, new_name=""):
        """Delete an attribute of a table"""
        internal_ref = self.get_internal_reference(tablename)
        if self.replace is False:
            internal_ref = self.copy_table(internal_ref, new_name)
        self.db_connection.cursor().execute(sql.SQL("ALTER TABLE {}.{} DROP COLUMN {}").format(sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1]),
                                                                                    sql.Identifier(attribute)))
        self.db_connection.commit()


    def get_supported_types(self):
        """Quick method that returns all the types supported by the TableTransformer for conversion purposes"""
        return ['VARCHAR(255)', 'CHAR(255)', 'INTEGER', 'FLOAT', 'DATE', 'TIME', 'TIMESTAMP']


    def get_conversion_options(self, tablename, attribute):
        """Returns a list of supported types that the given attribute can be converted to."""
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

    def get_attribute_type(self, tablename, attribute):
        """Return the postgres data type of an attribute."""
        internal_ref = self.get_internal_reference(tablename)
        cur = self.db_connection.cursor()
        cur.execute(sql.SQL("SELECT pg_typeof({}) FROM {}.{}").format(sql.Identifier(attribute), sql.Identifier(internal_ref[0]),
                                                                                   sql.Identifier(internal_ref[1])))
        return (cur.fetchone()[0], internal_ref)


    def __convert_numeric(self, internal_ref, attribute, to_type):
        """Conversion of "numeric" things (INTEGER and FLOAT, DATE, TIME, TIMESTAMP)"""
        sql_query = "ALTER TABLE {}.{} ALTER COLUMN  {} TYPE %s" % to_type
        self.db_connection.cursor().execute(sql.SQL(sql_query).format(sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1]),
                                                                                              sql.Identifier(attribute)), [to_type])
        self.db_connection.commit()
        

    
    def __convert_character(self, internal_ref, attribute, to_type, data_format):
        """Conversion of a character type (VARCHAR and CHAR)"""
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



    def change_attribute_type(self, tablename, attribute, to_type, data_format="", new_name=""):
        """Change the type of a table attribute.

        Parameters:
            to_type: A string representing the PostreSQL type we want to convert to, like INTEGER or VARCHAR(255)
            data_format: Optional parameter for when the attribute has to follow a specific format, like a DATE with format 'DD/MM/YYYY'
            new_name: The name for the new table constructed from this operation if the TableTransformer is not set to overwrite
        """
        cur_type, internal_ref  = self.get_attribute_type(tablename, attribute)
        if self.replace is False:
            internal_ref = self.copy_table(internal_ref, new_name)
        
        if (cur_type == 'character varying') or (cur_type == 'character'):
            self.__convert_character(internal_ref, attribute, to_type, data_format)

        else:
            self.__convert_numeric(internal_ref, attribute, to_type)


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




    def find_and_replace(self, tablename, attribute, value, replacement, exact=True, new_name=""):
        """Method that finds values and replaces them with the provided argument.

        Parameters:
            value: The value that needs to found so it can be replaced.
            replacement: The value that will replace the values that were found.
            exact = A boolean indicating if we want to match whole-words only or allowing substring matching
        """
        cur_type, internal_ref  = self.get_attribute_type(tablename, attribute)
        if self.replace is False:
            internal_ref = self.copy_table(internal_ref, new_name)

        if exact is True:
            sql_query = "UPDATE {0}.{1} SET {2} = %s WHERE {2} = %s"
        elif exact is False:
            if value.isalnum() is not True: #Only alphanumerical substrings are supported
                raise self.ValueError("Values not containing alphanumerical characters can not be used for substring matching. "
                                      "Please use whole-word matching to find and replace the values.")
            
            if self.get_attribute_type(tablename, attribute) not in ['character', 'character varying']:
                raise self.AttrTypeError("Substring matching is only possible with character strings. "
                                         "Please convert the attribute to a character string type.")
                
            sql_query = "UPDATE {0}.{1} SET {2} = %s WHERE {2} LIKE %s"
            value = '%{}%'.format(value)

        self.db_connection.cursor().execute(sql.SQL(sql_query).format(sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1]),
                                                                      sql.Identifier(attribute)), (replacement, value))

            
                

        # Possible danger of SQL injections, so we pass value and replacement like this
        self.db_connection.cursor().execute(sql.SQL("UPDATE {}.{} SET  {} = %s WHERE {} = %s").format(sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1]),
                                                                                           sql.Identifier(attribute), sql.Identifier(attribute)), (replacement, value))
        self.db_connection.commit()



    def one_hot_encode(self, tablename, attribute, new_name=""):
        """Method that performs one hot encoding given an attribute"""
        internal_ref = self.get_internal_reference(tablename)
        sql_query = "SELECT * FROM \"{}\".\"{}\"".format(*internal_ref)
        df = pd.read_sql(sql_query, self.engine)
        encoded = pd.get_dummies(df[attribute]) #Perfom one-hot-encoding
        df = df.drop(attribute, axis=1) #Drop the attribute used for encoding
        df = df.join(encoded) #Join the original attributes with the encoded table
        if self.replace is True:
            #If the table should be replaced, drop it and recreate it.
            df.to_sql(tablename, self.engine, None, internal_ref[0], 'replace', index = False)
        elif self.replace is False:
            #We need to create a new table and leave the original untouched
            df.to_sql(new_name, self.engine, None, internal_ref[0], 'fail', index = False)

        
    def __calculate_zscore(self, mean, standard_dev, value):
        """Method to quickly calculate z-scores"""
        zscore = (value - mean) / standard_dev
        return zscore
            


    def normalize_using_zscore(self, tablename, attribute, new_name = ""):
        """Method that normalizes the values of an attribute using the z-score.
        This will normalize everything in a 1-point range, thus [0-1].
        """
        #Let's check if the attribute is a numeric type, this should not be performed on non-numeric types
        attr_type = self.get_attribute_type(tablename, attribute)
        if attr_type not in ['integer', 'double precision']:
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

        if self.replace is True:
            #If the table should be replaced, drop it and recreate it.
            df.to_sql(tablename, self.engine, None, internal_ref[0], 'replace', index = False)
        elif self.replace is False:
            #We need to create a new table and leave the original untouched
            df.to_sql(new_name, self.engine, None, internal_ref[0], 'fail', index = False)




    def discretizise_using_equal_width(self, tablename, attribute, new_name=""):
        """Method that calulates the bins for an equi-distant discretizisation and performs it"""
        internal_ref = self.get_internal_reference(tablename)
        sql_query = "SELECT * FROM \"{}\".\"{}\"".format(*internal_ref)
        df = pd.read_sql(sql_query, self.engine)

        minimum = df[attribute].min()
        maximum = df[attribute].max() + 0.0001 #Add a little bit to make sure the max element is included
        leftmost_edge = math.floor(minimum)
        rightmost_edge = math.ceil(maximum)
        value_range = rightmost_edge - leftmost_edge
        nr_values = df[attribute].size
        #A good rule of thumb for the amount of bins is the square root of the amount of elements
        nr_bins = math.ceil(math.sqrt(nr_values))
        #Generally speaking we want to keep the amount of bins between 2 and 20 for readibility
        if nr_bins < 2:
            nr_bins = 2
        elif nr_bins > 20:
            nr_bins = 20
        #Calculate the width of the bins
        bin_width = math.ceil(value_range / nr_bins)
        bins = [leftmost_edge]
        #Get all the intervals in list form
        for i in range(nr_bins):
            next_interval = bins[i] + bin_width
            bins.append(next_interval)

        #Make labels for these intervals using the list of bin values
        binlabels = []
        for i in range(1, len(bins)):
            label = "[" + str(bins[i-1]) + " - " + str(bins[i]) + "["                                
            binlabels.append(label)
            
        column_name = attribute + "_category"
        df['category'] = pd.cut(df[attribute], bins, right=False, labels = binlabels, include_lowest=True)

        if self.replace is True:
            #If the table should be replaced, drop it and recreate it.
            df.to_sql(tablename, self.engine, None, internal_ref[0], 'replace', index = False)
        elif self.replace is False:
            #We need to create a new table and leave the original untouched
            df.to_sql(new_name, self.engine, None, internal_ref[0], 'fail', index = False)


    def discretizise_using_equal_frequency(self, tablename, attribute, new_name=""):
        """Method that calulates the bins for an equi-frequent discretizisation and performs it"""
        #The initial steps are similar to equi-distant discretizisation
        internal_ref = self.get_internal_reference(tablename)
        sql_query = "SELECT * FROM \"{}\".\"{}\"".format(*internal_ref)
        df = pd.read_sql(sql_query, self.engine)
        nr_values = df[attribute].size
        nr_bins = math.floor(math.sqrt(nr_values))
        if nr_bins < 2:
            nr_bins = 2
        elif nr_bins > 20:
            nr_bins = 20
        
        elements = df[attribute].tolist() #Load all the elements in a python list
        elements.sort()
        index_bins = [0]
        temp_width = math.floor(len(elements) / nr_bins) #Rough estimate of the indices
        remainder = len(elements) % nr_bins
        for i in range(nr_bins):
            index_value = index_bins[i-1] + temp_width
            if remainder > 0:
                index_value += 1
                remainder -= 1
            index_bins[i] = index_value
            index_bins.append(0) #Add dummy element that will be modified in the next loop

        
        del index_bins[-1] #Delete last dummy element

        #Calculate actual values for the bins
        bins = [elements[0]]
        for i in index_bins:
            bins.append((elements[i-1] + 1)) #The elements need to be included too so we add 1 to compensate


        binlabels = []
        for i in range(1, len(bins)):
            label = "[" + str(bins[i-1]) + " - " + str(bins[i]) + "["                                
            binlabels.append(label)

        column_name = attribute + "_category"
        df[column_name] = pd.cut(df[attribute], bins, right=False, labels = binlabels, include_lowest=True)

        if self.replace is True:
            #If the table should be replaced, drop it and recreate it.
            df.to_sql(tablename, self.engine, None, internal_ref[0], 'replace', index = False)
        elif self.replace is False:
            #We need to create a new table and leave the original untouched
            df.to_sql(new_name, self.engine, None, internal_ref[0], 'fail', index = False)



        
        

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
