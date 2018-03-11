import pandas as pd
from db_wrapper import DBConnection
from psycopg2 import sql
from sqlalchemy import create_engine


class DataTransformer:

    def __init__(self, userid, replace = True):
        self.userid = userid
        self.replace = replace

    """Extra option to check whether deleting the attribute will destroy integrity constraints.
    Checks for other constraints is possible as well"""
    def __integrity_check(self, tablename, setid):
        #implemented in secret file
        pass


    #Get the internal reference for the table of (setid) and (tablename). This returns a pair (id, name)
    def get_internal_reference(self, setid, tablename):
            return (str(setid), tablename)

    #In case the transformation has to result in a new table, we copy the existing one
    def copy_table(self, internal_ref, new_name):
        with DBConnection() as db_conn:
            #Execute the query this way to avoid SQL injections
            db_conn.cursor().execute(sql.SQL("CREATE TABLE {}.{} AS SELECT * FROM {}.{}").format(sql.Identifier(internal_ref[0]),sql.Identifier(new_name),
                                                                                              sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1])))
            db_conn.commit()
            return (internal_ref[0], new_name)

        
    #Delete an attribute of a table
    def delete_attribute(self, setid, tablename, attribute, new_name=""):
        with DBConnection() as db_conn:
            internal_ref = self.get_internal_reference(setid, tablename)
            if self.replace is False:
                internal_ref = self.copy_table(internal_ref, new_name)
            #Since this is a query generated out of own data, we can use string interpolation without SQL injection problems.
            #Delete the specified attribute
            db_conn.cursor().execute(sql.SQL("ALTER TABLE {}.{} DROP COLUMN {}").format(sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1]),
                                                                                        sql.Identifier(attribute)))
            db_conn.commit()


    #Returns a list of supported types to convert to given a data_type
    def get_conversion_options(self,  setid, tablename, attribute):
        data_type = self.get_attribute_type(setid, tablename, attribute)
        options = { 'character varying' : ['CHAR(255)', 'INTEGER', 'FLOAT', 'DATE', 'TIME', 'TIMESTAMP'],
                    'character'         : ['VARCHAR(255)', 'INTEGER', 'FLOAT', 'DATE', 'TIME', 'TIMESTAMP'],
                    'integer'           : ['CHAR(255)', 'VARCHAR(255)', 'FLOAT'],
                    'double precision'  : ['CHAR(255)', 'VARCHAR(255)', 'INTEGER'] ,
                    'date'              : ['CHAR(255)', 'VARCHAR(255)'],
                    'time without time zone' : ['CHAR(255)', 'VARCHAR(255)'],
                    'timestamp without time zone' : ['CHAR(255)', 'VARCHAR(255)']
                    }

        return options.setdefault(data_type, [])

    #Return the postgres data type of an attribute
    def get_attribute_type(self, setid, tablename, attribute):
        with DBConnection() as db_conn:
            internal_ref = self.get_internal_reference(setid, tablename)
            db_conn.cursor().execute(sql.SQL("SELECT pg_typeof({}) FROM {}.{}").format(sql.Identifier(attribute), sql.Identifier(internal_ref[0]),
                                                                                       sql.Identifier(internal_ref[1])))
            return (db_conn.cursor().fetchone()[0], internal_ref)


    #Conversion of a "numeric" things (INTEGER and FLOAT, DATE, TIME, TIMESTAMP)
    def __convert_numeric(self, internal_ref, attribute, to_type):
        with DBConnection() as db_conn:
            sql_query = "ALTER TABLE {}.{} ALTER COLUMN  {} TYPE %s" % to_type
            db_conn.cursor().execute(sql.SQL(sql_query).format(sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1]),
                                                                                                  sql.Identifier(attribute)), [to_type])
            db_conn.commit()
        

    #Conversion of a character type (VARCHAR and CHAR)
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
        with DBConnection() as db_conn:
            db_conn.cursor().execute(sql_query)
            db_conn.commit()



                

    #Change the attribute type, if the data follows a specific format like TIMESTAMP provide it as well.
    def change_attribute_type(self, setid, tablename, attribute, to_type, data_format="", new_name=""):
        cur_type, internal_ref  = self.get_attribute_type(setid, tablename, attribute)
        if self.replace is False:
            internal_ref = self.copy_table(internal_ref, new_name)
        
        if (cur_type == 'character varying') or (cur_type == 'character'):
            self.__convert_character(internal_ref, attribute, to_type, data_format)

        else:
            self.__convert_numeric(internal_ref, attribute, to_type)


    #In case that change_attribute_type fails due to elements that can't be converted
    #this method will force the conversion by deleting the rows containing problematic elemants
    def force_attribute_type(self, setid, tablename, attribute, to_type, data_format="", new_name=""):
        if self.replace is True:
            internal_ref = self.get_internal_reference(setid, tablename)
        else:
            internal_ref = self.get_internal_reference(setid, new_name)
        
        pattern = ""
        if to_type == 'INTEGER':
            pattern = '^[-+]?[0-9]+$'
            
        elif to_type == 'FLOAT':
            pattern = '^[-+.]?[0-9]+[.]?[e]?[-+]?[0-9]*$'

        else:
            #This is going to be very tough to express....
            return None

        with DBConnection() as db_conn:
            db_conn.cursor().execute(sql.SQL("DELETE FROM {}.{} WHERE ({} !~ %s )").format(sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1]),
                                                                                         sql.Identifier(attribute)), [pattern])
            db_conn.commit();

        if self.replace is True:
            self.change_attribute_type(setid, tablename, attribute, to_type, data_format, new_name)
        else:
            #The first call of change_attribute_type already created a new table which is a copy of (tablename)
            #But we don't want to copy this table once again, only overwrite it
            self.replace = True
            self.change_attribute_type(setid, new_name, attribute, to_type, data_format, new_name)





    def find_and_replace(self, setid, tablename, attribute, value, replacement, new_name=""):
        cur_type, internal_ref  = self.get_attribute_type(setid, tablename, attribute)
        if self.replace is False:
            internal_ref = self.copy_table(internal_ref, new_name)

        with DBConnection() as db_conn:
            #Possible danger of SQL injections, so we pass value and replacement like this
            db_conn.cursor().execute(sql.SQL("UPDATE {}.{} SET  {} = %s WHERE {} = %s").format(sql.Identifier(internal_ref[0]), sql.Identifier(internal_ref[1]),
                                                                                               sql.Identifier(attribute), sql.Identifier(attribute)), (replacement, value))
            db_conn.commit()

        
        

if __name__ == '__main__':
    dt = DataTransformer(1, True)
    #print(dt.get_internal_reference("etherdelta", "employees"))dt.delete_attribute("etherdelta", "clients", "stupid")
    dt.force_attribute_type(1, 'clients5', 'clientnumber', 'INTEGER', '', '')
    #dt.change_attribute_type(1, 'clients2', 'clientnumber', 'VARCHAR(255)', '', 'clients5')
    #dt.find_and_replace(1, 'clients5', 'clientnumber', '332', 'probleem')
    print("SUCCES!")