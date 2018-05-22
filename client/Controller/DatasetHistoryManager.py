import re
import math

import psycopg2
import psycopg2.extras
from psycopg2 import sql
import pandas as pd

class DatasetHistoryManager:
    """Class that manages the transformation history ofimport reimport re a dataset.

    Attributes:
        setid: The id of the dataset that the manager has to access.
        db_connection: psycopg2 database connection to execute SQL queries.
        engine: SQLalchemy engine to use pandas functionality.
        track: Boolean indicating if the history has to be tracked and written to the history table.
    """

    def __init__(self, setid, db_connection, track=True):
        self.setid = setid
        self.db_connection = db_connection
        self.track = track
        self.entry_count = self.__initialize_entrycount()
        self.choice_dict = None
        
    def __initialize_entrycount(self):
            cur = self.db_connection.cursor()
            query = "SELECT COUNT(*) FROM system.dataset_history WHERE setid = %s"
            cur.execute(sql.SQL(query), [self.setid])
            return int(cur.fetchone()[0])
        
    def get_rowcount(self, tablename=None):
        """Quick method to get the number of rows in the dataset history table."""
        if tablename is None: #If we're viewing history of all the tables.
            return self.entry_count

        else:
            cur = self.db_connection.cursor()
            query = "SELECT COUNT(*) FROM system.dataset_history WHERE setid = %s AND (table_name = %s OR origin_table = %s)"
            cur.execute(sql.SQL(query), [self.setid, tablename, tablename])
            return int(cur.fetchone()[0])
        
    def write_to_history(self, table_name, origin_table, attribute, parameters, transformation_type):
        """Method thar writes an entry to the dataset history table for a performed transformation.

        Parameters:
            table_name: Name of the table containing the results of the transformation.
            origin_table: Name of the table that was used for the transformation.
            attribute: Name of attribute that was use for the transformation.
            parameters: List of parameters used with the transformation
            transformation_type: Integer representing the transformation used.
        """
        if self.track is False:
            return None
        
        param_array = self.__python_list_to_postgres_array(parameters, transformation_type)
        cur = self.db_connection.cursor()
        query = 'INSERT INTO SYSTEM.DATASET_HISTORY VALUES (%s, %s, %s, %s, %s, %s)'
        cur.execute(sql.SQL(query), [self.setid, table_name, attribute, transformation_type, param_array, origin_table])
        self.db_connection.commit()

    def render_history_json(self, offset, limit, reverse_order=False, show_all=True, table_name=""):
        """Method that returns a json string containing the asked data of the dataset_history table.

        Parameters:
            offset: Offset to determine from which row number we start showing entries.
            limit: The number of rows we will render in the json.
            reverse_order: False implies earlier history entries first and True implies oldest entries first.
            show_all: True if all entries for the dataset should be shown, False implies history for a specific table
            transformation_type: If show_all is False, all the history entries of table_name will returned in json.
        """
        dict_cur = self.db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        ordering = 'DESC'
        if reverse_order:
            ordering = 'ASC'
        
        if show_all is False:
            query = ("SELECT * FROM system.dataset_history WHERE setid = %s AND (table_name = %s OR origin_table = %s)"
                     " ORDER BY transformation_date {} LIMIT %s OFFSET %s").format(ordering)
            dict_cur.execute(sql.SQL(query), [self.setid, table_name, table_name, limit, offset])
        else:
            query = ("SELECT * FROM system.dataset_history WHERE setid = %s ORDER BY transformation_date "
                     "{} LIMIT %s OFFSET %s").format(ordering)
            dict_cur.execute(sql.SQL(query), [self.setid, limit, offset])

        all_rows = dict_cur.fetchall()
        df = self.__rows_to_dataframe(all_rows)
        json_string = df.to_json(orient='values', date_format='iso')
        return json_string
        
    def __python_list_to_postgres_array(self, py_list, transformation_type):
        """Method that represents a python list as a postgres array for inserting into a PostreSQL database."""
        param_array = ""
        nr_elements = len(py_list)
        
        if nr_elements == 0: #Return an empty postgres array string
            return "{}"

        if transformation_type > 14: #Arguments for transformation 15 and 16 are already quoted
            param_array = "{" + py_list[0] + "}"
            return param_array
            
        param_array = "{}"
        if nr_elements > 1:
            for i in range(nr_elements-1):
                param_array += ", {}"

        param_array = param_array.format(*py_list)
        param_array = "{" + param_array  + "}"

        return param_array

    def __backup_table(self, t_id):
        cur = self.db_connection.cursor()
        cur.execute("SELECT table_name FROM system.dataset_history WHERE transformation_id = %s", (t_id,))
        tablename = cur.fetchone()[0]

        backup = 'backup."{}"'.format(str(t_id))
        backup_query = 'CREATE TABLE {} AS SELECT * FROM "{}"."{}"'.format(backup, str(self.setid), tablename)
        cur.execute(backup_query)

    def __get_recent_transformations(self, tablename):
        """Method that returns the recent operations performed on a table."""
        dict_cur = self.db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        query = ("SELECT * FROM system.dataset_history WHERE setid = %s AND table_name = %s LIMIT 10")
        dict_cur.execute(query, (self.setid, tablename))
        return dict_cur.fetchall()

    def __get_latest_backup(self, tablename):
        recent_tx = self.__get_recent_transformations(tablename)
        distance = 0
        for d in recent_tx:
            distance += self.__get_edit_distance(d['transformation_type'])
            if distance >= 3.0:
                return d['transformation_id']

        # If all the transformations haven't reached a distance of 3.0, that means the only backup
        # is the original table.
        return None

    def __get_transformation_list(self, tablename, start_id):
        """Return a list of transformations and their arguments that need to be performed to go back
        N-1th transformation and basically emulating an undo of the Nth transformation.
        """
        cur = self.db_connection.cursor()
        query = ('SELECT transformation_id, parameters  FROM system.dataset_history WHERE setid = %s AND table_name = %s'
                 'AND origin_table = %s AND transformation_id > %s')
        cur.execute(query, (self.setid, tablename, tablename, start_id ))
        all_tx = cur.fetchall()
        return all_tx
        
    def __get_edit_distance(self, t_id):
        """Method that calculates the edit distance defined by us to determine how dissimilar two tables are.
        This is done by looking at various transformations and rating how hard a transformation changed the
        data and how expensive that transformation was.
        """
        #These are the expensive operations, so performing these will warrant creating a backup sooner.
        if t_id in [4, 5, 6, 13, 14, 16]:
            return 1.0
        #These are light transformations and only should make a backup after several operations.
        else:
            return 0.3
        
    def __generate_choice_dict(self):
        """Generate the dictionary used to write away history table entries."""
        if self.choice_dict is not None:
            return
        
        choice_dict = {
            0  : self.__rowstring_generator0,
            1  : self.__rowstring_generator1,
            2  : self.__rowstring_generator2,
            3  : self.__rowstring_generator3,
            4  : self.__rowstring_generator4,
            5  : self.__rowstring_generator5,
            6  : self.__rowstring_generator6,
            7  : self.__rowstring_generator7,
            8  : self.__rowstring_generator8,
            9  : self.__rowstring_generator9,
            10 : self.__rowstring_generator10,
            11 : self.__rowstring_generator11,
            12 : self.__rowstring_generator12,
            13 : self.__rowstring_generator13,
            14 : self.__rowstring_generator14,
            15 : self.__rowstring_generator15,
            16 : self.__rowstring_generator16
            }
        
        self.choice_dict =  choice_dict

    def __rows_to_dataframe(self, row_list):
        """Method that translates row results of a query to a pandas dataframe."""
        list_a = []
        list_b = []
        self.__generate_choice_dict()

        for elem in row_list:
            tr_type = int(elem['transformation_type'])
            field1 = self.choice_dict[tr_type](elem)
            if self.__is_new_table(elem):
                field1 += self.__get_new_table_string(elem)
            field2 = elem['transformation_date']
            list_a.append(field1)
            list_b.append(field2)

        val_dict = { 'Transformation description' : list_a,
                     'Operation date'             : list_b}
        
        pd.set_option('display.max_colwidth', -1)
        df = pd.DataFrame(data=val_dict)
        return df

    def __unquote_string(self, string):
        """Assuming a string is quoted, this method will return the same string without the quotes"""
        return string[1:-1]
    
    def __is_new_table(self, dict_obj):
        """Method that checks whether a table is a new table created from a transformation."""
        if dict_obj['table_name'] != dict_obj['origin_table'] :
            return False
        else:
            return False

    def __get_new_table_string(self, dict_obj):
        """Get a string that explains what new table the transformation resulted in."""
        string = 'This transformation resulted in a new table "{}".'.format(dict_obj['table_name'])
        return string

    def __rowstring_generator0(self, dict_obj):
        rowstring = 'Renamed ...'
        return rowstring


    def __rowstring_generator1(self, dict_obj):
        param = dict_obj['parameters']
        rowstring = 'Converted attribute "{}" of table "{}" to type {}.'
        rowstring = rowstring.format(dict_obj['attribute'], dict_obj['origin_table'], param[0])
        return rowstring

    def __rowstring_generator2(self, dict_obj):
        rowstring = 'Deleted attribute "{}" of table "{}"'.format(dict_obj['attribute'], dict_obj['origin_table'])
        return rowstring

    def __rowstring_generator3(self, dict_obj):
        param = dict_obj['parameters']
        if param[0] == 'True':
            operand = 'larger'
        else:
            operand = 'smaller'

        value = self.__unquote_string(param[1])
        replacement = self.__unquote_string(param[2])

        rowstring = 'Deleted outliers {} than {} from attribute "{}" of table "{}" by replacing them with the value {}.'
        rowstring = rowstring.format(operand, value, dict_obj['attribute'], dict_obj['origin_table'], replacement)
        return rowstring

    def __rowstring_generator4(self, dict_obj):
        param = dict_obj['parameters']
        rowstring = 'Discretized attribute "{}" of table "{}" using custom ranges ( {} ).'
        rowstring = rowstring.format(dict_obj['attribute'], dict_obj['attribute'], self.__unquote_string(param[0]))
        return rowstring

    def __rowstring_generator5(self, dict_obj):
        rowstring = 'Discretized attribute "{}" of table "{}" in equi-frequent intervals.'
        rowstring = rowstring.format(dict_obj['attribute'], dict_obj['attribute'])
        return rowstring

    def __rowstring_generator6(self, dict_obj):
        rowstring = 'Discretized attribute "{}" of table "{}" in equi-distant intervals.'
        rowstring = rowstring.format(dict_obj['attribute'], dict_obj['attribute'])
        return rowstring

    def __rowstring_generator7(self, dict_obj):
        param = dict_obj['parameters']
        rowstring = 'Extracted {} from attribute "{}" of table "{}".'
        rowstring = rowstring.format(param[0], dict_obj['attribute'], dict_obj['origin_table'])
        return rowstring   

    def __rowstring_generator8(self, dict_obj):
        rowstring = 'Performed find-and-replace on attribute "{}" of table "{}" looking for \'{}\', {} substring matches and replacing the {} with \'{}\'.'
        param = dict_obj['parameters']
        replacement_style = 'whole string'
        if param[2] == 'True':
            option = 'not allowing'
        else:
            option = 'allowing'
            if param[3] == 'False':
                replacement_style = 'substring'

        rowstring = rowstring.format(dict_obj['attribute'], dict_obj['origin_table'], param[0], option, replacement_style, param[1])
        return rowstring

    def __rowstring_generator9(self, dict_obj):
        param = dict_obj['parameters']
        if param[2] == 'True':
            sens_option = 'case sensitive'
        else:
            sens_option = 'case insensitive'
        rowstring = 'Performed find-and-replace on attribute "{}" of table "{}" with the regular expression \'{}\' ({}) replacing the matches with \'{}\'.'
        rowstring = rowstring.format(dict_obj['attribute'], dict_obj['origin_table'], param[0], sens_option, param[1])
        return rowstring

    def __rowstring_generator10(self, dict_obj):
        param = dict_obj['parameters']
        rowstring = 'Filled null values with provided value {} in attribute "{}" of table "{}".'
        rowstring = rowstring.format(self.__unquote_string(param[0]), dict_obj['attribute'], dict_obj['origin_table'])
        return rowstring

    def __rowstring_generator11(self, dict_obj):
        param = dict_obj['parameters']
        rowstring = 'Filled null values with the mean ({}) in attribute "{}" of table "{}".'
        rowstring = rowstring.format(self.__unquote_string(param[0]), dict_obj['attribute'], dict_obj['origin_table'])
        return rowstring

    def __rowstring_generator12(self, dict_obj):
        param = dict_obj['parameters']
        rowstring = 'Filled null values with a custom value ({}) in attribute "{}" of table "{}".'
        rowstring = rowstring.format(self.__unquote_string(param[0]), dict_obj['attribute'], dict_obj['origin_table'])
        return rowstring

    def __rowstring_generator13(self, dict_obj):
        rowstring = 'Normalized attribute "{}" of table "{}" in range [0-1] using the Z-score.'
        rowstring = rowstring.format(dict_obj['attribute'], dict_obj['origin_table'])
        return rowstring

    def __rowstring_generator14(self, dict_obj):
        rowstring = 'Performed One-hot-encoding using attribute "{}" of table "{}".'
        rowstring = rowstring.format(dict_obj['attribute'], dict_obj['origin_table'])
        return rowstring

    def __rowstring_generator15(self, dict_obj):
        param = dict_obj['parameters']
        rowstring = 'Deleted rows from table "{}" using the following predicate: {}.'
        rowstring = rowstring.format(dict_obj['origin_table'], param[0])
        return rowstring

    def __rowstring_generator16(self, dict_obj):
        param = dict_obj['parameters']
        rowstring = 'Executed user-generated query on table "{}". Used query: {}'
        rowstring = rowstring.format(dict_obj['origin_table'], param[0])
        return rowstring
