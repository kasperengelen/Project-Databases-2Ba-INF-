import re
import math
import json

import psycopg2
import psycopg2.extras
from psycopg2 import sql
import pandas as pd

class DatasetHistoryManager:
    """Class that manages the transformation history of a dataset.

    Attributes:
        setid: The id of the dataset that the manager has to access.
        db_connection: psycopg2 database connection to execute SQL queries.
        engine: SQLalchemy engine to use pandas functionality.
        track: Boolean indicating whether the transformation has to be written in history.
    """

    def __init__(self, setid, db_connection, track = True):
        self.setid = setid
        self.db_connection = db_connection
        self.entry_count = self.__initialize_entrycount()
        self.track = track
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
            return
        param_array = self.__python_list_to_postgres_array(parameters, transformation_type)
        cur = self.db_connection.cursor()
        query = 'INSERT INTO SYSTEM.DATASET_HISTORY VALUES (%s, %s, %s, %s, %s, %s) RETURNING transformation_id'
        cur.execute(sql.SQL(query), [self.setid, table_name, attribute, transformation_type, param_array, origin_table])
        t_id = cur.fetchone()[0]
        self.db_connection.commit()
        backups = self.get_latest_backups(table_name)
        backup_id = 0
        if len(backups) > 0:
            backups = [i[0] for i in backups]
            backup_id = max(backups)
        if self.auto_backup_check(backup_id, table_name) is True:
            self.__backup_table(table_name, t_id)
            if len(backups) == 2: #We need to drop the oldest backup after creating a new one
                backup_name = self.__get_backup_name_from_id(min(backups))
                self.__delete_backup(backup_name)
                self.__delete_history_entry(min(backups))
        elif transformation_type == 0:
            self.__backup_table(table_name, t_id)
            
                

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
            query = ("SELECT * FROM system.dataset_history WHERE setid = %s AND table_name = %s"
                     " ORDER BY transformation_date {} LIMIT %s OFFSET %s").format(ordering)
            dict_cur.execute(sql.SQL(query), [self.setid, table_name, limit, offset])
        else:
            query = ("SELECT * FROM system.dataset_history WHERE setid = %s AND transformation_type >= 0"
                     " ORDER BY transformation_date {} LIMIT %s OFFSET %s").format(ordering)
            dict_cur.execute(sql.SQL(query), [self.setid, limit, offset])

        all_rows = dict_cur.fetchall()
        data = self.__rows_to_list(all_rows)
        json_string = json.dumps(data, default=str)
        return json_string

    def is_undo_enabled(self, tablename):
        """Method that returns whether it's possible to undo the most recent transformation
        of a table in the dataset.
        """
        last_tid = self.get_edge_transformation(tablename, True)
        if last_tid is None: #There is no recent transformation for this table.
            return False
        oldest_tid = self.get_edge_transformation(tablename, False)
        backups = self.get_latest_backups(tablename)
        if len(backups) > 0: #There are backup(s) available
            if len(backups) == 2:
                return True
            if len(backups) == 1:
                # There seems to be only 1 backup
                if backups[0][0] < last_tid:
                    return True
                
                if self.original_exists(tablename):
                    if self.is_in_recover_range(tablename, oldest_tid, backups[0][0]) is True:
                        return True
                    else:
                        return False

                else:
                    return False
        #There are no backups
        if self.original_exists(tablename):
            return True
            
    def get_latest_backups(self, tablename):
        """Returns the the id of the latest backups, could be one or could be two results depending
        on how many transformations were performed."""
        cur = self.db_connection.cursor()
        values = [self.setid, tablename]
        cur.execute(sql.SQL('SELECT transformation_id FROM system.dataset_history WHERE setid = %s'
                            ' AND origin_table = %s AND transformation_type = -1'), values)
        return cur.fetchall()

    def get_edge_transformation(self, tablename, edge=True):
        """Returns the most recent or oldest transformation_id of a table in the dataset.
        Depending on the boolean edge, True is the oldest, False is the youngest transformation.
        """
        cur = self.db_connection.cursor()
        values = [self.setid, tablename]
        agg = 'MAX'
        if edge is False:
            agg = 'MIN'
            
        query = ('SELECT {}(transformation_id) FROM system.dataset_history WHERE '
                'setid = %s AND table_name = %s AND transformation_type > -1').format(agg)
        cur.execute(query, values)
        result = cur.fetchone()
        if result is not None:
            return result[0]
        else:
            return None

    def get_all_transformations_in_interval(self, tablename, id1, id2):
        """Returns a list of the transformation types that happened
        after transformation id1 to (and including) transformation id2.
        """
        cur = self.db_connection.cursor()
        query = ('SELECT transformation_type FROM system.dataset_history WHERE '
                 'setid = %s AND table_name = %s AND transformation_type > -1'
                 ' AND transformation_id >= %s AND transformation_id < %s '
                 'ORDER BY transformation_id')
        cur.execute(query, [self.setid, tablename, id1, id2])
        return cur.fetchall()

    def get_transformations_after_id(self, tablename, id1):
        """Method that returns all transformation types performed after a certain id."""
        cur = self.db_connection.cursor()
        values = [self.setid, tablename, id1]
        cur.execute(('SELECT transformation_type FROM system.dataset_history WHERE '
                     'setid = %s AND table_name = %s AND transformation_type > -1'
                     ' AND transformation_id > %s'), values)
        return cur.fetchall()

    def get_edit_distance(self, tx_list):
        """Method that calculates the edit distance."""
        distance = 0
        for tx in tx_list:
            distance += self.__transformation_distance(tx[0])

        return distance
        
    def is_in_recover_range(self, tablename, id1, id2):
        """Method that returns whether a table is edited too much from id1 to still be
        recovered using the original table. id1 is the first transformation to alter the original
        and id2 is the the transformation that caused the backup.
        """
        tx_list = self.get_all_transformations_in_interval(tablename, id1, id2)
        distance = self.get_edit_distance(tx_list)
        #The distance between the originals and their first backup is max 39
        if distance > 39:
            return False
        else:
            return True

    def original_exists(self, tablename):
        """Method that returns whether the table is a table originally added to the dataset which means that the table
        has a backup of that original data.
        """
        cur = self.db_connection.cursor()
        original_schema = 'original_' + str(self.setid)
        values = [original_schema, tablename]
        query  = 'SELECT table_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s' 
        cur.execute(query, values)
        result = cur.fetchone()
        if result is None:
            return False
        else:
            return True

    def auto_backup_check(self, start_id, tablename):
        """Check whether it's time to make a backup of the transformation."""
        tx_list = self.get_transformations_after_id(tablename, start_id)
        distance = self.get_edit_distance(tx_list)
        if distance >= 30:
            return True
        else:
            return False

    def __delete_history_entry(self, t_id):
        """Method that deletes a history entry given the t_id."""
        cur = self.db_connection.cursor()
        cur.execute(('DELETE FROM system.dataset_history WHERE transformation_id = %s'), [t_id])
        self.db_connection.commit()
        
    def __python_list_to_postgres_array(self, py_list, transformation_type):
        """Method that represents a python list as a postgres array for inserting into a PostreSQL database."""
        param_array = ""
        nr_elements = len(py_list)
        
        if nr_elements == 0: #Return an empty postgres array string
            return "{}"
        """
        if 15 < transformation_type < 17: #Arguments for transformation 15 and 16 are already quoted
            param_array = "{" + py_list[0] + "}"
            return param_array"""
            
        param_array = "{}"
        if nr_elements > 1:
            for i in range(nr_elements-1):
                param_array += ", {}"

        param_array = param_array.format(*py_list)
        param_array = "{" + param_array  + "}"

        return param_array

    def __backup_table(self, tablename, t_id):
        cur = self.db_connection.cursor()
        backup = 'backup."{}"'.format(str(t_id))
        backup_query = 'CREATE TABLE {} AS SELECT * FROM "{}"."{}"'.format(backup, str(self.setid), tablename)
        query = 'INSERT INTO SYSTEM.DATASET_HISTORY VALUES (%s, %s, %s, %s, %s, %s)'
        cur.execute(backup_query)
        cur.execute(query, [self.setid, str(t_id), '', -1, '{}', tablename])
        self.db_connection.commit()

    def __delete_backup(self, backup_name):
        cur = self.db_connection.cursor()
        backup = 'backup."{}"'.format(str(backup_name))
        cur.execute('DROP TABLE {}'.format(backup))
        self.db_connection.commit()

    def __get_backup_name_from_id(self, t_id):
        cur = self.db_connection.cursor()
        query = 'SELECT table_name FROM system.dataset_history WHERE transformation_id = %s'
        cur.execute(query, [t_id])
        self.db_connection.commit()
        return cur.fetchone()[0]
        
    def __get_recent_transformations(self, tablename):
        """Method that returns the recent operations performed on a table."""
        dict_cur = self.db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        query = ("SELECT * FROM system.dataset_history WHERE setid = %s AND table_name = %s LIMIT 10")
        dict_cur.execute(query, (self.setid, tablename))
        return dict_cur.fetchall()

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
        
    def __transformation_distance(self, t_id):
        """Method that calculates the edit distance defined by us to determine how dissimilar two tables are.
        This is done by looking at various transformations and rating how hard a transformation changed the
        data and how expensive that transformation was.
        """
        #These are the expensive operations, so performing these will warrant creating a backup sooner.
        if t_id in [4, 5, 6, 13, 14, 16]:
            return 10
        #These are light transformations and only should make a backup after several operations.
        else:
            return 3
        
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
            16 : self.__rowstring_generator16,
            17 : self.__rowstring_generator17,
            18 : self.__rowstring_generator18,
            19 : self.__rowstring_generator19
            }
        
        self.choice_dict =  choice_dict

    def __rows_to_list(self, row_list):
        """Method that translates row results of a query to a list of lists."""
        self.__generate_choice_dict()
        result = []
        
        for elem in row_list:
            tr_type = int(elem['transformation_type'])
            field1 = self.choice_dict[tr_type](elem)
            field2 = elem['transformation_date']
            result.append((field2, field1))

        return result

    def __unquote_string(self, string):
        """Assuming a string is quoted, this method will return the same string without the quotes"""
        return string[1:-1]
    
    def __rowstring_generator0(self, dict_obj):
        rowstring = 'Created table "{}" which is a copy of table "{}".'.format(dict_obj['table_name'],
                                                                               dict_obj['origin_table'])
        return rowstring
    
    def __rowstring_generator1(self, dict_obj):
        param = dict_obj['parameters']
        rowstring = 'Converted attribute "{}" of table "{}" to type {}.'
        rowstring = rowstring.format(dict_obj['attribute'], dict_obj['origin_table'], param[3])
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
        rowstring = rowstring.format(dict_obj['attribute'], dict_obj['table_name'], self.__unquote_string(param[0]))
        return rowstring

    def __rowstring_generator5(self, dict_obj):
        rowstring = 'Discretized attribute "{}" of table "{}" in equi-frequent intervals.'
        rowstring = rowstring.format(dict_obj['attribute'], dict_obj['table_name'])
        return rowstring

    def __rowstring_generator6(self, dict_obj):
        param = dict_obj['parameters']
        rowstring = 'Discretized attribute "{}" of table "{}" in {} equi-distant intervals.'
        rowstring = rowstring.format(dict_obj['attribute'], dict_obj['table_name'], param[0])
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
        param = dict_obj['parameters']
        if param[0] == 'False':
            rowstring += ' The normalized values have been written to a new column "{}".'.format(param[1])
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

    def __rowstring_generator17(self, dict_obj):
        param = dict_obj['parameters']
        values = [dict_obj['attribute'], dict_obj['table_name'], param[0]]
        rowstring = 'Renamed column "{}" of table "{}" to "{}".'.format(*values)
        return rowstring

    def __rowstring_generator18(self, dict_obj):
        table = dict_obj['table_name']
        rowstring = 'Deleted duplicate rows of table "{}" by performing Data Deduplication.'.format(table)
        return rowstring

    def __rowstring_generator19(self, dict_obj):
        param = dict_obj['parameters']
        rowstring = 'Forcibly converted attribute "{}" of table "{}" to type {} by.'
        rowstring = rowstring.format(dict_obj['attribute'], dict_obj['origin_table'], param[0])
        return rowstring
        
