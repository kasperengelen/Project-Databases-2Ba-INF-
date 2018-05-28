import ast
import json

import psycopg2

from Controller.TableTransformer import TableTransformer
from Controller.QueryExecutor import QueryExecutor
from Controller.Deduplicator import Deduplicator



class TransformationReverser:
    """Class that reverses the most recent transformation on a table to go back to a previous state.
    This operation simulates an undo of the most recent state change of the table.

    Attributes:
        setid: The id of the dataset that the user wants to modify
        table_name: The name of the table we're reverting to a previous state
        db_connection: psycopg2 database connection to execute SQL queries
        engine: SQLalchemy engine to use pandas functionality
        is_original: Boolean indicating whether we're viewing the original data uploaded by the users.
    """

    def __init__(self, setid, table_name, db_connection, engine):
        self.setid = setid
        self.schema = str(setid)
        self.table_name = table_name
        self.db_connection = db_connection
        self.engine = engine
        self.Tt = TableTransformer(self.setid, self.db_connection, self.engine, True, False)
        self.Qe = QueryExecutor(self.setid, self.db_connection, self.engine, True)
        self.Dd = Deduplicator(self.db_connection, self.engine)
        self.transformation_dict = self.initialize_transformation_dict()


    def undo_last_transformation(self):
        """Method that undoes the latest table. This method should only be called if
        DatasetHistoryManager confirmed that the undo is possible.
        """
        backups = self.get_backups()
        if len(backups) == 0:
            #There must be an original table for use.
            original_schema = 'original_' + str(self.setid)
            table_tuple = (original_schema, self.table_name)
            backup_name = self.table_name
            backup_id = 0
    
        else:
            #We need the id and the name of the latest backup
            backup_name = backups[0]['table_name']
            backup_id   = backups[0]['transformation_id']
            table_tuple = ('backup', backup_name)

        tx_list = self.get_transformations_after_backup(backup_id)
        if len(tx_list) == 0:
            #If there are no transformations after the backup
            self.delete_history_entry(backup_id)
            self.drop_backup(backup_name)
            if len(backups) == 1:
                #This was the only backup and undo is still possible
                #This means the closest backup is the original table
                original_schema = 'original_' + str(self.setid)
                table_tuple = (original_schema, self.table_name)
                backup_name = self.table_name
                backup_id = 0
                
            if len(backups) == 2:
                #There must be transformations after the older backup
                #Going past this backup means it will be useless and it
                #needs to be deleted
                backup_name = backups[1]['table_name']
                backup_id   = backups[1]['transformation_id']
                table_tuple = ('backup', backup_name)
                
        tx_list = self.get_transformations_after_backup(backup_id)
        undone_transformation = tx_list.pop()
        self.redo_series_of_transformations(table_tuple, tx_list)
        self.delete_history_entry(undone_transformation['transformation_id'])
        return None
        
    def redo_series_of_transformations(self, table_tuple, tx_list):
        """Redo a list of transformations."""
        self.drop_table(table_tuple[1])
        self.recreate_table_from_backup(table_tuple)
        for elem in tx_list:
            tr_type = elem['transformation_type']
            method = self.transformation_dict[tr_type]
            table_name = elem['table_name']
            attribute  = elem['attribute']
            parameters = elem['parameters']
            method(table_name, attribute, parameters)

        return None
    
    def delete_history_entry(self, t_id):
        """Method that deletes a history entry given the t_id."""
        cur = self.db_connection.cursor()
        cur.execute(('DELETE FROM system.dataset_history WHERE transformation_id = %s'), [t_id])
        self.db_connection.commit()

    def drop_backup(self, backup_name):
        """Method that drops a backup of a table in our system."""
        cur = self.db_connection.cursor()
        cur.execute(('DROP TABLE backup."{}"').format(backup_name))
        self.db_connection.commit()

    def drop_table(self, table):
        """Execute query that drops an SQL table of the dataset."""
        cur = self.db_connection.cursor()
        query_args = [self.schema, self.table_name]
        cur.execute(('DROP TABLE IF EXISTS "{}"."{}"').format(*query_args))
        self.db_connection.commit()

    def recreate_table_from_backup(self, table_tuple):
        """Execute query that recreates a table using another table as base."""
        cur = self.db_connection.cursor()
        query_args = [self.schema, self.table_name, table_tuple[0], table_tuple[1]]
        cur.execute(('CREATE TABLE "{}"."{}" AS SELECT * FROM "{}"."{}"').format(*query_args))
        self.db_connection.commit()
    
    def get_backups(self):
        """Returns the backups (a tuple of the backup name and id) used to perform the undo."""
        dict_cur = self.db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        values = [self.setid, self.table_name]
        query = ('SELECT table_name, transformation_id FROM system.dataset_history WHERE setid = %s  AND'
                 ' origin_table = %s AND transformation_type = -1 ORDER BY transformation_id DESC LIMIT 2')
        dict_cur.execute(query, values)
        return dict_cur.fetchall()

    def get_transformations_after_backup(self, t_id):
        """Returns all the transformations that happened after a t_id."""
        dict_cur = self.db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        query = ('SELECT * FROM system.dataset_history WHERE setid = %s AND table_name = %s  AND '
                 'transformation_type > 0 AND transformation_id > %s ORDER BY transformation_date')
        dict_cur.execute(query, [self.setid, self.table_name, t_id])
        return dict_cur.fetchall()

    def initialize_transformation_dict(self):
        tr_dict = {
            1 : self.redo_attribute_conversion,
            2 : self.redo_attribute_deletion,
            3 : self.redo_outliers_deletion,
            4 : self.redo_custom_discretization,
            5 : self.redo_equifrequent_discretization,
            6 : self.redo_equidistant_discretization,
            7 : self.redo_date_part_extraction,
            8 : self.redo_normal_find_and_replace,
            9 : self.redo_regex_find_and_replace,
            10: self.redo_fill_nulls_mean,
            11: self.redo_fill_nulls_median,
            12: self.redo_fill_null_custom_value,
            13: self.redo_normalization_by_zscore,
            14: self.redo_one_hot_encoding,
            15: self.redo_row_deletion_by_predicate,
            16: self.redo_query_execution,
            17: self.redo_attribute_renaming,
            18: self.redo_data_deduplication,
            19: self.redo_forced_attribute_conversion}

        return tr_dict

    def redo_attribute_conversion(self, table_name, attribute, parameters):
        to_type = parameters[0]
        data_format = parameters[1]
        length = ast.literal_eval(parameters[2])
        self.Tt.change_attribute_type(table_name, attribute, to_type, data_format, length)

    def redo_attribute_deletion(self, table_name, attribute, parameters):
        self.Tt.delete_attribute(table_name, attribute)
        
    def redo_outliers_deletion(self, table_name, attribute, parameters):
        larger     = ast.literal_eval(parameters[0])
        value      = ast.literal_eval(parameters[1])
        replacement= ast.literal_eval(parameters[2])
        self.Tt.delete_outliers(table_name, attribute, larger, value, replacement)

    def redo_custom_discretization(self, table_name, attribute, parameters):
        exclude= parameters.pop(-1)
        exclude= ast.literal_eval(exclude)
        str_ranges = ast.literal_eval(str(parameters))
        ranges = [int(i) for i in str_ranges]
        
            
        self.Tt.discretize_using_custom_ranges(table_name, attribute, ranges, exclude)

    def redo_equidistant_discretization(self, table_name, attribute, parameters):
        nr_bins   = ast.literal_eval(parameters[0])
        self.Tt.discretize_using_equal_width(table_name, attribute, nr_bins)

    def redo_equifrequent_discretization(self, table_name, attribute, parameters):
        self.Tt.discretize_using_equal_frequency(table_name, attribute)

    def redo_date_part_extraction(self, table_name, attribute, parameters):
        extraction_arg   = parameters[0]
        self.Tt.extract_part_of_date(table_name, attribute, extraction_arg)

    def redo_normal_find_and_replace(self, table_name, attribute, parameters):
        exact      = ast.literal_eval(parameters[2])
        replace_all= ast.literal_eval(parameters[3])
        self.Tt.find_and_replace(table_name, attribute,parameters[0], parameters[1], exact, replace_all)
        
    def redo_regex_find_and_replace(self, table_name, attribute, parameters):
        case_sens  = ast.literal_eval(parameters[2])
        self.Tt.regex_find_and_replace(table_name, attribute, parameters[0], parameters[1], case_sens)

    def redo_fill_nulls_mean(self, table_name, attribute, parameters):
        self.Tt.fill_nulls_with_mean(table_name, attribute)

    def redo_fill_nulls_median(self, table_name, attribute, parameters):
        self.Tt.fill_nulls_with_median(table_name, attribute)

    def redo_fill_null_custom_value(self, table_name, attribute, parameters):
        custom_val = ast.literal_eval(parameters[0])
        self.Tt.fill_nulls_with_custom_value(table_name, attribute, custom_val)

    def redo_normalization_by_zscore(self, table_name, attribute, parameters):
        overwrite = ast.literal_eval(parameters[0])
        attribute = parameters[1]
        self.Tt.normalize_using_zscore(table_name, attribute, overwrite)

    def redo_one_hot_encoding(self, table_name, attribute, parameters):
        self.Tt.one_hot_encode(table_name, attribute)

    def redo_row_deletion_by_predicate(self, table_name, attribute, parameters):
        #Just executing the query provides the effect we need.
        cur = self.db_connection.cursor()
        cur.execute('SET search_path to "{}"'.format(self.schema))
        cur.execute(parameters[0])
        cur.execute('SET search_path to public')
        return None

    def redo_query_execution(self, table_name, attribute, parameters):
        query = parameters[0]
        self.Qe.execute_transaction(query)

    def redo_attribute_renaming(self, table_name, attribute, parameters):
        self.Tt.change_attribute_name(table_name, attribute, parameters[0])

    def redo_data_deduplication(self, table_name, attribute, parameters):
        joined_parameters = ",".join(parameters)
        self.Dd.redo_dedup(self.setid, table_name, joined_parameters)

    def redo_forced_attribute_conversion(self, table_name, attribute, parameters):
        to_type     = parameters[0]
        data_format = parameters[1]
        length      = ast.literal_eval(parameters[2])
        force_mode  = parameters[3]
        self.Tt.force_attribute_type(table_name, attribute, to_type, data_format, length, force_mode)
        
        
