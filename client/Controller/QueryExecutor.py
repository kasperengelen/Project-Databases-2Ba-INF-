import re
import json

import psqlparse
import psycopg2
import sqlalchemy
import numpy as np
import pandas as pd

from Controller.DatasetHistoryManager import DatasetHistoryManager


class QueryExecutor:
    """Class that analyzes and validates user-generated PostgreSQL queries and executes them
    if they don't have syntax errors and don't violate the permissions granted to the users.
    
    Attributes:
        setid: The id of the dataset that the user wants to modify
        db_conn: psycopg2 database connection to execute the PostgreSQL queries
        engine: SQLalchemy engine to use pandas functionality
        write_perm: Boolean specifying whether the user passing the query has atleast write permissions
        track: Boolean indicating whether TableTransformer needs to write to history.
    """

    def __init__(self, setid, db_conn, engine, write_perm, track=True):
        self.schema = str(setid)
        self.db_conn = db_conn
        self.engine = engine
        self.write_perm = write_perm
        self.history_manager = DatasetHistoryManager(setid, db_conn, track)
        self.altered_data = {}

    class QueryError(Exception):
        """
        Base exception for QueryExecutor to reference all it's exceptions.
        """

    class SyntaxError(Exception):
        """
        This exception is raised whenever the user provides a query with a syntax error.
        """

    class PermissionError(Exception):
        """
        This exception is raised whenever an user attempts to execute a statement in a query, but
        the user does not have the permission to do so.
        """

    class ProgrammingError(Exception):
        """
        This exception is raised whenever a quey with the correct syntax still contains elements
        that make it fail (e.g., referencing tables / columns that don't exist in the dataset).
        """

    def execute_transaction(self, query):
        """Method that executes SQL statements (transaction) if they are correct.
        Even just one SQL statement counts as a valid transaction for this method.
        This method can be called for one or more statements without problem.
        """
        statements = filter(None, query.split(';'))
        try:
            for statement in statements:
                result = self.__execute_statement(statement)

        #Catch whatever went wrong and rollback all the changes before re-raising the exception
        except:
            self.db_conn.rollback()
            raise

        #If nothing failed and every statement has been succesfully executed commit
        self.db_conn.commit()
        #If some table has been altered due one or more queries in this transaction
        if len(self.altered_data) > 0:
            for key, value in self.altered_data.items():
                value = value.replace('"', '\\"') 
                self.history_manager.write_to_history(key, key, '', [value], 16)
        return result
        
    def __execute_statement(self, query):
        """Execute user-generated query."""
        try: #Let's try parsing the query purely on syntax.
            statement = psqlparse.parse(query)[0]
            print(statement.where_clause)
            return None

        except psqlparse.exceptions.PSqlParseError as e:
            raise self.SyntaxError(str(e))

        #Check if the statement is permitted
        if self.__assert_permitted_statement(statement) is False:
            raise self.PermissionError('Unable to execute query: permission denied to this user.')

        #Check if the query doesn't contain bogus tables
        valid_tables = self.__get_valid_tables()
        used_tables = statement.tables()
        for table in used_tables:
            if table not in valid_tables:
                error_msg = 'Error, table "' + table + '" does not exist in this dataset. Please verify the spelling is correct.'
                raise self.ProgrammingError(error_msg)

        if type(statement) == psqlparse.nodes.SelectStmt:
            return self.__execute_visual(query, used_tables)
        else:
            return self.__execute_simple(query, used_tables, query)

    def __execute_simple(self, query, tables, original_query):
        """Method used for simple execution of queries. This method is used for queries
        without result that needs to be visualized (INSERT INTO, UPDATE, DELETE statements).
        """
        cur = self.db_conn.cursor()
        try:
            cur.execute('SET search_path to "{}"'.format(self.schema))
            cur.execute(query)
            cur.execute('SET search_path to public')
        except psycopg2.ProgrammingError as e:
            raise self.SyntaxError(str(e))

        modified_table = self.__get_modified_table(original_query, tables)
        parameter = '"{}"'.format(original_query + ';')
        if modified_table in self.altered_data:
            self.altered_data[modified_table] += parameter
        else:
           self.altered_data[modified_table] = parameter 
        return None
            
    def __execute_visual(self, query, tables):
        """Method that needs to visualize the result of the query. This is the
        case for SELECT statements that obviously need to be visualized.
        """
        cur = self.db_conn.cursor()
        try:
            cur.execute('SET search_path to "{}"'.format(self.schema))
            cur.execute(query)
             
        except psycopg2.ProgrammingError as e:
            raise self.SyntaxError(str(e))

        cols =  [desc[0] for desc in cur.description]
        json_string = json.dumps(cur.fetchall(), default=str)
        cur.execute('SET search_path to public')
        return (cols, json_string)

    def __assert_permitted_statement(self, statement_obj):
        """We need to assert whether the query contains statements that the user is allowed
        to use. Only INSERT INTO, UPDATE, DELETE and SELECT statements are allowed.
        """

        if self.write_perm is False: # Without write permissions, you can only use SELECT statmements.
            if type(statement_obj) != psqlparse.nodes.SelectStmt:
                return False
                
        permitted = [psqlparse.nodes.parsenodes.SelectStmt, psqlparse.nodes.parsenodes.InsertStmt,
                     psqlparse.nodes.parsenodes.UpdateStmt, psqlparse.nodes.parsenodes.DeleteStmt]

        if type(statement_obj) in permitted:
            return True
        else:
            return False

    def __get_valid_tables(self):
        """Get all valid tablenames that could possibly appear in the query."""
        cur = self.db_conn.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s", [self.schema])
        result = cur.fetchall()
        tablenames = [t[0] for t in result]
        return tablenames

    def __get_modified_table(self, query, tables):
        """Method that returns which table has been modified after a INSERT / UPDATE / DELETE statement.
        This is usually the first table occurence in the query.
        """
        words = query.split()
        for w in words:
            if w in tables:
                return w
        raise ValueError()
