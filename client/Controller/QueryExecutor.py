import re

import psqlparse
import psycopg2
from psycopg2 import sql
import sqlalchemy

from Controller.DatasetHistoryManager import DatasetHistoryManager


class QueryExecutor:
    """Class that analyzes and validates user-generated PostgreSQL queries and executes them
    if they don't have syntax errors and don't violate the permissions granted to the users.
    
    Attributes:
        setid: The id of the dataset that the user wants to modify
        db_conn: psycopg2 database connection to execute the PostgreSQL queries
        engine: SQLalchemy engine to use pandas functionality
        write_perm: Boolean specifying whether the user passing the query has atleast write permissions
    """

    def __init__(self, setid, db_conn, engine, write_perm):
        self.schema = str(setid)
        self.db_conn = db_conn
        self.engine = engine
        self.write_perm = write_perm
        self.history_manager = DatasetHistoryManager(setid, db_conn, True)


    def execute_query(self, query):
        """Execute user-generated query."""
        try: #Let's try parsing the query purely on syntax.
            statement = psqlparse.parse(query)[0]

        except psqlparse.exceptions.PSqlParseError as e:
            raise ValueError(str(e))

        #Check if the statement is permitted
        if self.__assert_permitted_statement(statement) is False:
            raise ValueError('Permission denied, users are only allowed to execute ...')

        #Check if the query doesn't contain bogus tables
        valid_tables = self.__get_valid_tables()
        used_tables = statement.tables()
        for table in used_tables:
            if table not in valid_tables:
                error_msg = 'Error, table "' + table + '" does not exist in this dataset. Please verify.'
                raise ValueError(error_msg)

        #Modify the query by mapping the tables to the correct internal tables.
        internal_query = query
        regex = '(?<!([^\s,])){}(?!([^\s,]))' #Matches {} only seperated by whitespaces or commas, just like SQL tables in queries.
        for table in used_tables:
            internal_table = '"{}".'.format(self.schema) + table
            cur_regex = regex.format(table)
            internal_query = re.sub(cur_regex, internal_table, internal_query)

        if type(statement) == psqlparse.nodes.SelectStmt:
            self.__execute_visual(internal_query)
        else:
            self.__execute_simple(internal_query, used_tables, query)
        
            


    def __execute_simple(self, query, tables, original_query):
        """Method used for simple execution of queries. This method is used for queries
        without result that needs to be visualized (INSERT INTO, UPDATE, DELETE statements).
        """
        cur = self.db_conn.cursor()
        try:
            cur.execute(query)
            
        except Exception as e:
            raise ValueError(e)

        self.db_conn.commit()
        modified_table = self.__get_modified_table(original_query, tables)
        parameter = '"{}"'.format(original_query)
        self.history_manager.write_to_history(modified_table, modified_table, '', [parameter], 16)
        print(query)
            

    def __execute_visual(self, query):
        """Method that needs to visualize the result of the query. This is the
        case for SELECT statements that obviously need to be visualized.
        """
        cur = self.db_conn.cursor()
        try:
            cur.execute(query)
            
        except Exception as e:
            raise ValueError(e)

        self.db_conn.commit()
        print(query)


    def __assert_permitted_statement(self, statement_obj):
        """We need to assert whether the query contains statements that the user is allowed
        to use. Only INSERT INTO, UPDATE, DELETE and SELECT statements are allowed.
        """

        if self.write_perm is False:
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
        """Method that returns which table has been modified after a INSERT / UPDATE / DELETE statement."""
        words = query.split()
        for w in words:
            if w in tables:
                return w
        raise ValueError()
        



        





if __name__ == '__main__':
    #qe = QueryExecutor(1, None, None)
    #qe.execute_query('SELECT * FROM mytable; DROP DATABASE pingu')
    strong = 'SELECT mytable_col FROM mytable,mytable'
    ap = re.sub('(?<!([^\s,]))mytable(?!([^\s,]))', 'pom', strong)
    print(ap)
