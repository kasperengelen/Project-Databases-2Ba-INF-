import psycopg2
from psycopg2 import sql
from Controller.DatasetHistoryManager import DatasetHistoryManager
from Model.DatabaseConfiguration import DatabaseConfiguration
from Model.QueryManager import QueryManager


class JoinException(Exception):
    def __init__(self, message):
        super().__init__(message)


class TypeMismatch(JoinException):
    # exception thrown if the columns that have to be compared don't have matching types
    def __init__(self):
        super().__init__("Types of comparing columns don't match")


class NoneUniqueColumnNames(JoinException):
    # exception thrown if the two tables have columns with the same name
    def __init__(self):
        super().__init__("Tables share column names")


class NoJoinColumns(JoinException):
    # exception thrown if the two tables have no columns to join on
    def __init__(self):
        super().__init__("Can't apply natural join, no columns have matching names")


class TableJoiner:

    def __init__(self, setid, table1, table2, new_table, db_connection):
        self.schema = str(setid)
        self.table1 = table1
        self.table2 = table2
        self.new_table = new_table
        self.db_connection = db_connection
        self.cur = db_connection.cursor()
        self.query_man = QueryManager(self.db_connection, None)

    def normal_join(self, table1_columns, table2_columns, type="inner"):
        """:param type: can be 'inner', 'left', 'right' or 'full' """

        if type not in ["inner", "left", "right", "full"]:
            raise ValueError("type should be 'inner', 'left', 'right' or 'full'")

        self.__check_exceptions_normal_join(table1_columns, table2_columns)

        query = sql.SQL("CREATE TABLE {} AS (SELECT * FROM {} t1 {} JOIN {} t2 ON ").format(sql.Identifier(self.new_table),
                                                                                         sql.Identifier(self.table1),
                                                                                         sql.SQL(type),
                                                                                         sql.Identifier(self.table2))

        for i in range(len(table1_columns) - 1):
            query += sql.SQL("t1.{} = t2.{} AND ").format(sql.Identifier(table1_columns[i]),
                                                          sql.Identifier(table2_columns[i]))

        query += sql.SQL("t1.{} = t2.{})").format(sql.Identifier(table1_columns[-1]),
                                                  sql.Identifier(table2_columns[-1]))

        self.cur.execute("SET search_path TO {};".format(self.schema))
        self.cur.execute(query)
        self.db_connection.commit()

    def natural_join(self, type="inner"):
        """:param type: can be 'inner', 'left', 'right' or 'full' """
        
        if type not in ["inner", "left", "right", "full"]:
            raise ValueError("type should be 'inner', 'left', 'right' or 'full'")

        self.__check_exceptions_natural_join()

        query = sql.SQL("CREATE TABLE {} AS (SELECT * FROM ({} NATURAL {} JOIN {}))").format(sql.Identifier(self.new_table),
                                                                                             sql.Identifier(self.table1),
                                                                                             sql.SQL(type),
                                                                                             sql.Identifier(self.table2))

        self.cur.execute("SET search_path TO {};".format(self.schema))
        self.cur.execute(query)
        self.db_connection.commit()

    def __check_exceptions_normal_join(self, table1_columns, table2_columns):
        """Checks for exceptions and raises them"""

        # check if types match
        table1_types = self.query_man.get_col_types(self.schema, self.table1)
        table2_types = self.query_man.get_col_types(self.schema, self.table2)
        for i in range(len(table1_columns)):
            if table1_types[table1_columns[i]] != table2_types[table2_columns[i]]:
                raise TypeMismatch

        # check if column names are unique
        table1_cols = set(self.query_man.get_col_names(self.schema, self.table1))
        table2_cols = set(self.query_man.get_col_names(self.schema, self.table2))
        if not table1_cols.isdisjoint(table2_cols):
            raise NoneUniqueColumnNames

    def __check_exceptions_natural_join(self):
        """Checks for exceptions and raises them"""

        # check if natural join can be applied
        table1_cols = set(self.query_man.get_col_names(self.schema, self.table1))
        table2_cols = set(self.query_man.get_col_names(self.schema, self.table2))
        if table1_cols.isdisjoint(table2_cols):
            raise NoJoinColumns

        # check if types match
        join_columns = table1_cols.intersection(table2_cols)
        table1_types = self.query_man.get_col_types(self.schema, self.table1)
        table2_types = self.query_man.get_col_types(self.schema, self.table2)
        for col in join_columns:
            if table1_types[col] != table2_types[col]:
                raise TypeMismatch


if __name__ == "__main__":
    DC = DatabaseConfiguration()
    TJ = TableJoiner(37, "join1", "join2", "joined", DC.get_db())
    TJ.natural_join(type="left")
