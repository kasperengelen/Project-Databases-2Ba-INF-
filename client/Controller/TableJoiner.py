import psycopg2
from psycopg2 import sql
from Controller.DatasetHistoryManager import DatasetHistoryManager
from Model.DatabaseConfiguration import DatabaseConfiguration

class TableJoiner:

    def __init__(self, setid, table1, table2, new_table, db_connection):
        self.schema = str(setid)
        self.table1 = table1
        self.table2 = table2
        self.new_table = new_table
        self.db_connection = db_connection
        self.cur = db_connection.cursor()

    def normal_join(self, table1_columns, table2_columns):
        query = sql.SQL("CREATE TABLE {} AS (SELECT * FROM {} t1 JOIN {} t2 ON ").format(sql.Identifier(self.new_table),
                                                                                         sql.Identifier(self.table1),
                                                                                         sql.Identifier(self.table2))

        for i in range(len(table1_columns) - 1):
            query += sql.SQL("t1.{} = t2.{} AND ").format(sql.Identifier(table1_columns[i]),
                                                          sql.Identifier(table2_columns[i]))

        query += sql.SQL("t1.{} = t2.{})").format(sql.Identifier(table1_columns[-1]),
                                                  sql.Identifier(table2_columns[-1]))

        self.cur.execute("SET search_path TO {};".format(self.schema))
        self.cur.execute(query)
        self.db_connection.commit()

    def natural_join(self):
        query = sql.SQL("CREATE TABLE {} AS (SELECT * FROM ({} NATURAL INNER JOIN {}))").format(sql.Identifier(self.new_table),
                                                                                         sql.Identifier(self.table1),
                                                                                         sql.Identifier(self.table2))

        self.cur.execute("SET search_path TO {};".format(self.schema))
        self.cur.execute(query)
        self.db_connection.commit()

if __name__ == "__main__":
    DC = DatabaseConfiguration()
    TJ = TableJoiner(37, "join1", "join2", "joined", DC.get_db())
    TJ.natural_join()
