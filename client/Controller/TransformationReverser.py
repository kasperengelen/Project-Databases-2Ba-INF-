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
        self.table_name = table_name
        self.db_connection = db_connection
        self.engine = engine
        self.Tt = TableTransformer()
        self.Qe = QueryExecutor()
        self.Dd = Deduplicator()


    def undo_last_transformation(self):
        """Method that undoes the latest table transformation if possible."""
        pass

    def delete_history_entry(self, t_id):
        """Method that deletes a history entry given the t_id."""
        pass
