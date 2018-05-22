from psycopg2 import sql
from psycopg2 import extensions

from Model.db_access import get_db
from Model.db_access import get_sqla_eng
from Model.TableUploader import TableUploader
from Model.DatasetDownloader import DatasetDownloader

from Controller.TableViewer import TableViewer
from Controller.TableTransformer import TableTransformer
from Controller.DatasetHistoryManager import DatasetHistoryManager
from Controller.TableJoiner import TableJoiner
from Controller.QueryExecutor import QueryExecutor

class DatasetInfo:
    """Class that represents a dataset."""

    @staticmethod
    def fromSqlTuple(tupl, db_conn = None):
        """Convert a SQL-tuple containing information about a user
        to a DatasetInfo object."""

        setid       = int(tupl[0])
        setname     = str(tupl[1])
        description = str(tupl[2])

        return DatasetInfo(setid, setname, description, db_conn = db_conn)
    # ENDMETHOD

    def __init__(self, setid, name, description, db_conn = None):
        self.setid = setid
        self.name = name
        self.desc = description

        if db_conn is None:
            self.db_conn = get_db()
        else:
            self.db_conn = db_conn
    # ENDMETHOD

    def toDict(self):
        """Retrieve a JSON-compatible dict that contains
        information about the dataset."""
        return {
            'setid': self.setid,
            'name': self.name,
            'desc': self.desc
        }
    # ENDMETHOD

    def getTableNames(self):
        """Retrieve the names of the tables that are part of the dataset."""

        cur = self.db_conn.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s;", [str(self.setid)])
        result = cur.fetchall()

        tablenames = [t[0] for t in result]

        return tablenames
    # ENDMETHOD

    def getOriginalTableNames(self):
        """Retrieve the names of the original tables that are part of the dataset."""

        cur = self.db_conn.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s;", ["original_" + str(self.setid)])
        result = cur.fetchall()

        tablenames = [t[0] for t in result]

        return tablenames
    # ENDMETHOD

    def getTableViewer(self, tablename, original = False):
        """Retrieves a TableViewer object associated with the specified set and table."""

        return TableViewer(setid = self.setid, 
                            tablename = tablename, 
                            engine = get_sqla_eng(), 
                            db_connection = self.db_conn, 
                            is_original = original)
    # ENDMETHOD

    def getTableTransformer(self, tablename):
        """Retrieves a TableTransformer object associated with the specified set and table."""

        return TableTransformer(self.setid, self.db_conn, get_sqla_eng())
    # ENDMETHOD

    def getDownloader(self):
        """Retrieves a DatasetDownloader object associated with the dataset."""

        return DatasetDownloader(setid=self.setid, db_connection=self.db_conn)
    # ENDMETHOD

    def getTableJoiner(self, table1, table2, newtable):
        """Retrieves a TableJoiner object for this dataset and the specified table names."""

        return TableJoiner(setid= self.setid, table1=table1, table2=table2, new_table=newtable, db_connection=self.db_conn)
    # ENDMETHOD

    def getUploader(self):
        """Retrieve the TableUploader for this dataset."""
        return TableUploader(self.setid, db_connection=self.db_conn, engine=get_sqla_eng())
    # ENDMETHOD

    def getHistoryManager(self):
        """Retrieve the history manager for this dataset."""
        return DatasetHistoryManager(self.setid, self.db_conn)
    # ENDMETHOD

    def getQueryExecutor(self, write_perm):
        """Retrieve the query executor associated with this set."""

        return QueryExecutor(setid=self.setid, db_conn=self.db_conn, engine=get_sqla_eng(), write_perm=write_perm)
    # ENDMETHOD

    def deleteTable(self, tablename):
        """Deletes the specified table from the dataset."""
        if not tablename in self.getTableNames():
            raise RuntimeError("Invalid tablename.")

        # TODO update this for original tables
        cur = self.db_conn.cursor()
        cur.execute("DROP TABLE \"{}\".{};".format(int(self.setid), extensions.quote_ident(tablename, get_db().cursor())))
        cur.execute("DROP TABLE IF EXISTS original_{}.{}".format(int(self.setid), extensions.quote_ident(tablename, get_db().cursor())))
        self.db_conn.commit()
    # ENDMETHOD
