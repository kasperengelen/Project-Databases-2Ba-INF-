from psycopg2 import sql
from psycopg2 import extensions
from utils import get_db
from utils import get_sqla_eng
from Controller.TableViewer import TableViewer
from Controller.TableTransformer import TableTransformer
from Controller.DataLoader import DataLoader
from Controller.DatasetHistoryManager import DatasetHistoryManager
from Model.DataDownloader import DataDownloader

class DatasetInfo:
    """Class that represents a dataset."""

    @staticmethod
    def fromSqlTuple(tupl, db_conn = None):
        """Convert a SQL-tuple containing information about a user
        to a DatasetInfo object."""

        setid = int(tupl[0])
        setname = str(tupl[1])
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
        
        if not tablename in self.getTableNames():
            raise RuntimeError("Invalid tablename.")

        return TableViewer(setid = self.setid, 
                            tablename = tablename, 
                            engine = get_sqla_eng(), 
                            db_connection = self.db_conn, 
                            is_original = original)
    # ENDMETHOD

    def getTableTransformer(self, tablename):
        """Retrieves a TableTransformer object associated with the specified set and table."""

        if not tablename in self.getTableNames():
            raise RuntimeError("Invalid tablename.")

        return TableTransformer(self.setid, self.db_conn, get_sqla_eng())
    # ENDMETHOD

    def getDownloader(self):
        """Retrieves a DataDownloader object associated with the dataset."""

        return DataDownloader(setid=self.setid, db_connection=self.db_conn)
    # ENDMETHOD

    def getDataLoader(self):
        """Retrieve the dataloader for this dataset."""
        return DataLoader(self.setid, self.db_conn)

    def getHistoryManager(self):
        """Retrieve the history manager for this dataset."""
        return DatasetHistoryManager(self.setid, self.db_conn)

    def deleteTable(self, tablename):
        """Deletes the specified table from the dataset."""
        if not tablename in self.getTableNames():
            raise RuntimeError("Invalid tablename.")

        cur = self.db_conn.cursor()
        cur.execute("DROP TABLE \"{}\".{};".format(int(self.setid), extensions.quote_ident(tablename, get_db().cursor())))
        cur.execute("DROP TABLE \"original_{}\".{};".format(int(self.setid), extensions.quote_ident(tablename, get_db().cursor())))
        # TODO remove history?
        self.db_conn.commit()
    # ENDMETHOD