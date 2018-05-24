from psycopg2 import sql
from psycopg2 import extensions
import datetime

from Model.db_access import get_db
from Model.db_access import get_sqla_eng
from Model.TableUploader import TableUploader
from Model.DatasetDownloader import DatasetDownloader
from Model.QueryManager import QueryManager

from Controller.TableViewer import TableViewer
from Controller.TableTransformer import TableTransformer
from Controller.DatasetHistoryManager import DatasetHistoryManager
from Controller.TableJoiner import TableJoiner
from Controller.QueryExecutor import QueryExecutor
from Controller.Deduplicator import Deduplicator
from Controller.TransformationReverser import TransformationReverser

class DatasetInfo:
    """Class that represents a dataset."""

    def __init__(self, setid, name, description, creation_date, db_conn = None):
        self.setid = setid
        self.name = name
        self.desc = description
        self.creation_date = creation_date

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
            'desc': self.desc,
            "creation_date": self.creation_date.strftime("%d-%m-%Y"),
            "creation_time": self.creation_date.strftime("%H:%M:%S"),
        }
    # ENDMETHOD

    # TODO user querymanager
    def getTableNames(self):
        """Retrieve the names of the tables that are part of the dataset."""

        qm = QueryManager(db_conn = self.db_conn, engine = None)
        tablenames = qm.get_table_names(str(self.setid))

        return tablenames
    # ENDMETHOD

    def getOriginalTableNames(self):
        """Retrieve the names of the original tables that are part of the dataset."""

        qm = QueryManager(db_conn = self.db_conn, engine = None)
        tablenames = qm.get_table_names("original_" + str(self.setid))

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

    def getDeduplicator(self):
        """Retrieve the Deduplicator for this dataset."""
        return Deduplicator(self.db_conn, get_sqla_eng())
    # ENDMETHOD

    def getTransformationReverser(self, tablename):
        """Retrieve the TransformationReverser for this dataset and the specified table."""

        return TransformationReverser(setid=self.setid, table_name=tablename, db_connection=self.db_conn, engine=get_sqla_eng())

    # add to querymanager
    def deleteTable(self, tablename):
        """Deletes the specified table from the dataset."""
        if not tablename in self.getTableNames():
            raise RuntimeError("Invalid tablename.")

        qm = QueryManager(db_conn = self.db_conn, engine = None)
        qm.destroyTable("\"{}\".\"{}\"".format(self.setid, tablename), cascade = True)
        qm.destroyTable("\"original_{}\".\"{}\"".format(self.setid, tablename), if_exists = True, cascade = True) # this one may not always exist!
    # ENDMETHOD
