from utils import get_db
from Controller.DatasetInfo import DatasetInfo

class DatasetManager:
    """Class that provides facilities for managing datasets."""
    @staticmethod
    def existsID(setid, db_conn = None):
        """Determine if there exists a dataset with the specified set id."""

        if db_conn is None:
            db_conn = get_db()

        cur = db_conn.cursor()
        cur.execute("SELECT * FROM SYSTEM.datasets WHERE setid=%s;", [setid])
        result = cur.fetchone()

        return result is not None
    # ENDMETHOD

    @staticmethod
    def getDataset(setid, db_conn = None):
        """Retrieve the dataset with the specified setid."""

        if db_conn is None:
            db_conn = get_db()

        if not DatasetManager.existsID(setid, db_conn = db_conn):
            raise RuntimeError("There exists no dataset with the specified set id.")

        cur = db_conn.cursor()
        cur.execute("SELECT * FROM SYSTEM.datasets WHERE setid=%s;", [setid])
        result = cur.fetchone()

        return DatasetInfo.fromSqlTuple(result, db_conn = db_conn)
    # ENDMETHOD

    @staticmethod
    def getDatasetsForUser(userid, db_conn = None):
        """Retrieve all datasets that the user with the specified userid has access to."""

        if db_conn is None:
            db_conn = get_db()

        cur = db_conn.cursor()
        cur.execute("SELECT * FROM SYSTEM.datasets WHERE setid IN (SELECT setid FROM SYSTEM.set_permissions WHERE userid = %s);", [userid])
        results = cur.fetchall()

        retval = []

        for result in results:
            retval.append(DatasetInfo.fromSqlTuple(result, db_conn = db_conn))

        return retval
    # ENDMETHOD

    @staticmethod
    def createDataset(name, desc, db_conn = None):
        """Create a new dataset with the specified name and description.
        Returns the set id of the newly created dataset."""

        if db_conn is None:
            db_conn = get_db()

        cur = db_conn.cursor()
        cur.execute("INSERT INTO SYSTEM.datasets(setname, description) VALUES (%s, %s) RETURNING setid;", [name, desc])
        db_conn.commit()
        setid = int(cur.fetchone()[0])

        # CREATE SCHEMA
        db_conn.cursor().execute("CREATE SCHEMA \"{}\";".format(int(setid)))
        db_conn.commit()

        # CREATE BACKUP SCHEMA
        db_conn.cursor().execute("CREATE SCHEMA \"original_{}\";".format(int(setid)))
        db_conn.commit()

        return setid
    # ENDMETHOD

    @staticmethod
    def destroyDataset(setid, db_conn = None):
        """Destroy the dataset with the specified set id."""

        if db_conn is None:
            db_conn = get_db()

        # CHECK
        if not DatasetManager.existsID(setid, db_conn = db_conn):
            raise RuntimeError("There exists no dataset with the specified set id.")

        # DROP SCHEMA
        db_conn.cursor().execute("DROP SCHEMA \"{}\" CASCADE;".format(int(setid)))
        db_conn.commit()

        # DROP BACKUP SCHEMA
        db_conn.cursor().execute("DROP SCHEMA \"original_{}\" CASCADE;".format(int(setid)))
        db_conn.commit()

        # DELETE DATASET
        db_conn.cursor().execute("DELETE FROM SYSTEM.datasets WHERE setid=%s;", [setid])
        db_conn.commit()
    # ENDMETHOD

    @staticmethod
    def getAllDatasets(db_conn = None):
        """Retrieve a list of DatasetInfo objects that represent all datasets."""

        if db_conn is None:
            db_conn = get_db()

        cur = db_conn.cursor()
        cur.execute("SELECT * FROM SYSTEM.datasets;")
        results = cur.fetchall()

        retval = []

        for result in results:
            retval.append(DatasetInfo.fromSqlTuple(result, db_conn = db_conn))

        return retval
    # ENDMETHOD

    @staticmethod
    def changeMetadata(setid, new_name, new_desc, db_conn = None):
        """Change the metadata of the specified dataset."""

        if db_conn is None:
            db_conn = get_db()

        # CHECK
        if not DatasetManager.existsID(setid, db_conn = db_conn):
            raise RuntimeError("There exists no dataset with the specified set id.")

        db_conn.cursor().execute("UPDATE SYSTEM.datasets SET setname = %s, description = %s WHERE setid = %s;", [new_name, new_desc, int(setid)])
        db_conn.commit()
    # ENDMETHOD
# ENDCLASS
