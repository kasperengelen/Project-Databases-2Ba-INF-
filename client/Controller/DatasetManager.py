from utils import get_db
from Controller.DatasetInfo import DatasetInfo

class DatasetManager:
    """Class that provides facilities for managing datasets."""
    @staticmethod
    def existsID(setid, db_conn = None):
        """Determine if there exists a dataset with the specified set id."""

        if db_conn is None:
            db_conn = get_db()

        db_conn.cursor().execute("SELECT * FROM SYSTEM.datasets WHERE setid=%s;", [setid])
        result = db_conn.cursor().fetchone()

        return result is not None
    # ENDMETHOD

    @staticmethod
    def getDataset(setid, db_conn = None):
        """Retrieve the dataset with the specified setid."""

        if db_conn is None:
            db_conn = get_db()

        if not DatasetManager.existsID(setid, db_conn = db_conn):
            raise RuntimeError("There exists no dataset with the specified set id.")

        db_conn.cursor().execute("SELECT * FROM SYSTEM.datasets WHERE setid=%s;", [setid])
        result = db_conn.cursor().fetchone()

        return DatasetInfo.fromSqlTuple(result, db_conn = db_conn)
    # ENDMETHOD

    @staticmethod
    def getDatasetsForUser(userid, db_conn = None):
        """Retrieve all datasets that the user with the specified userid has access to."""

        if db_conn is None:
            db_conn = get_db()

        db_conn.cursor().execute("SELECT * FROM SYSTEM.datasets WHERE setid IN (SELECT setid FROM SYSTEM.set_permissions WHERE userid = %s);", [userid])
        results = db_conn.cursor().fetchall()

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

        db_conn.cursor().execute("INSERT INTO SYSTEM.datasets(setname, description) VALUES (%s, %s) RETURNING setid;", [name, desc])
        setid = int(db_conn.cursor().fetchall()[0][0])

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

        db_conn.cursor().execute("SELECT * FROM SYSTEM.datasets;")
        results = db_conn.cursor().fetchall()

        retval = []

        for result in results:
            retval.append(DatasetInfo.fromSqlTuple(result, db_conn = db_conn))

        return retval
    # ENDMETHOD

    @staticmethod
    def userHasAccessTo(setid, userid, minimum_perm_type, db_conn = None):
        """Determine if the specfied user has at least
        the specified permissions for the specified set."""

        if db_conn is None:
            db_conn = get_db()

        # CHECK
        if not DatasetManager.existsID(setid, db_conn = db_conn):
            raise RuntimeError("There exists no dataset with the specified set id.")

        # list of permission types that are equivalent or higher
        higher_perm_list = []

        for ptype in ['admin', 'write', 'read']:
            # higher or equivalent perm is always added to the list
            higher_perm_list.append(ptype)

            # stop if equivalent perm is reached
            if ptype == minimum_perm_type:
                break;
        # ENDFOR

        db_conn.cursor().execute("SELECT permission_type FROM SYSTEM.set_permissions WHERE setid=%s AND userid = %s;", [int(setid), int(userid)])
        result = db_conn.cursor().fetchone()

        if result is None:
            return False

        return result[0] in higher_perm_list
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
