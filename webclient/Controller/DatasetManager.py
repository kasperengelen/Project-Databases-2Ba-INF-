from utils import get_db
from DatasetInfo import DatasetInfo

class DatasetManager:
    """Class that provides facilities for managing datasets."""
    @staticmethod
    def existsID(setid):
        """Determine if there exists a dataset with the specified set id."""
        get_db().cursor().execute("SELECT * FROM SYSTEM.datasets WHERE setid=%s;", [setid])
        result = get_db().cursor().fetchone()

        return result is not None
    # ENDMETHOD

    @staticmethod
    def getDataset(setid):
        """Retrieve the dataset with the specified setid."""

        if not DatasetManager.existsID(setid):
            raise RuntimeError("There exists no dataset with the specified set id.")

        get_db().cursor().execute("SELECT * FROM SYSTEM.datasets WHERE setid=%s;", [setid])
        result = get_db().cursor().fetchone()

        return DatasetInfo.fromSqlTuple(result)
    # ENDMETHOD

    @staticmethod
    def getDatasetsForUser(userid):
        """Retrieve all datasets that the user with the specified userid has access to."""

        get_db().cursor().execute("SELECT * FROM SYSTEM.datasets WHERE setid IN (SELECT setid FROM SYSTEM.set_permissions WHERE userid = %s);", [userid])
        results = get_db().cursor().fetchall()

        retval = []

        for result in results:
            retval.append(DatasetInfo.fromSqlTuple(result))

        return retval
    # ENDMETHOD

    @staticmethod
    def createDataset(name, desc):
        """Create a new dataset with the specified name and description.
        Returns the set id of the newly created dataset."""
        
        get_db().cursor().execute("INSERT INTO SYSTEM.datasets(setname, description) VALUES (%s, %s) RETURNING setid;", [name, desc])
        setid = int(get_db().cursor().fetchall()[0][0])

        # CREATE SCHEMA
        get_db().cursor().execute("CREATE SCHEMA \"{}\";".format(int(setid)))
        get_db().commit()

        # CREATE BACKUP SCHEMA
        get_db().cursor().execute("CREATE SCHEMA \"original_{}\";".format(int(setid)))
        get_db().commit()

        return setid
    # ENDMETHOD

    @staticmethod
    def destroyDataset(setid):
        """Destroy the dataset with the specified set id."""

        # CHECK
        if not DatasetManager.existsID(setid):
            raise RuntimeError("There exists no dataset with the specified set id.")

        # DROP SCHEMA
        get_db().cursor().execute("DROP SCHEMA \"{}\" CASCADE;".format(int(setid)))
        get_db().commit()

        # DROP BACKUP SCHEMA
        get_db().cursor().execute("DROP SCHEMA \"original_{}\" CASCADE;".format(int(setid)))
        get_db().commit()


        # DELETE DATASET
        get_db().cursor().execute("DELETE FROM SYSTEM.datasets WHERE setid=%s CASCADE;", [setid])
        get_db().commit()
    # ENDMETHOD

    @staticmethod
    def getAllDatasets():
        """Retrieve a list of DatasetInfo objects that represent all datasets."""
        
        get_db().cursor().execute("SELECT * FROM SYSTEM.datasets;")
        results = get_db().cursor().fetchall()

        retval = []

        for result in results:
            retval.append(DatasetInfo.fromSqlTuple(result))

        return retval
    # ENDMETHOD

