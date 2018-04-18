from utils import get_db
from DatasetInfo import DatasetInfo
from UserManager import UserManager

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

        # create the history table
        get_db().cursor().execute(open("Controller/dataset_history.sql", 'r').read())
        get_db().cursor().execute("ALTER TABLE DATASET_HISTORY.temp RENAME TO \"{}\"".format(str(setid)))
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
        get_db().cursor().execute("DELETE FROM SYSTEM.datasets WHERE setid=%s;", [setid])
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

    @staticmethod
    def userHasAccessTo(setid, userid, minimum_perm_type):
        """Determine if the specfied user has at least
        the specified permissions for the specified set."""

        # CHECK
        if not DatasetManager.existsID(setid):
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

        if not UserManager.existsID(int(userid)):
            raise RuntimeError("Specified user does not exist.")

        get_db().cursor().execute("SELECT permission_type FROM SYSTEM.set_permissions WHERE setid=%s AND userid = %s;", [int(setid), int(userid)])
        result = get_db().cursor().fetchone()

        if result is None:
            return False

        return result[0] in higher_perm_list
    # ENDMETHOD

    @staticmethod
    def changeMetadata(setid, new_name, new_desc):
        """Change the metadata of the specified dataset."""

        # CHECK
        if not DatasetManager.existsID(setid):
            raise RuntimeError("There exists no dataset with the specified set id.")

        get_db().cursor().execute("UPDATE SYSTEM.datasets SET setname = %s, description = %s WHERE setid = %s;", [new_name, new_desc, int(setid)])
        get_db().commit()
    # ENDMETHOD
# ENDCLASS
