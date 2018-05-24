from Model.db_access import get_db
from Controller.DatasetInfo import DatasetInfo
from Model.QueryManager import QueryManager

class DatasetManager:
    """Class that provides facilities for managing datasets."""
    @staticmethod
    def existsID(setid, db_conn = None):
        """Determine if there exists a dataset with the specified set id."""

        if db_conn is None:
            db_conn = get_db()

        qm = QueryManager(db_conn = db_conn, engine = None)
        result = qm.getDataset(setid=setid)

        return len(result) > 0
    # ENDMETHOD

    @staticmethod
    def getDataset(setid, db_conn = None):
        """Retrieve the dataset with the specified setid. If no dataset
        with the specified setid exists, None will be returned."""

        if db_conn is None:
            db_conn = get_db()

        if not DatasetManager.existsID(setid, db_conn = db_conn):
            raise RuntimeError("There exists no dataset with the specified set id.")

        qm = QueryManager(db_conn = db_conn, engine = None)
        results = qm.getDataset(setid=setid)

        if not results:
            return None

        return DatasetInfo(setid         = results[0]['setid'],
                           name          = results[0]['setname'],
                           description   = results[0]['description'],
                           creation_date = results[0]['creation_date'],
                           db_conn       = db_conn)
    # ENDMETHOD

    @staticmethod
    def getDatasetsForUser(userid, db_conn = None):
        """Retrieve all datasets that the user with the specified userid has access to."""

        if db_conn is None:
            db_conn = get_db()

        qm = QueryManager(db_conn=db_conn, engine=None)
        results = qm.getDatasetsForUserID(userid=userid)

        retval = []

        for result in results:
            retval.append(DatasetInfo(setid         = result['setid'],
                                      name          = result['setname'],
                                      description   = result['description'],
                                      creation_date = result['creation_date'],
                                      db_conn       = db_conn))
        # ENDFOR

        return retval
    # ENDMETHOD

    @staticmethod
    def createDataset(name, desc, db_conn = None):
        """Create a new dataset with the specified name and description.
        Returns the set id of the newly created dataset."""

        if db_conn is None:
            db_conn = get_db()

        qm = QueryManager(db_conn = db_conn, engine = None)
        setid = qm.insertDataset(setname = name, description = desc, returning = 'setid')

        # schema
        qm.createSchema(str(setid)) 
        # backup schema
        qm.createSchema("original_" + str(setid))

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

        qm = QueryManager(db_conn = db_conn, engine = None)
        # drop schema
        qm.destroySchema(str(setid), cascade=True)
        # drop backup schema
        qm.destroySchema("original_" + str(setid), cascade=True)
        # delete dataset
        qm.deleteDataset(setid=setid)
    # ENDMETHOD

    @staticmethod
    def getAllDatasets(db_conn = None):
        """Retrieve a list of DatasetInfo objects that represent all datasets."""

        if db_conn is None:
            db_conn = get_db()

        qm = QueryManager(db_conn = db_conn, engine = None)
        results = qm.getDataset()

        retval = []

        for result in results:
            retval.append(DatasetInfo(setid         = result['setid'],
                                      name          = result['setname'],
                                      description   = result['description'],
                                      creation_date = result['creation_date'],
                                      db_conn       = db_conn))

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

        qm = QueryManager(db_conn = db_conn, engine=None)
        qm.updateDataset(reqs={'setid': setid}, sets={'setname': new_name, 'description': new_desc})
    # ENDMETHOD
# ENDCLASS
