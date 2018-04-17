from psycopg2 import sql
from psycopg2 import extensions
from utils import get_db
from utils import get_sqla_eng
from UserManager import UserManager
from TableViewer import TableViewer
from TableTransformer import TableTransformer

class DatasetInfo:
    """Class that represents a dataset."""

    @staticmethod
    def fromSqlTuple(tupl):
        """Convert a SQL-tuple containing information about a user
        to a DatasetInfo object."""

        setid = int(tupl[0])
        setname = str(tupl[1])
        description = str(tupl[2])

        return DatasetInfo(setid, setname, description)
    # ENDMETHOD

    def __init__(self, setid, name, description):
        self.setid = setid
        self.name = name
        self.desc = description
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
        get_db().cursor().execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s;", [str(self.setid)])
        result = get_db().cursor().fetchall()

        tablenames = [t[0] for t in result]

        return tablenames
    # ENDMETHOD

    def getTableViewer(self, tablename):
        """Retrieves a TableViewer object associated with the specified set and table."""
        
        if not tablename in self.getTableNames():
            raise RuntimeError("Invalid tablename.")
        return TableViewer(self.setid, tablename, get_sqla_eng(), get_db())
    # ENDMETHOD

    def getTableTransformer(self, tablename):
        """Retrieves a TableTransformer object associated with the specified set and table."""
        if not tablename in self.getTableNames():
            raise RuntimeError("Invalid tablename.")

        return TableTransformer(self.setid, get_db(), get_sqla_eng())
    # ENDMETHOD

    def deleteTable(self, tablename):
        """Deletes the specified table from the dataset."""
        if not tablename in self.getTableNames():
            raise RuntimeError("Invalid tablename.")

        get_db().cursor().execute("DROP TABLE \"{}\".{};".format(int(self.setid), extensions.quote_ident(tablename, get_db().cursor())))
        get_db().cursor().execute("DROP TABLE \"original_{}\".{};".format(int(self.setid), extensions.quote_ident(tablename, get_db().cursor())))
        get_db().commit()
    # ENDMETHOD

    def changeMetadata(self, new_name, new_desc):
        """Changes the name and description of the dataset to the 
        specified values."""
        
        # UPDATE INFO IN DB
        get_db().cursor().execute("UPDATE SYSTEM.datasets SET setname = %s, description = %s WHERE setid = %s;", [new_name, new_desc, self.setid])
        get_db().commit()

        # UPDATE INFO IN CLASS
        new  = DatasetManager.DatasetManager.getDataset(self.setid)
        self.name = new.name
        self.desc = new.desc
    # ENDMETHOD

    def getAdminPerms(self):
        """Retrieve a list of UserInfo objects that represent
        the users that have admin access to the dataset."""
        
        return self.__getPermIDs('admin')
    # ENDMETHOD

    def getWritePerms(self):
        """Retrieve a list of UserInfo objects that represent
        the users that have write access to the dataset."""
        
        return self.__getPermIDs('write')
    # ENDMETHOD

    def getReadPerms(self):
        """Retrieve a list of UserInfo objects that represent
        the users that have read access to the dataset."""
        
        return self.__getPermIDs('read')
    # ENDMETHOD

    def hasAdminPerms(self, userid):
        """Determine whether the user with the specified userid has
        admin access to the dataset."""

        return userid in self.getAdminPerms()
    # ENDMETHOD

    def hasWritePerms(self, userid):
        """Determine whether the user with the specified userid has
        write access to the dataset."""

        return userid in self.getWritePerms()
    # ENDMETHOD

    def hasReadPerms(self, userid):
        """Determine whether the user with the specified userid has
        read access to the dataset."""

        return userid in self.getReadPerms()
    # ENDMETHOD

    def addPerm(self, email, perm_type):
        """Give the user with the specified userid access to
        the dataset with the specified permission type."""
        
        # DETERMINE IF perm_type IS VALID
        if not perm_type in ['admin', 'write', 'read']:
            raise RuntimeError("The specified permission type is not valid.")

        # DETERMINE IF THE USER EXISTS
        if not UserManager.existsEmail(email):
            raise RuntimeError("The specified e-mail does not belong to an existing user.")

        user = UserManager.getUserFromEmail(email)

        # DETERMINE IF USER ALREADY HAS PERMISSION
        if self.hasAdminPerms(user.userid) or self.hasWritePerms(user.userid) or self.hasReadPerms(user.userid):
            raise RuntimeError("The specified user already has access to the dataset.")

        # ADD PERMISSION
        get_db().cursor().execute("INSERT INTO SYSTEM.set_permissions(userid, setid, permission_type) VALUES (%s, %s, %s);", 
                                                                                        [int(user.userid), int(self.setid), str(perm_type)])
        get_db().commit()
    # ENDMETHOD

    def removePerm(self, userid):
        """Revoke the specified user's access to the dataset."""

        # DETERMINE IF THE USER HAS PERMISSION
        if not (self.hasAdminPerms(userid) or self.hasWritePerms(userid) or self.hasReadPerms(userid)):
            raise RuntimeError("Specified user does not have permissions for the dataset.")

        # REMOVE PERMISSION
        get_db().cursor().execute("DELETE FROM SYSTEM.set_permissions WHERE setid=%s AND userid=%s;", [int(self.setid), int(userid)])
        get_db().commit()
    # ENDMETHOD

    def getPermForUserID(self, userid):
        """Returns the permission that the specified user has."""

        if not UserManager.existsID(int(userid)):
            raise RuntimeError("Specified user does not exist.")

        get_db().cursor().execute("SELECT permission_type FROM SYSTEM.set_permissions WHERE setid=%s AND userid = %s;", [int(self.setid), int(userid)])
        result = get_db().cursor().fetchone()

        return result[0]

    # ENDMETHOD

    def __getPermIDs(self, perm_type):
        """Returns a list of userids of users that have the specified
        access-level to the dataset."""

        # DETERMINE IF perm_type IS VALID
        if not perm_type in ['admin', 'write', 'read']:
            raise RuntimeError("The specified permission type is not valid.")

        # RETRIEVE userid's
        get_db().cursor().execute("SELECT userid FROM SYSTEM.set_permissions WHERE setid = %s AND permission_type = %s;", [int(self.setid), perm_type])
        results = get_db().cursor().fetchall()

        retval = [t[0] for t in results]

        return retval
    # ENDMETHOD

import DatasetManager
