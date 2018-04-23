from psycopg2 import sql
from psycopg2 import extensions
from utils import get_db
from utils import get_sqla_eng
from Controller.UserManager import UserManager
from Controller.TableViewer import TableViewer
from Controller.TableTransformer import TableTransformer
from Controller.DataLoader import DataLoader

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
        self.db_conn.cursor().execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s;", [str(self.setid)])
        result = self.db_conn.cursor().fetchall()

        tablenames = [t[0] for t in result]

        return tablenames
    # ENDMETHOD

    def getTableViewer(self, tablename, engine = None, original = False):
        """Retrieves a TableViewer object associated with the specified set and table."""
        
        if engine is None:
            engine = get_sqla_eng()

        if not tablename in self.getTableNames():
            raise RuntimeError("Invalid tablename.")
        return TableViewer(setid = self.setid, 
                            tablename = tablename, 
                            engine = engine, 
                            db_connection = self.db_conn, 
                            is_original = original)
    # ENDMETHOD

    def getTableTransformer(self, tablename, engine = None):
        """Retrieves a TableTransformer object associated with the specified set and table."""

        if engine is None:
            engine = get_sqla_eng()

        if not tablename in self.getTableNames():
            raise RuntimeError("Invalid tablename.")

        return TableTransformer(self.setid, self.db_conn, engine)
    # ENDMETHOD

    def getDataLoader(self):
        """Retrieve the dataloader for this dataset."""
        return DataLoader(self.setid, self.db_conn)

    def deleteTable(self, tablename):
        """Deletes the specified table from the dataset."""
        if not tablename in self.getTableNames():
            raise RuntimeError("Invalid tablename.")

        self.db_conn.cursor().execute("DROP TABLE \"{}\".{};".format(int(self.setid), extensions.quote_ident(tablename, get_db().cursor())))
        self.db_conn.cursor().execute("DROP TABLE \"original_{}\".{};".format(int(self.setid), extensions.quote_ident(tablename, get_db().cursor())))
        self.db_conn.commit()
    # ENDMETHOD

    def getAdminPerms(self):
        """Retrieve a list of userid's that represent
        the users that have admin access to the dataset."""
        
        return self.__getPermIDs('admin')
    # ENDMETHOD

    def getWritePerms(self):
        """Retrieve a list of userid's objects that represent
        the users that have write access to the dataset."""
        
        return self.__getPermIDs('write')
    # ENDMETHOD

    def getReadPerms(self):
        """Retrieve a list of userid's objects that represent
        the users that have read access to the dataset."""
        
        return self.__getPermIDs('read')
    # ENDMETHOD

    def addPerm(self, email, perm_type):
        """Give the user with the specified userid access to
        the dataset with the specified permission type."""
        
        # DETERMINE IF perm_type IS VALID
        if not perm_type in ['admin', 'write', 'read']:
            raise RuntimeError("The specified permission type is not valid.")

        # DETERMINE IF THE USER EXISTS
        if not UserManager.existsEmail(email, self.db_conn):
            raise RuntimeError("The specified e-mail does not belong to an existing user.")

        user = UserManager.getUserFromEmail(email, self.db_conn)

        # DETERMINE IF USER ALREADY HAS PERMISSION
        if (user.userid in self.getAdminPerms()) or (user.userid in self.getWritePerms()) or (user.userid in self.getReadPerms()):
            raise RuntimeError("The specified user already has access to the dataset.")

        # ADD PERMISSION
        self.db_conn.cursor().execute("INSERT INTO SYSTEM.set_permissions(userid, setid, permission_type) VALUES (%s, %s, %s);", 
                                                                                        [int(user.userid), int(self.setid), str(perm_type)])
        self.db_conn.commit()
    # ENDMETHOD

    def removePerm(self, userid):
        """Revoke the specified user's access to the dataset."""

        # DETERMINE IF THE USER HAS PERMISSION
        if not ((userid in self.getAdminPerms()) or (userid in self.getWritePerms()) or (userid in self.getReadPerms())):
            raise RuntimeError("Specified user does not have permissions for the dataset.")

        # REMOVE PERMISSION
        self.db_conn.cursor().execute("DELETE FROM SYSTEM.set_permissions WHERE setid=%s AND userid=%s;", [int(self.setid), int(userid)])
        self.db_conn.commit()
    # ENDMETHOD

    def getPermForUserID(self, userid):
        """Returns the permission that the specified user has."""

        if not UserManager.existsID(int(userid), self.db_conn):
            raise RuntimeError("Specified user does not exist.")

        self.db_conn.cursor().execute("SELECT permission_type FROM SYSTEM.set_permissions WHERE setid=%s AND userid = %s;", [int(self.setid), int(userid)])
        result = self.db_conn.cursor().fetchone()

        return result[0]

    # ENDMETHOD

    def __getPermIDs(self, perm_type):
        """Returns a list of userids of users that have the specified
        access-level to the dataset."""

        # DETERMINE IF perm_type IS VALID
        if not perm_type in ['admin', 'write', 'read']:
            raise RuntimeError("The specified permission type is not valid.")

        # RETRIEVE userid's
        self.db_conn.cursor().execute("SELECT userid FROM SYSTEM.set_permissions WHERE setid = %s AND permission_type = %s;", [int(self.setid), perm_type])
        results = self.db_conn.cursor().fetchall()

        retval = [t[0] for t in results]

        return retval
    # ENDMETHOD