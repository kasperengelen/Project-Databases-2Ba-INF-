from Model.db_access import get_db

class DatasetPermissionsManager:
    """Class that provides facilities to manage dataset permissions."""

    @staticmethod
    def userHasSpecifiedAccessTo(setid, userid, minimum_perm_type, db_conn = None):
        """Determine if the specified user has at least the
        specified permissions for the specified dataset."""

        if db_conn is None:
            db_conn = get_db()

        # list of permissions types that are equivalent or higher
        higher_perm_list = []

        for ptype in ['admin', 'write', 'read']:
            # higher or equivalent is always added to the list
            higher_perm_list.append(ptype)

            if ptype == minimum_perm_type:
                break;
        # ENDFOR

        user_perm = DatasetPermissionsManager.getPermForUserID(setid, userid, db_conn = db_conn)

        # determine if the user's permission is higher than or equal to the required permission
        return user_perm in higher_perm_list
    # ENDFUNCTION

    @staticmethod
    def getAdminPerms(setid, db_conn = None):
        """Retrieve a list of userids that represent the users that have
        admin access to the dataset."""

        if db_conn is None:
            db_conn = get_db()

        return DatasetPermissionsManager.__getPermIDs(setid, 'admin', db_conn = db_conn)
    # ENDFUNCTION

    @staticmethod
    def getWritePerms(setid, db_conn = None):
        """Retrieve a list of userids that represent the users that have
        write access to the dataset."""

        if db_conn is None:
            db_conn = get_db()

        return DatasetPermissionsManager.__getPermIDs(setid, 'write', db_conn = db_conn)
    # ENDFUNCTION

    @staticmethod
    def getReadPerms(setid, db_conn = None):
        """Retrieve a list of userids that represent the users that have
        read access to the dataset."""

        if db_conn is None:
            db_conn = get_db()

        return DatasetPermissionsManager.__getPermIDs(setid, 'read', db_conn = db_conn)
    # ENDFUNCTION

    @staticmethod
    def __getPermIDs(setid, perm_type, db_conn = None):
        """Returns a list of userids that have the specified
        access level to the dataset."""

        if db_conn is None:
            db_conn = get_db()

        # determine if perm_type is valid
        if not perm_type in ['admin', 'write', 'read']:
            raise RuntimeError("The specified permission type is not valid.")

        cur = db_conn.cursor()
        cur.execute("SELECT userid FROM SYSTEM.set_permissions WHERE setid = %s and permission_type = %s;", [int(setid), perm_type])
        results = cur.fetchall()

        return [t[0] for t in results]
    # ENDFUNCTION

    @staticmethod
    def addPerm(setid, userid, perm_type, db_conn = None):
        """Give the specified user the specified access-level to
        the specified dataset."""

        if db_conn is None:
            db_conn = get_db()

        # determine if perm_type is valid
        if not perm_type in ['admin', 'write', 'read']:
            raise RuntimeError("The specified permission type is not valid.")

        # check if user does not already have permission
        if DatasetPermissionsManager.getPermForUserID(setid, userid, db_conn = db_conn) is not None:
            raise RuntimeError("The specified user already has access to the dataset.")

        db_conn.cursor().execute("INSERT INTO SYSTEM.set_permissions(userid, setid, permission_type) VALUES (%s, %s, %s);",
                                                                                [int(userid), int(setid), str(perm_type)])
        db_conn.commit()
    # ENDFUNCTION

    @staticmethod
    def removePerm(setid, userid, db_conn = None):
        """Revoke the specified user's access to the specified
        dataset."""

        if db_conn is None:
            db_conn = get_db()

        if DatasetPermissionsManager.getPermForUserID(setid, userid, db_conn = db_conn) is None:
            raise RuntimeError("The specified user does not have access to the specified dataset.")

        db_conn.cursor().execute("DELETE FROM SYSTEM.set_permissions WHERE setid = %s AND userid = %s;", [int(setid), int(userid)])
        db_conn.commit()
    # ENDFUNCTION

    @staticmethod
    def getPermForUserID(setid, userid, db_conn = None):
        """Returns the permission that the specified user has."""

        if db_conn is None:
            db_conn = get_db()

        cur = db_conn.cursor()
        cur.execute("SELECT permission_type FROM SYSTEM.set_permissions WHERE setid = %s AND userid = %s;", [int(setid), int(userid)])
        result = cur.fetchone()

        if result is None:
            return None

        return result[0]
    # ENDFUNCTION
# ENDCLASS