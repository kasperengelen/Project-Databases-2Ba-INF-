
import psycopg2
import psycopg2.extras

class QueryManager:

    def __init__(self, db_conn, engine):
        self.db_conn = db_conn
        self.engine = engine
        self.dict_cur = self.db_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def getUser(self, *args, **kwargs):
        """Method to retrieve user entries from the SYSTEM.user_accounts table.
        Fields:
            userid (int),
            fname  (str),
            lname  (str),
            email  (str),
            passwd (str),
            admin  (bool),
            active (bool),
            register_data (list[d (int), m (int), y (int)])

        Returns a list of all tuples that conform to the specified requirements.
        The format will be a list of dicts that map field names to the tuple's values.

        Example:

        getUser(userid=5, fname="Peter"); getUser(lname="Peter")
        """

        self.__check_specified_params('system.user_accounts', kwargs)

        query, values = self.__form_select_statement('system.user_accounts', kwargs)

        self.dict_cur.execute(query, values)

        return self.dict_cur.fetchall()
    # ENDFUNCTION.

    def getDataset(self):
        pass

    def existsUser(self):
        pass

    def existsDataset(self):
        pass

    def get_table_names(self, schema):
        self.dict_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s;",
                         [schema])
        result = self.dict_cur.fetchall()
        table_names = [t[0] for t in result]
        return table_names

    def get_col_types(self, schema, tablename):
        cur = self.db_conn.cursor()
        cur.execute("SELECT column_name, data_type FROM information_schema.columns "
                         "WHERE table_schema = '{}' AND table_name = '{}'".format(schema, tablename))
        types = cur.fetchall()
        types_dict = dict()
        for tuple in types:
            types_dict[tuple[0]] = tuple[1]
        return types_dict

    def get_col_names(self, schema, tablename):
        self.dict_cur.execute("SELECT column_name,  FROM information_schema.columns "
                         "WHERE table_schema = '{}' AND table_name = '{}'".format(schema, tablename))
        col_names = self.dict_cur.fetchall()
        return col_names

    def __check_specified_params(self, tablename, specified_fields):
        tablefields = self.__get_table_fields(tablename)

        # check if all specified fields are valid
        for param_name, param_value in specified_fields.items():
            if not param_name in tablefields:
                raise RuntimeError("Invalid field specified: '" + param_name + "'")

            field_type = tablefields[param_name]

            if not isinstance(param_value, field_type):
                raise RuntimeError("Invalid type for field '" + field_name + "': '" + str(type(param_value)) + "' specified, '" + str(field_type) + "' expected.")
        # ENDFOR
    # ENDFUNCTION

    def __form_select_statement(self, tablename, specified_fields):

        query = "SELECT * FROM " + tablename + " WHERE "
        values = []

        tablefields = self.__get_table_fields(tablename)

        # process fields
        for field_name, field_type in tablefields.items():
            # check if field is specified.
            if field_name in kwargs:
                param_value = kwargs[field_name]

                query += "" + field_name + "= %s, "
                values.append(kwargs[field_name])
        # ENDFOR

        query = query[:-2] + ";"

        return query, values
    # ENDFUNCTION

    def __get_table_fields(self, tablename):
        """Retrieve a dict that maps the fieldnames of the specified table to types."""

        if tablename == 'system.user_accounts':
            return {
                "userid": int,
                "fname":  str,
                "lname":  str,
                "email":  str,
                "passwd": str,
                "admin":  bool,
                "active": bool,
                "register_date": None # fix this
            }
        elif tablename == 'system.datasets':
            return {
                "setid": int,
                "setname": str,
                "description": str,
                "creation_date": None # fix this
            }
        elif tablename == 'system.set_permissions':
            return {
                "userid": int,
                "setid": int,
                "permission_type": str
            }
        elif tablename == 'system.dataset_history':
            return {
                "setid": int,
                "table_name": int,
                "attribute": str,
                "transformation_type": int,
                "parameters": [str, "inf"],
                "transformation_date": None # fix this
            }
        else: 
            raise RuntimeError("Invalid tablename specified: '" + tablename + "'")
    # ENDFUNCTION