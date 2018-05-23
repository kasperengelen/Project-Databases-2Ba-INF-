
import psycopg2
import psycopg2.extras
import datetime

class QueryManager:

    def __init__(self, db_conn, engine):
        self.db_conn = db_conn
        self.engine = engine
        self.dict_cur = self.db_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def getUser(self, *args, **kwargs):
        """Method to retrieve user entries from the SYSTEM.user_accounts table.
        Fields:
            userid        (int),
            fname         (str),
            lname         (str),
            email         (str),
            passwd        (str),
            admin         (bool),
            active        (bool),
            register_date (datetime.datetime)

        Returns a list of all tuples that conform to the specified requirements.
        The format will be a list of dicts that map field names to the tuple's values.
        datetime is a python datetime.datetime object.

        Example:

        getUser(userid=5, fname="Peter"); getUser(lname="Peter")

        DB REQ: only db_conn.
        """

        self.__check_specified_params('system.user_accounts', kwargs)

        query, values = self.__form_select_statement('system.user_accounts', kwargs)

        self.dict_cur.execute(query, values)
        results = self.dict_cur.fetchall()

        return [dict(result) for result in results]
    # ENDFUNCTION

    def getDataset(self, *args, **kwargs):
        """Method to retrieve dataset entries from the SYSTEM.datasets table.
        Fields:
            setid         (int),
            setname       (str),
            description   (str),
            creation_date (datetime.datetime)

        Returns a list of all tuples that conform to the specified requirements.
        The format will be a list of dicts that map field names to the tuple's values.
    
        getDataset(setid=5); getDataset(setname="abcdef")
        """

        self.__check_specified_params('system.datasets', kwargs)

        query, values = self.__form_select_statement('system.datasets', kwargs)

        self.dict_cur.execute(query, values)
        results = self.dict_cur.fetchall()

        return [dict(result) for result in results]
    # ENDFUNCTION

    def insertUser(self, *args, **kwargs):
        # fetch return value
        """Method to insert entries into the SYSTEM.user_accounts table.
        Fields:
            userid        (int),
            fname         (str),
            lname         (str),
            email         (str),
            passwd        (str),
            admin         (bool),
            active        (bool),
            register_date (datetime.datetime)

        Inserts a tuple with the specified files and the associated values.

        insertUser(fname="Peter", lname="Selie", email="peter@selie.com", passwd="abcdef123")"""

        self.__check_specified_params('system.user_accounts', kwargs)

        query, values = self.__form_insert_statement('system.user_accounts', kwargs)

        self.dict_cur.execute(query, values)
        self.db_conn.commit()

        # check if retval is not empty

        if 'returning' in kwargs and kwargs['returning'] is not None:
            result = self.dict_cur.fetchone()
            return result[kwargs['returning']]
    # ENDFUNCTION

    def insertDataset(self, *args, **kwargs):
        """Method to insert entries into the SYSTEM.datasets table.
        Fields:
            setid         (int),
            setname       (str),
            desciption    (str),
            creation_date (datetime.datetime)

        Inserts a tuple with the specified fields and the associated values.

        insertDataset(setname="Abc", description="Some dataset.")"""

        self.__check_specified_params('system.datasets', kwargs)

        query, values = self.__form_insert_statement('system.datasets', kwargs)

        self.dict_cur.execute(query, values)
        self.db_conn.commit()

        if 'returning' in kwargs and kwargs['returning'] is not None:
            result = self.dict_cur.fetchone()
            return result[kwargs['returning']]
    # ENDFUNCTION

    def deleteUser(self, *args, **kwargs):
        """Method to delete entries from the SYSTEM.user_accounts table.
        Fields:
            userid        (int),
            fname         (str),
            lname         (str),
            email         (str),
            passwd        (str),
            admin         (bool),
            active        (bool),
            register_date (datetime.datetime)

        Deletes tuples that conform to the specified requirements."""

        self.__check_specified_params('system.user_accounts', kwargs)

        query, values = self.__form_delete_statement('system.user_accounts', kwargs)

        self.dict_cur.execute(query, values)
        self.db_conn.commit()
    # ENDFUNCTION

    def deleteDataset(self, *args, **kwargs):
        """Method to delete entries from SYSTEM.datasets table.
        Fields:
            setid         (int),
            setname       (str),
            description   (str),
            creation_date (datetime.datetime)

        Deletes tuples that conform to the specified requirements."""

        self.__check_specified_params('system.datasets', kwargs)

        query, values = self.__form_delete_statement('system.datasets', kwargs)

        self.dict_cur.execute(query, values)
        self.db_conn.commit()
    # ENDFUNCTION

    def updateUser(self, reqs, sets):
        """Method to alter entries from the  SYSTEM.user_accounts table.
        Fields:
            userid        (int),
            fname         (str),
            lname         (str),
            email         (str),
            passwd        (str),
            admin         (bool),
            active        (bool),
            register_date (datetime.datetime)

        Modifies the specified fields of the tuples that conform to the specified requirements.
        The requirements can be specified as a dict in 'reqs'. These will be used in the WHERE clause.
        The setters/new values can be specified as a dict in 'sets'. These will be used in the SET clause.

        updateUser(reqs={'userid': 50, 'fname': 'Jan'}, 
                   sets={'lname': 'Met de pet'})               """

        if not (sets and reqs):
            raise RuntimeError("Specified requirements and setters are empty.")

        self.__check_specified_params('system.user_accounts', reqs)
        self.__check_specified_params('system.user_accounts', sets)

        # compile query
        query, values = self.__form_update_statement('system.user_accounts', reqs, sets)

        # execute query
        self.dict_cur.execute(query, values)
        self.db_conn.commit()
    # ENDFUNCTION

    def updateDataset(self, reqs, sets):
        """Method to alter entries from the  SYSTEM.datasets table.
        Fields:
            setid         (int),
            setname       (str),
            description   (str),
            creation_date (datetime.datetime)

        Modifies the specified fields of the tuples that conform to the specified requirements.
        The requirements can be specified as a dict in 'reqs'. These will be used in the WHERE clause.
        The setters/new values can be specified as a dict in 'sets'. These will be used in the SET clause.
        """

        if not (sets and reqs):
            raise RuntimeError("Specified requirements and setters are empty.")

        self.__check_specified_params('system.datasets', reqs)
        self.__check_specified_params('system.datasets', sets)

        # compile query
        query, values = self.__form_update_statement('system.datasets', reqs, sets)

        # execute query
        self.dict_cur.execute(query, values)
        self.db_conn.commit()
    # ENDFUNCTION

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

    ################################################## INTERNAL FUNCTIONS ##################################################

    def __check_specified_params(self, tablename, specified_fields):
        tablefields = self.__get_table_fields(tablename)

        # check if all specified fields are valid
        for param_name, param_value in specified_fields.items():
            if not param_name in tablefields:
                continue # irrelevant parameter

            field_type = tablefields[param_name]

            if not isinstance(param_value, field_type):
                raise RuntimeError("Invalid type for field '" + field_name + "': '" + str(type(param_value)) + "' specified, '" + str(field_type) + "' expected.")
        # ENDFOR

        # check specified return field
        if 'returning' in specified_fields and specified_fields['returning'] is not None:
            if not specified_fields['returning'] in tablefields:
                raise RuntimeError("Invalid field specified for query return value: '" + specified_fields['returning'] + "'")
    # ENDFUNCTION

    def __form_select_statement(self, tablename, specified_fields):

        query = "SELECT * FROM " + tablename + " WHERE "
        values = []

        tablefields = self.__get_table_fields(tablename)

        # process fields
        for field_name, field_type in tablefields.items():
            # check if field is specified.
            if field_name in specified_fields:
                field_value = specified_fields[field_name]

                if isinstance(field_value, datetime.datetime):
                    field_value = field_value.isoformat()

                query += "" + field_name + "= %s, "
                values.append(field_value)
        # ENDFOR

        if not specified_fields:
            query += "TRUE;"
        else:
            query = query[:-2] + ";"

        return query, values
    # ENDFUNCTION

    def __form_insert_statement(self, tablename, specified_fields):
        
        value_pairs = []

        tablefields = self.__get_table_fields(tablename)

        # add parameter names
        for field_name, field_type in tablefields.items():
            if field_name in specified_fields:
                field_value = specified_fields[field_name]

                if isinstance(field_value, datetime.datetime):
                    field_value = field_value.isoformat()

                value_pairs.append((field_name, field_value))
        # ENDFOR

        query = "INSERT INTO " + tablename + "("

        for pair in value_pairs:
            query += pair[0] + ","
        # ENDFOR

        query = query[:-1]

        query += ") VALUES ("

        for pair in value_pairs:
            query += "%s,"
        # ENDFOR

        query = query[:-1] + ")"

        if 'returning' in specified_fields and specified_fields['returning'] is not None and specified_fields['returning'] in tablefields:
            query += " RETURNING " + specified_fields['returning']

        query += ";"
        values = [pair[1] for pair in value_pairs]

        return query, values
    # ENDFUNCTION

    def __form_delete_statement(self, tablename, specified_fields):
        query = "DELETE FROM " + tablename + " WHERE "
        values = []

        tablefields = self.__get_table_fields(tablename)

        # process fields
        for field_name, field_type in tablefields.items():
            # check if field is specified.
            if field_name in specified_fields:
                field_value = specified_fields[field_name]

                if isinstance(field_value, datetime.datetime):
                    field_value = field_value.isoformat()

                query += "" + field_name + "= %s, "
                values.append(field_value)
        # ENDFOR

        if not specified_fields:
            query += "TRUE;"
        else:
            query = query[:-2] + ";"

        return query, values
    # ENDFUNCTION

    def __form_update_statement(self, tablename, reqs, sets):
        query = "UPDATE " + tablename + " "

        tablefields = self.__get_table_fields(tablename)

        values = []

        # iterate over setters
        query += "SET "
        for field_name, field_type in tablefields.items():
            if field_name in sets:
                field_value = sets[field_name]

                if isinstance(field_value, datetime.datetime):
                    field_value = field_value.isoformat()

                query += field_name + " = %s,"
                values.append(field_value)
        # ENDFOR
        query = query[:-1] + " "

        # iterate over requirements
        query += "WHERE "
        for field_name, field_type in tablefields.items():
            if field_name in reqs:
                field_value = reqs[field_name]

                if isinstance(field_value, datetime.datetime):
                    field_value = field_value.isoformat()

                query += field_name + " = %s,"
                values.append(field_value)
        # ENDFOR
        query = query[:-1] + ";"

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
                "register_date": datetime.datetime
            }
        elif tablename == 'system.datasets':
            return {
                "setid": int,
                "setname": str,
                "description": str,
                "creation_date": datetime.datetime
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
                "transformation_date": datetime.datetime
            }
        else: 
            raise RuntimeError("Invalid tablename specified: '" + tablename + "'")
    # ENDFUNCTION