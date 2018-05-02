import sqlalchemy


class SQLTypeHandler:
    """Helper class following the singleton pattern that facilitates the
    interaction between SQL types and python types and provides type handling
    methods for the web application.
    """

    __instance = None

    class __InnerClass:

        def __init__(self):
            self.type_dict = self.initialize_type_dict()
            self.sqla_dict = self.initialize_sqla_dict()
    
        def initialize_type_dict(self):
            type_dict = {
                'character varying' : 'VARCHAR',
                'character'         : 'CHAR',
                'double precision'  : 'FLOAT',
                'date'              : 'DATE',
                'time without time zone' : 'TIME',
                'timestamp without time zone' : 'TIMESTAMP',
                'time with time zone' : 'TIMETZ',
                'timestamp with time zone' : 'TIMESTAMPTZ',
                'bit varying'       : 'VARBIT'
                }

            return type_dict

        def initialize_sqla_dict(self):
            sqla_dict = {
                'uint8'   : sqlalchemy.types.INTEGER(),
                'int64'   : sqlalchemy.types.INTEGER(),
                'float64' : sqlalchemy.types.Float(precision=25, asdecimal=True),
                'object'  : sqlalchemy.types.VARCHAR(length=255),
                'category': sqlalchemy.types.VARCHAR(length=255)
                }

            return sqla_dict
            

        def is_numerical(self, attr_type):
            """Method that returns whether a postgres attribute type is a numerical type."""
            
            numericals = ['integer', 'double precision', 'bigint', 'real', 'smallint']
            if attr_type in numericals:
                return True
            else:
                return False

        def get_type_alias(self, systype):
            """Method that translates a type known to postgres' system to its standard SQL alias if it has one."""
            try:
                # Try to find the alias in the dict using the EAFP principle.
                alias = self.type_dict[systype]

            except KeyError:
                # There is no alias for this type, so the type itself is correct.
                alias = systype.upper()

            return alias
                
        def pandas_to_postgres(self, pandas_type):
            """Method that maps a pandas-type object to a SQLAlchemy type object that represents a PostgreSQL type."""
            p_type = self.sqla_dict[pandas_type]
            return p_type

        def sql_time_to_dict(sql_date_string):
            """Given a string of the format "YYYY:MM:DD HH:MM:SS.SSSSSS" this
            returns a dict containing the same data under the keys 'Y', 'M', 'D', 'hr', 'min', 'sec', 'sec_full'.
            With 'sec' containing the seconds rounded to an integer, and 'sec_full' containing the full original value."""

            sql_date_string = str(sql_date_string)

            date = sql_date_string.split(' ')[0] # split on space between date and time
            time = sql_date_string.split(' ')[1]

            year = int(date.split('-')[0])
            month = int(date.split('-')[1])
            day = int(date.split('-')[2])

            hour = int(time.split(':')[0])
            minute = int(time.split(':')[1])
            sec_full = float(time.split(':')[2])
            sec = int(sec_full)

            return {
                "Y": year,
                "M": month,
                "D": day,

                "hr": hour,
                "min": minute,
                "sec": sec,
                "sec_full": sec_full
            }

    def __init__(self):
        if SQLTypeHandler.__instance is None:
            SQLTypeHandler.__instance = self.__InnerClass()

    def __getattr__(self, name):
        return getattr(self.__instance, name)


        
            

    
