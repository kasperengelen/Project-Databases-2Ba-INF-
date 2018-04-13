import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import math
import re
import os
import csv
from utils import get_db
import db_wrapper
from psycopg2 import sql

class TableViewer:
    """Class that extracts table information for viewing purposes.

    Attributes:
        setid: The id of the dataset that the user wants to modify
        tablename: The name of the table we're extracting information from.
        engine: SQLalchemy engine to use pandas functionality
        db_connection: psycopg2 database connection to execute SQL queries
    """

    def __init__(self, setid, tablename, engine, db_connection=None):
        self.engine = engine
        self.setid = setid
        self.tablename = tablename
        self.db_connection = get_db()
        self.maxrows = None



    def get_attributes(self):
        """Method that returns a list of all attributes of the table."""
        SQL_query = "SELECT * FROM \"%s\".\"%s\" LIMIT 1" % (str(self.setid), self.tablename)
        data_frame = pd.read_sql(SQL_query, self.engine)
        return data_frame.columns.values.tolist()
        
    
    #Given a a number of rows to display this functions returns a list of possible page indices.
    def get_page_indices(self, display_nr, page_nr=1):
        """Method that returns the relevant indices for a table that's being viewed.

        Parameters:
            display_nr: Integer specifying how much rows of a table have to be shown per page.
            page_nr: Integer indicating which page we're viewing to extract the right rows.
        """
        if self.maxrows is None:
            #This method will set the maxrows
            self.is_in_range(1, 1)
    
        table_size = self.maxrows
        self.maxrows = table_size
        max_index = math.ceil(table_size / display_nr)
        #At this point the table is too large to just show all the indices, we have to minimize clutter
        if(max_index > 5):
            if page_nr > 4:
                indices = ['1', '...', ]
                start = page_nr - 1 #Get index before current
            else:
                indices = []
                start = 1

            end = start + 3 #Show 3 indices including current page
            if(start == 1):
                end += 1
            
            if (end >= max_index):
                start = max_index -3 #Keep last pages from being isolated
                end = max_index + 1 

        else:
            indices = []
            start = 1
            end = max_index + 1
            
        for i in range(start, end):
            indices.append(str(i))

        if(end < max_index):
            indices.append('...')
            indices.append(str(max_index))

        return indices


    def is_in_range(self, page_nr, nr_rows):
        """Method that returns True if a page index is in range and False if it's not in range.

        Parameters:
            page_nr: Integer indicating which page we're trying to view
            nr_rows: The number of rows that are being showed per page. 
        """
        count_query  = "SELECT COUNT(*) FROM \"%s\".\"%s\"" % (self.setid, self.tablename)
        query_result = pd.read_sql(count_query, self.engine)
        table_size = query_result.iat[0, 0]
        self.maxrows = table_size
            
        if((page_nr - 1) * nr_rows >= self.maxrows):
            #In case it's an empty table, the first page should still be in range
            if (table_size == 0) and (page_nr == 1):
                return True
            else:
                return False
        else:
            return True

    def __translate_system_type(self, systype):
        """Method that translates a type known to postgres' system to its standard SQL name"""
        translations = {
            'character varying' : 'VARCHAR',
            'character'         : 'CHAR',
            'integer'           : 'INTEGER',
            'double precision'  : 'FLOAT',
            'date'              : 'DATE',
            'time without time zone' : 'TIME',
            'timestamp without time zone' : 'TIMESTAMP'
            }

        return translations.setdefault(systype, 'UNKNOWN TYPE')

        


    def render_table(self, page_nr, nr_rows, show_types=True):
        """This method returns a html table representing the page of the SQL table.

        Parameters:
            page_nr: Integer indicating which page we're viewing.
            nr_rows: The number of rows that are being showed per page.
            show_types: Boolean indicating whether data types should be included in the column header
        """
        offset = 0
        offset = (page_nr - 1) * nr_rows
        SQL_query = "SELECT * FROM \"%s\".\"%s\" LIMIT %s OFFSET %s" % (str(self.setid), self.tablename, nr_rows, offset)
        data_frame = pd.read_sql(SQL_query, self.engine)
        html_table = re.sub(' mytable', '" id="mytable', data_frame.to_html(None, None, None, True, False, classes='mytable'))
        if show_types is False:
            return html_table
        attributes = self.get_attributes()
        for string in attributes: #Let's add the types to the tablenames
            cur = self.db_connection.cursor()
            cur.execute(sql.SQL("SELECT pg_typeof({}) FROM {}.{} LIMIT 1").format(sql.Identifier(string), sql.Identifier(str(self.setid)),
                                                                                  sql.Identifier(self.tablename)))
            sqltype = self.__translate_system_type(cur.fetchone()[0])
            new_string = string + "<br>(" + sqltype + ")"
            html_table = html_table.replace(string, new_string, 1)
        return html_table


    

    def to_csv(self, foldername, delimiter=',', quotechar='"', null="NULL"):
        """Convert a table from the dataset to a CSV file. The csv file will be stored
        in the specified folder. The filename will be the tablename followed by '.csv'."""

        filename = os.path.join(foldername, self.tablename + ".csv")

        with open(filename, 'w') as outfile:
            outcsv = csv.writer(outfile, delimiter=delimiter, quotechar=quotechar)
            conn = get_db()
            # conn = db_wrapper.DBWrapper()

            conn.cursor().execute("SELECT column_name FROM information_schema.columns WHERE table_schema = '{}' AND table_name = '{}'".format(self.setid, self.tablename))

            # write header
            outcsv.writerow([x[0] for x in conn.cursor().fetchall()])

            conn.cursor().execute(sql.SQL("SELECT * FROM {}.{}").format(sql.Identifier(str(self.setid)), sql.Identifier(self.tablename)))
            rows = conn.cursor().fetchall()

            # replace NULL values with parameter 'null'
            for i in range(len(rows)):
                rows[i] = list(rows[i])
                for j in range(len(rows[i])):
                    if rows[i][j] is None: rows[i][j] = null

            # write rows
            outcsv.writerows(rows)

    def show_histogram(self, columnname):
        pass

    def get_most_frequent_value(self, columnname):
        """Return the value that appears most often in the column"""
        conn = get_db() # = self.db_connection

        # get the frequence of every value and select the one that has the highest one
        conn.cursor().execute(sql.SQL("SELECT {}, COUNT(*) FROM {}.{} GROUP BY {} ORDER BY COUNT(*) DESC,"
                                      " {} LIMIT 1").format(sql.Identifier(columnname), sql.Identifier(str(self.setid)),
                                                            sql.Identifier(self.tablename),
                                                            sql.Identifier(columnname),
                                                            sql.Identifier(columnname)))
        return conn.cursor().fetchone()[0]


    def get_null_frequency(self, columnname):
        """Return the amount of times NULL appears in the column"""
        conn = get_db() # = self.db_connection

        conn.cursor().execute(sql.SQL("SELECT {}, COUNT(*) FROM {}.{} WHERE {} IS NULL GROUP BY {}"
                                      "").format(sql.Identifier(columnname), sql.Identifier(str(self.setid)),
                                                            sql.Identifier(self.tablename),
                                                            sql.Identifier(columnname),
                                                            sql.Identifier(columnname)))
        return conn.cursor().fetchone()[1]

    def __aggregate_function(self, columnname, aggregate):
        """Wrapper that returns result of aggregate function"""
        conn = get_db() # = self.db_connection

        conn.cursor().execute(sql.SQL("SELECT " + aggregate + "({}) FROM {}.{}").format(sql.Identifier(columnname),
                                                                          sql.Identifier(str(self.setid)),
                                                                          sql.Identifier(self.tablename)))
        return conn.cursor().fetchone()[0]

    def get_max(self, columnname):
        """Return max value of the column"""
        return self.__aggregate_function(columnname, "MAX")

    def get_min(self, columnname):
        """Return min value of the column"""
        return self.__aggregate_function(columnname, "MIN")

    def get_avg(self, columnname):
        """Return average value of the column"""
        return self.__aggregate_function(columnname, "AVG")

if __name__ == '__main__':
    tv = TableViewer(1, 'test', None)
    print(tv.get_avg("dept_name"))
    # print(tv.get_page_indices(50, 88))

