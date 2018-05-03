import pandas as pd
import matplotlib
from Model.SQLTypeHandler import SQLTypeHandler
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import math
import re
import os
import csv
import psycopg2
from psycopg2 import sql
import mpld3

class TableViewer:
    """Class that extracts table information for viewing purposes.

    Attributes:
        setid: The id of the dataset that the user wants to modify
        tablename: The name of the table we're extracting information from.
        engine: SQLalchemy engine to use pandas functionality
        db_connection: psycopg2 database connection to execute SQL queries
        is_original: Boolean indicating whether we're viewing the original data uploaded by the users.
    """

    def __init__(self, setid, tablename, engine, db_connection=None, is_original=False):
        self.engine = engine
        self.tablename = tablename
        self.db_connection = db_connection
        self.cur = self.db_connection.cursor()
        if is_original is True:
            self.schema = 'original_' + str(setid)
        else:
            self.schema = str(setid)
        self.maxrows = None

    def get_attributes(self):
        """Method that returns a list of all attributes of the table."""
        SQL_query = "SELECT * FROM \"%s\".\"%s\" LIMIT 1" % (self.schema, self.tablename)
        data_frame = pd.read_sql(SQL_query, self.engine)
        return data_frame.columns.values.tolist()
        
    
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
        max_index = math.ceil(table_size / display_nr)
        if max_index == 0:
            return [1]
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
                if(page_nr == 4): #At this point only index = 2 will be '...', we only want to skip 2 or more values.
                    end += 1

            elif(page_nr == (max_index - 3)): #At this point the last 4 indices should always be shown
               start = page_nr - 1
               end = max_index + 1
               
            
            elif (end >= max_index):
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
        count_query  = "SELECT COUNT(*) FROM \"%s\".\"%s\"" % (self.schema, self.tablename)
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


    def is_numerical(self, attr_type):
        """Method that returns whether a postgres attribute type is a numerical type."""
        
        numericals = ['integer', 'double precision', 'bigint', 'bigserial', 'real', 'smallint', 'smallserial', 'serial']
        if attr_type in numericals:
            return True
        else:
            return False

        


    def render_table(self, page_nr, nr_rows, show_types=False):
        """This method returns a html table representing the page of the SQL table.

        Parameters:
            page_nr: Integer indicating which page we're viewing.
            nr_rows: The number of rows that are being showed per page.
            show_types: Boolean indicating whether data types should be included in the column header.
        """
        offset = 0
        offset = (page_nr - 1) * nr_rows
        SQL_query = "SELECT * FROM \"%s\".\"%s\" LIMIT %s OFFSET %s" % (self.schema, self.tablename, nr_rows, offset)
        data_frame = pd.read_sql(SQL_query, self.engine)
        html_table = re.sub(' mytable', '" id="mytable', data_frame.to_html(None, None, None, True, False, na_rep = 'NULL', classes='mytable'))
        if show_types is False:
            return html_table
        attributes = self.get_attributes()
        for string in attributes: #Let's add the types to the tablenames
            self.cur.execute(sql.SQL("SELECT pg_typeof({}) FROM {}.{} LIMIT 1").format(sql.Identifier(string), sql.Identifier(self.schema),
                                                                                  sql.Identifier(self.tablename)))
            sqltype = self.__translate_system_type(self.cur.fetchone()[0])
            new_string = string + "<br>(" + sqltype + ")"
            html_table = html_table.replace(string, new_string, 1)
            
        return html_table


    

    def to_csv(self, foldername, delimiter=',', quotechar='"', null="NULL"):
        """Convert a table from the dataset to a CSV file. The csv file will be stored
        in the specified folder. The filename will be the tablename followed by '.csv'."""

        filename = os.path.join(foldername, self.tablename + ".csv")

        with open(filename, 'w', encoding="utf-8") as outfile:
            outcsv = csv.writer(outfile, delimiter=delimiter, quotechar=quotechar)

            self.cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = '{}' AND table_name = '{}'".format(self.schema, self.tablename))

            # write header
            outcsv.writerow([x[0] for x in self.cur.fetchall()])

            self.cur.execute(sql.SQL("SELECT * FROM {}.{}").format(sql.Identifier(self.schema), sql.Identifier(self.tablename)))
            rows = self.cur.fetchall()

            # replace NULL values with parameter 'null'
            for i in range(len(rows)):
                rows[i] = list(rows[i])
                for j in range(len(rows[i])):
                    if rows[i][j] is None: rows[i][j] = null

            # write rows
            outcsv.writerows(rows)

    def get_numerical_histogram(self, columnname, bar_nr=10):
        # first check if the attribute type is numerical
        self.cur.execute(sql.SQL("SELECT pg_typeof({}) FROM {}.{}").format(sql.Identifier(columnname),
                                                                                                     sql.Identifier(self.schema),
                                                                                                     sql.Identifier(self.tablename)))
        type = self.cur.fetchone()[0]
        if not SQLTypeHandler().is_numerical(type):
            return "N/A"

        sql_query = "SELECT \"{}\" FROM \"{}\".\"{}\"".format(columnname, self.schema, self.tablename)
        df = pd.read_sql(sql_query, self.engine)
        fig = plt.figure(figsize=(5.12, 3.84))
        plt.hist(df[columnname], bins=bar_nr, align='left', alpha=0.8, color='grey')
        html = mpld3.fig_to_html(fig)

        # close the figure to free memory
        plt.close(fig)

        return html

    def get_frequency_pie_chart(self, columnname):
        # get the frequency of every value
        self.cur.execute(sql.SQL("SELECT {}, COUNT(*) FROM {}.{} GROUP BY {} ORDER BY COUNT(*) DESC,"
                                      " {}").format(sql.Identifier(columnname), sql.Identifier(self.schema),
                                                            sql.Identifier(self.tablename),
                                                            sql.Identifier(columnname),
                                                            sql.Identifier(columnname)))
        data = self.cur.fetchall()

        # pre processed data
        temp_labels = [x[0] for x in data]
        temp_sizes = [x[1] for x in data]
        labels = []
        sizes = []

        # group all containers smaller than 1%
        one_percent = sum(temp_sizes)/100
        other = 0

        for i in range(len(temp_labels)):
            if temp_sizes[i] < one_percent:
                other += temp_sizes[i]
            else:
                labels.append(temp_labels[i])
                sizes.append(temp_sizes[i])

        if other > 0:
            sizes.append(other)
            labels.append("< 1%")

        # taken from https://stackoverflow.com/questions/6170246/how-do-i-use-matplotlib-autopct
        def make_autopct(values):
            def my_autopct(pct):
                total = sum(sizes)
                val = int(round(pct * total / 100.0))
                return '{p:.1f}%  ({v:d})'.format(p=pct, v=val)

            return my_autopct

        fig, ax = plt.subplots()
        fig.set_size_inches(5.12, 3.84)
        ax.pie(sizes, labels=labels, autopct=make_autopct(sizes))
        ax.axis('equal')
        html = mpld3.fig_to_html(fig)
        # close the figure to free memory
        plt.close(fig)

        return html

    def get_most_frequent_value(self, columnname):
        """Return the value that appears most often in the column"""

        # get the frequency of every value and select the one that has the highest one
        self.cur.execute(sql.SQL("SELECT {}, COUNT(*) FROM {}.{} GROUP BY {} ORDER BY COUNT(*) DESC,"
                                      " {} LIMIT 1").format(sql.Identifier(columnname), sql.Identifier(self.schema),
                                                            sql.Identifier(self.tablename),
                                                            sql.Identifier(columnname),
                                                            sql.Identifier(columnname)))
        return self.cur.fetchone()[0]


    def get_null_frequency(self, columnname):
        """Return the amount of times NULL appears in the column"""

        self.cur.execute(sql.SQL("SELECT {}, COUNT(*) FROM {}.{} WHERE {} IS NULL GROUP BY {}"
                                      "").format(sql.Identifier(columnname), sql.Identifier(self.schema),
                                                            sql.Identifier(self.tablename),
                                                            sql.Identifier(columnname),
                                                            sql.Identifier(columnname)))
        frequency_tuple = self.cur.fetchone()
        if frequency_tuple is None:
            return 0
        else:
            return frequency_tuple[1]

    def __aggregate_function(self, columnname, aggregate):
        """Wrapper that returns result of aggregate function"""

        # first check if the attribute type is numerical
        self.cur.execute(
            sql.SQL("SELECT pg_typeof({}) FROM {}.{}").format(sql.Identifier(columnname),
                                                                     sql.Identifier(self.schema),
                                                                     sql.Identifier(self.tablename)))
        type = self.cur.fetchone()[0]
        if not SQLTypeHandler().is_numerical(type):
            return "N/A"

        self.cur.execute(sql.SQL("SELECT " + aggregate + "({}) FROM {}.{}").format(sql.Identifier(columnname),
                                                                          sql.Identifier(self.schema),
                                                                          sql.Identifier(self.tablename)))

        return self.cur.fetchone()[0]

    def get_max(self, columnname):
        """Return max value of the column"""
        return self.__aggregate_function(columnname, "MAX")

    def get_min(self, columnname):
        """Return min value of the column"""
        return self.__aggregate_function(columnname, "MIN")

    def get_avg(self, columnname):
        """Return average value of the column"""
        return self.__aggregate_function(columnname, "AVG")

