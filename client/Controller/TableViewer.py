import math
import re
import os
import csv

import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import mpld3

from Model.SQLTypeHandler import SQLTypeHandler

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
        self.maxrows = self.__initialize_rowcount()
        
    def __initialize_rowcount(self):
        cur = self.db_connection.cursor()
        cur.execute(sql.SQL('SELECT COUNT(*) FROM {}.{}').format(sql.Identifier(self.schema),
                                                                 sql.Identifier(self.tablename)))
        rowcount = cur.fetchone()[0]
        return int(rowcount)
        
    def get_attributes(self):
        """Method that returns a list of all attributes of the table."""
        SQL_query = "SELECT * FROM \"%s\".\"%s\" LIMIT 1" % (self.schema, self.tablename)
        data_frame = pd.read_sql(SQL_query, self.engine)
        return data_frame.columns.values.tolist()

    def get_rowcount(self):
        """Simple method to get the number of rows the table viewed by TableViewer has."""
        return self.maxrows

    def render_json(self, offset, limit, order=False, ascending=True, on_column=""):
        SQL_query =  'SELECT * FROM "%s"."%s" LIMIT %s OFFSET %s' % (self.schema, self.tablename, limit, offset)
        data_frame = pd.read_sql(SQL_query, self.engine)
        json_string = data_frame.to_json(orient='values', date_format='iso')
        return json_string
        
    def get_columntype_dict(self):
        """Method that generates all the types of the table attributes."""
        cur = self.db_connection.cursor()
        type_list = []
        SQL_query = 'SELECT * from "%s"."%s" LIMIT 1' % (self.schema, self.tablename)
        df = pd.read_sql(SQL_query, self.engine)
        attr_list = df.columns.values.tolist()
        type_dict = dict()
        
        for elem in attr_list:
            query = ("SELECT data_type, character_maximum_length FROM information_schema.columns"
                     " WHERE table_schema = %s AND table_name =  %s AND column_name = %s LIMIT 1")
            cur.execute(sql.SQL(query), [self.schema, self.tablename, elem])
            row = cur.fetchone()
            sql_type = SQLTypeHandler().get_type_alias(row[0])
            if (SQLTypeHandler().is_string(row[0]) is True) and (row[1] is not None): #This has a length limit
                type_name = sql_type + '({})'.format(row[1])       
            else:
                type_name = sql_type

            type_dict[elem] = type_name

        return type_dict

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

        # group all containers smaller than 5%
        five_percent = sum(temp_sizes)/20
        other = 0

        for i in range(len(temp_labels)):
            if temp_sizes[i] < five_percent:
                other += temp_sizes[i]
            else:
                labels.append(temp_labels[i])
                sizes.append(temp_sizes[i])

        if other > 0:
            sizes.append(other)
            labels.append("< 5%")

        return (labels, sizes)

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
