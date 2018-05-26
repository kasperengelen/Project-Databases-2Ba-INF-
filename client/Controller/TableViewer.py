import math
import re
import os
import csv

import json
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql

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
        cur = self.db_connection.cursor()
        query_args = [sql.Identifier(self.schema), sql.Identifier(self.tablename)]
        cur.execute(sql.SQL('SELECT * from {}.{} LIMIT 1').format(*query_args))
        attributes =  [desc[0] for desc in cur.description]
        return attributes

    def get_rowcount(self):
        """Simple method to get the number of rows the table viewed by TableViewer has."""
        return self.maxrows

    def render_json(self, offset, limit, order=False, ascending=True, on_column=""):
        cur = self.db_connection.cursor()
        read_query = 'SELECT * FROM {}.{}'
        identifiers = [sql.Identifier(self.schema), sql.Identifier(self.tablename)]
        if order is True:
            ordering = 'ASC' if ascending else 'DESC'
            identifiers.append(sql.Identifier(on_column))
            read_query += (' ORDER BY {} ' + ordering)
        read_query += ' LIMIT %s OFFSET %s' % (limit, offset)
        cur.execute(sql.SQL(read_query).format(*identifiers))
        json_string = json.dumps(cur.fetchall(), default=str)
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

    def get_numerical_histogram(self, columnname, bins=10):
        # first check if the attribute type is numerical
        self.cur.execute(sql.SQL("SELECT pg_typeof({}) FROM {}.{}").format(sql.Identifier(columnname),
                                                                                                     sql.Identifier(self.schema),
                                                                                                     sql.Identifier(self.tablename)))
        type = self.cur.fetchone()[0]
        if not SQLTypeHandler().is_numerical(type):
            return [], [], False

        self.cur.execute(sql.SQL("SELECT {} FROM {}.{} ORDER BY {} ASC").format(sql.Identifier(columnname),
                                                                                sql.Identifier(self.schema),
                                                                                sql.Identifier(self.tablename),
                                                                                sql.Identifier(columnname)))
        values = [x[0] for x in self.cur.fetchall()]
        min_val = min(values)
        max_val = max(values)
        interval_size = (max_val - min_val) / bins
        distributed_values = list()
        current_bin = list()
        next_interval = min_val + interval_size
        intervals = [(min_val, next_interval)]
        for value in values:
            if value >= next_interval:
                next_interval += interval_size
                intervals.append((intervals[-1][1], next_interval))
                distributed_values.append(current_bin)
                current_bin = [value]
            else:
                current_bin.append(value)

        # if the last bin was not added
        if len(intervals) != len(distributed_values):
            distributed_values.append(current_bin)

        sizes = [len(x) for x in distributed_values]
        # stringify the tuples representing the intervals
        intervals = [str((math.ceil(x[0]), math.floor(x[1]))) for x in intervals]

        return intervals, sizes, True

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
            labels.append("other")

        return labels, sizes

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
