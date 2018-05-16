import zipfile
import os
import shutil
import psycopg2
import re
import pandas as pd
from psycopg2 import sql
from Model.DatabaseConfiguration import DatabaseConfiguration
from flask import abort

class FileException(Exception):
    def __init__(self, message):
        super().__init__(message)


class InvalidFileExtention(FileException):
    # exception thrown if the file is of an unsupported type
    def __init__(self):
        super().__init__("File type is unsupported")

class EmptyFileException(FileException):
    # exception thrown if file is empty
    def __init__(self):
        super().__init__("Given file is empty")


class ColumnInconsistencyException(FileException):
    # exception thrown if data does not match the amount of columns
    def __init__(self):
        super().__init__("Data does not match amount of columns")


class DumpInconsistencyException(FileException):
    # exception thrown if the dump/sql file is inconsistent (query-wise)
    def __init__(self):
        super().__init__("Queries in dump/sql file are inconsistent")


class EODException(FileException):
    # exception thrown if there were no CREATE TABLE queries
    def __init__(self):
        super().__init__("End of dump reached without create table statements")


class TableUploader:

    def __init__(self, setid, db_connection=None, engine=None):
        self.db_conn = db_connection
        self.engine = engine
        self.cur = self.db_conn.cursor()
        self.setid = setid

        # predefine attribute
        self.header = None


    def read_file(self, filename, header=True, automatic_type_conversion=False):
        """
        :param header: True if the first line in the csv file contains the column names
        :param automatic_type_conversion: True if csv file should be read with pandas, so that types are automatically converted.
        if it's False, it will be read with psycopg2's function copy_from() which is much faster than pandas' read_csv()
        """
        self.header = header

        """
        Each function that reads a file does not commit by itself, this is so that if an error were to occur,
        nothing is committed.
        If the commit is reached at the end of this function, all went well and the tables are committed.
        """

        # check if file is empty
        if os.stat(filename).st_size == 0:
            raise EmptyFileException

        if filename.endswith(".csv"):
            if automatic_type_conversion:
                self.__csv_pandas(filename)
            else:
                self.__csv_psycopg2(filename)

        elif filename.endswith(".zip"):
            self.__unzip(filename)

        elif filename.endswith(".dump") or filename.endswith(".sql"):
            self.__dump(filename)

        else:
            raise InvalidFileExtention

        # only commit when no errors occurred
        self.db_conn.commit()

    def __csv_pandas(self, filename):
        dataframe = pd.read_csv(filename)
        # convert column to datetime type
        for column in dataframe.columns:
            # only convert text to datetime
            if pd.api.types.is_string_dtype(dataframe[column]):
                dataframe[column] = pd.to_datetime(dataframe[column], errors="ignore")

        tablename = os.path.basename(filename.replace(".csv", ""))
        # raise error if the table name is not alphanumeric, this is to not cause problems with url's
        if not self.__check_alnum(tablename):
            raise ValueError("Table names should be alphanumeric")

        dataframe.to_sql(self.__get_valid_name(tablename), self.engine, index=False, schema=str(self.setid))

    def __csv_psycopg2(self, filename):
        # list of sql.Identifiers for the column names
        column_names = []

        # read first line for table info
        with open(filename, encoding="utf-8") as csv:
            header = csv.readline()

        # if the first line in the file contains the column names
        if self.header:
            column_names = [x.strip() for x in header.split(',')]
            # replace spaces with underscores
            for i in range(len(column_names)):
                # make all column names sql.Identifiers while we're at it
                column_names[i] = sql.Identifier(column_names[i].replace(" ", "_"))
        else:
            for i in range(header.count(',') + 1):
                column_names.append(sql.Identifier("column_" + str(i)))

        # extract table name
        tablename = os.path.basename(filename.replace(".csv", ""))

        # raise error if the table name is not alphanumeric, this is to not cause problems with url's
        if not self.__check_alnum(tablename):
            raise ValueError("Table names should be alphanumeric")

        # get a name that is not in use
        tablename = self.__get_valid_name(tablename)

        # Create table query
        query = sql.SQL("CREATE TABLE {} (").format(sql.Identifier(tablename))
        query += sql.SQL(" VARCHAR, ").join(column_names)
        query += sql.SQL(" VARCHAR);")

        # insert everything into the database
        self.cur.execute("SET search_path TO {};".format(self.setid))
        self.cur.execute(query)
        with open(filename, 'r', encoding="utf-8") as csv:
            try:
                # tablename has to be wrapped in quotes for this function to work
                self.cur.copy_from(csv, "\"" + tablename + "\"", sep=',')
            except psycopg2.DataError:
                raise ColumnInconsistencyException

        # if the first line in the csv contained the names of the columns, that row has to be deleted from the table
        if self.header:
            self.cur.execute(sql.SQL("DELETE FROM {} WHERE ctid IN (SELECT ctid FROM {} LIMIT 1);").format(
                sql.Identifier(tablename), sql.Identifier(tablename)))

        # make backup
        self.__make_backup(tablename)

    def __dump(self, filename):
        # keep track of tables created for backups
        table_names = []

        with open(filename, 'r', encoding="utf-8") as dump:
            for command in dump.read().strip().split(';'):
                if re.search("CREATE TABLE.*\(.*\)", command, re.DOTALL | re.IGNORECASE):
                    # extract tablename
                    tablename = command.split()[2]
                    # remove bracket in tablename if there is no whitespace in between
                    tablename = tablename.split("(", 1)[0]

                    # raise error if the table name is not alphanumeric, this is to not cause problems with url's
                    if not self.__check_alnum(tablename):
                        raise ValueError("Table names should be alphanumeric")

                    self.cur.execute("SET search_path TO {};".format(self.setid))
                    try:
                        self.cur.execute(command.replace("\n", ""))
                    except psycopg2.ProgrammingError:
                        raise DumpInconsistencyException

                    table_names.append(tablename)

                elif re.search("INSERT INTO.*", command, re.DOTALL | re.IGNORECASE):
                    tablename = command.split()[2]

                    self.cur.execute("SET search_path TO {};".format(self.setid))
                    try:
                        self.cur.execute(command.replace("\n", ""))
                    except psycopg2.ProgrammingError:
                        raise DumpInconsistencyException

        # if no tables were created, raise error
        if len(table_names) == 0:
            raise EODException
        else:
            for tablename in table_names:
                self.__make_backup(tablename)

    def __unzip(self, filename):
        # unzip the file
        zip = zipfile.ZipFile(filename, 'r')
        # each dataset gets an unzip folder, so that no data can overlap
        unzip_folder = ".unzip_temp_" + str(self.setid)
        unzip_folder_complete = os.path.dirname(os.path.abspath(__file__)) + "/" + unzip_folder
        zip.extractall(unzip_folder_complete)
        zip.close()

        # load all files that were extracted
        directory = os.fsencode(unzip_folder_complete)
        for sub_file in os.listdir(directory):
            sub_filename = unzip_folder_complete + "/" + os.fsdecode(sub_file)
            # files other than csv's are ignored
            if sub_filename.endswith(".csv"):
                try:
                    self.__csv_psycopg2(sub_filename)
                except psycopg2.ProgrammingError:
                    self.db_conn.rollback()

        # delete the temporary folder
        shutil.rmtree(unzip_folder_complete)

    def __make_backup(self, tablename):
        self.cur.execute("CREATE SCHEMA IF NOT EXISTS original_{}".format(self.setid))
        self.cur.execute("SET search_path TO original_{};".format(self.setid))

        # copy table into the backup schema
        self.cur.execute(sql.SQL("SELECT * INTO {} FROM {}.{}").format(sql.Identifier(tablename),
                                                                                    sql.Identifier(str(self.setid)),
                                                                                    sql.Identifier(tablename)))

    def __check_alnum(self, tablename):
        # isalnum() that also allows underscores
        temp_name = tablename.replace('_', 'a')
        return temp_name.isalnum()

    def __get_valid_name(self, tablename):
        # create a new tablename if the current one is already in use

        self.cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s;",
                                      [str(self.setid)])
        result = self.cur.fetchall()
        table_names = [t[0] for t in result]
        new_name = tablename

        # if there is at least one table in the dataset
        if len(table_names) != 0:
            name_available = False
            name_count = 0

            while not name_available:
                for i in range(len(table_names)):
                    # if every name has been checked
                    if i == len(table_names) - 1:
                        name_available = True
                    if new_name == table_names[i]:
                        name_available = False
                        break

                if not name_available:
                    name_count += 1
                    new_name = tablename + '_' + str(name_count)

        return new_name


if __name__ == "__main__":
    DC = DatabaseConfiguration()
    test = TableUploader(37, DC.get_db(), DC.get_engine())
    test.read_file("../type_test.csv", True, automatic_type_conversion=True)
    # test.read_file("load_departments.dump", True)
    # test.delete_dataset()
    pass

