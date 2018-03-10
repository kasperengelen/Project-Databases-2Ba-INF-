import pandas as pd
import zipfile
import os
import shutil
import datetime
from db_wrapper import DBConnection

class DataLoader:

    def __init__(self, filename, setname, description, header=False, sep=','):
        """
        :param header: True if the first line in the csv file contains the column names
        :param sep: The separator character for csv files
        """
        self.header = header
        self.sep = sep

        with DBConnection() as db_conn:
            # insert dataset entry for the current dataset
            db_conn.cursor().execute("INSERT INTO SYSTEM.datasets (setname, description) VALUES (%s, %s) RETURNING setid;",
                                     [setname, description])

            # get the setid of the current set
            self.setid = db_conn.cursor().fetchone()[0]
            print(self.setid)

            # create new schema with name setid
            db_conn.cursor().execute("CREATE SCHEMA \"{}\";".format(self.setid))
            db_conn.commit()

        try:
            if filename.endswith(".csv"):
                self.__csv(filename)

            elif filename.endswith(".zip"):
                self.__unzip(filename)

            elif filename.endswith(".dump"):
                # doesn't work yet
                self.cancel()

            else:
                raise ValueError("file type not supported")

        except:
            self.cancel()
            raise

    def __csv(self, filename):
        column_names = []
        # read first line for table info
        with open(filename) as csv:
            header = csv.readline()

        if self.header:
            column_names = [x.strip() for x in header.split(self.sep)]
        else:
            for i in range(header.count(self.sep) + 1):
                column_names.append("column" + str(i))

        # extract dataframe name
        tablename = filename.replace(".csv", "")

        query = "CREATE TABLE " + tablename + "("
        for column in column_names:
            query += column + " VARCHAR, "
        query = query[:-2] + ");"

        with DBConnection() as db_conn:
            db_conn.cursor().execute("SET search_path TO {};".format(self.setid))
            db_conn.cursor().execute(query)
            csv = open(filename, 'r')
            db_conn.cursor().copy_from(csv, tablename, sep=self.sep)
            # if the first line in the csv contained the names of the columns, that row has to be deleted from the table
            if self.header:
                db_conn.cursor().execute("DELETE FROM " + tablename + " WHERE ctid "
                                    "IN (SELECT ctid FROM " + tablename + " LIMIT 1);")
            db_conn.commit()

    def __dump(self, filename):
        return

    def __unzip(self, filename):
        # unzip the file
        zip = zipfile.ZipFile(filename, 'r')
        zip.extractall(".unzip_temp")
        zip.close()

        # load all files that were extracted
        directory = os.fsencode(".unzip_temp")
        for sub_file in os.listdir(directory):
            sub_filename = os.fsdecode(sub_file)
            # files other than csv's are ignored
            if sub_filename.endswith(".csv"):
                self.__csv(sub_filename)

        # delete the temporary folder
        shutil.rmtree(".unzip_temp")

    def join_tables(self, table1, table2, columns):
        # not implemented yet
        return

    def cancel(self):
        with DBConnection() as db_conn:
            db_conn.cursor().execute("DROP SCHEMA \"" + str(self.setid) + "\" CASCADE;")
            db_conn.cursor().execute("DELETE FROM SYSTEM.datasets AS d WHERE d.setid = %s;", [self.setid])
            db_conn.commit()


if __name__ == "__main__":
    test = DataLoader("test_csv.zip", "zip", "zippy")
