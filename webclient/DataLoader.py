import zipfile
import os
import shutil
import psycopg2
import re
from psycopg2 import sql
from utils import get_db


class DataLoader:

    def __init__(self):
        # predefine attributes
        self.header = None
        self.setid = None
        # contains a list of all created tables
        self.tables = []
        # true if the first batch of files still has to be read
        self.first_batch = None
        self.new_dataset = None

        self.db_conn = get_db()

    def create_new(self, setname, description):
        self.first_batch = True
        self.new_dataset = True

        # insert dataset entry for the current dataset
        self.db_conn.cursor().execute("INSERT INTO SYSTEM.datasets (setname, description) VALUES ({}, {}) "
                                      "RETURNING setid;".format(sql.Identifier(setname), sql.Identifier(description)))

        # get the setid of the current set
        self.setid = self.db_conn.cursor().fetchone()[0]
        print(self.setid)

        # create new schema with name setid
        self.db_conn.cursor().execute("CREATE SCHEMA \"{}\";".format(sql.Identifier(self.setid)))
        self.db_conn.commit()

    def use_existing(self, setid):
        self.first_batch = False
        self.new_dataset = False

        found = None
        self.db_conn.cursor().execute("SELECT EXISTS (SELECT TRUE FROM SYSTEM.datasets WHERE setid = %s);".format(sql.Identifier(setid)))
        found = self.db_conn.cursor().fetchone()[0]

        if found:
            self.setid = setid

    def read_file(self, filename, header=False):
        """
        :param header: True if the first line in the csv file contains the column names
        """
        self.header = header

        try:
            if filename.endswith(".csv"):
                self.__csv(filename)

            elif filename.endswith(".zip"):
                self.__unzip(filename)

            elif filename.endswith(".dump") or filename.endswith(".sql"):
                # doesn't work yet
                self.__dump(filename)

            else:
                raise ValueError("file type not supported")

        except:
            # if no file was read successfully and no table exists in the dataset, delete the dataset
            self.end()
            raise

        # if at least 1 file was read successfully, first_batch is not true anymore
        if len(self.tables):
            self.first_batch = False

    def end(self):
        # if no file was read successfully and no table exists in the dataset, delete the dataset
        if self.first_batch and len(self.tables) == 0:
            self.cancel()
        else:
            if self.new_dataset:
                # create the backup schema
                self.db_conn.cursor().execute("CREATE SCHEMA original_{};".format(sql.Identifier(self.setid)))

            self.db_conn.cursor().execute("SET search_path TO original_{};".format(sql.Identifier(self.setid)))

            # copy all tables to the backup schema
            for table in self.tables:
                self.db_conn.cursor().execute("SELECT * INTO {} FROM \"{}\".{}".format(sql.Identifier(table),
                                                                                       sql.Identifier(self.setid),
                                                                                       sql.Identifier(table)))

            self.db_conn.commit()

    def __csv(self, filename):
        column_names = []
        # read first line for table info
        with open(filename) as csv:
            header = csv.readline()

        if self.header:
            column_names = [x.strip() for x in header.split(',')]
            # replace spaces with underscores
            for i in range(len(column_names)):
                column_names[i] = column_names[i].replace(" ", "_")
        else:
            for i in range(header.count(',') + 1):
                column_names.append("column" + str(i))

        # extract table name
        tablename = os.path.basename(filename.replace(".csv", ""))

        # raise error if the table name contains a period, this is to prevent sql injections
        if tablename.count('.'):
            raise ValueError("Table names are not allowed to contain periods.")

        # create table query
        query = "CREATE TABLE {} (".format(sql.Identifier(tablename))
        for column in column_names:
            query += "{} VARCHAR, ".format(sql.Identifier(column))
        query = query[:-2] + ");"

        # insert everything into the database
        self.db_conn.cursor().execute("SET search_path TO {};".format(sql.Identifier(self.setid)))
        self.db_conn.cursor().execute(query)
        csv = open(filename, 'r')
        self.db_conn.cursor().copy_from(csv, tablename, sep=',')
        # if the first line in the csv contained the names of the columns, that row has to be deleted from the table
        if self.header:
            self.db_conn.cursor().execute("DELETE FROM {} WHERE ctid IN (SELECT ctid FROM {} LIMIT 1);".format(
                sql.Identifier(tablename), sql.Identifier(tablename)))

        self.tables.append(tablename)
        self.db_conn.commit()

    def __dump(self, filename):
        # make a copy in case an error occurs
        temp_tables = self.tables

        """
        Set up so that if an error accurs in the file, everything is ignored and nothing is commited
        to the database
        """
        try:
            with open(filename, 'r') as dump:
                for command in dump.read().strip().split(';'):

                    if re.search("CREATE TABLE.*\(.*\)", command, re.DOTALL | re.IGNORECASE):
                        # check for periods in table name, this is to prevent SQL injections
                        if command.split()[2].count('.'):
                            raise ValueError("Table names are not allowed to contain periods.")

                        self.db_conn.cursor().execute("SET search_path TO {};".format(sql.Identifier(self.setid)))
                        self.db_conn.cursor().execute(command.replace("\n", ""))
                        self.db_conn.commit()

                        self.tables.append(command.split()[2])

                    elif re.search("INSERT INTO.*", command, re.DOTALL | re.IGNORECASE):
                        # check for periods in table name, this is to prevent SQL injections
                        if command.split()[2].count('.'):
                            raise ValueError("Table names are not allowed to contain periods.")

                        self.db_conn.cursor().execute("SET search_path TO {};".format(sql.Identifier(self.setid)))
                        self.db_conn.cursor().execute(command.replace("\n", ""))
                        self.db_conn.commit()
                self.db_conn.commit()
        except:
            # all tables that were created should be removed from self.tables
            self.tables = temp_tables
            raise



    def __unzip(self, filename):
        # unzip the file
        zip = zipfile.ZipFile(filename, 'r')
        # each dataset gets an unzip folder, so that no data can overlap
        unzip_folder = ".unzip_temp_" + str(self.setid)
        zip.extractall(unzip_folder)
        zip.close()

        # load all files that were extracted
        directory = os.fsencode(unzip_folder)
        for sub_file in os.listdir(directory):
            sub_filename = os.fsdecode(sub_file)
            # files other than csv's are ignored
            if sub_filename.endswith(".csv"):
                try:
                    self.__csv(sub_filename)
                except psycopg2.ProgrammingError:
                    print("Didn't read file " + sub_filename)

        # delete the temporary folder
        shutil.rmtree(unzip_folder)

    def cancel(self):
        self.db_conn.cursor().execute("DROP SCHEMA \"{}\" CASCADE;".format(sql.Identifier(self.setid)))
        self.db_conn.cursor().execute("DELETE FROM SYSTEM.datasets AS d WHERE d.setid = {};".format(sql.Identifier(self.setid)))
        self.db_conn.commit()


if __name__ == "__main__":
     test = DataLoader()
     test.create_new("demo", "dataset for the demo")
    # test.use_existing(12)
     test.read_file("records.csv", True)
    # # test.read_file("load_departments.dump", True)
    # dt = DataTransformer(3, True)
    # dt.change_column_name(12, "test_csv", "num2", "numero2")
    # dt.join_tables(12, "test_csv", "test_csv_x", ["numero2"], ["num2"], "combined")

     test.end()
