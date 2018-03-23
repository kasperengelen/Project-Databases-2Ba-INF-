import zipfile
import os
import shutil
import psycopg2
import re
from psycopg2 import sql
import db_wrapper
from TableTransformer import TableTransformer
# from utils import get_db

class DataLoader:

    def __init__(self, setid):
        self.db_conn = db_wrapper.DBWrapper()

        # first check if the setid is valid
        self.db_conn.cursor().execute("SELECT EXISTS (SELECT TRUE FROM SYSTEM.datasets WHERE setid = %s);", [setid])
        found = self.db_conn.cursor().fetchone()[0]
        # found = True

        if found:
            self.setid = setid
        else:
            raise ValueError("setid is not valid")

        # predefine attribute
        self.header = None

    def read_file(self, filename, header=False):
        """
        :param header: True if the first line in the csv file contains the column names
        """
        self.header = header

        """
        Each function that reads a file does not commit by itself, this is so that if an error were to occur,
        nothing is committed.
        If the commit is reached at the end of this function, all went well and the tables are committed.
        """

        if filename.endswith(".csv"):
            self.__csv(filename)

        elif filename.endswith(".zip"):
            self.__unzip(filename)

        elif filename.endswith(".dump") or filename.endswith(".sql"):
            self.__dump(filename)

        else:
            raise ValueError("file type not supported")

        # only commit when no errors occured
        self.db_conn.commit()

    def __csv(self, filename):
        # list of sql.Identifiers for the column names
        column_names = []

        # read first line for table info
        with open(filename) as csv:
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
                column_names.append(sql.Identifier("column" + str(i)))

        # extract table name
        tablename = os.path.basename(filename.replace(".csv", ""))

        # raise error if the table name is not alphanumeric, this is to not cause problems with url's
        if not tablename.isalnum():
            raise ValueError("Table names should be alphanumeric")

        # Create table query
        query = sql.SQL("CREATE TABLE {} (").format(sql.Identifier(tablename))
        query += sql.SQL(" VARCHAR, ").join(column_names)
        query += sql.SQL(" VARCHAR);")

        # insert everything into the database
        self.db_conn.cursor().execute("SET search_path TO {};".format(self.setid))
        self.db_conn.cursor().execute(query)
        csv = open(filename, 'r')
        self.db_conn.cursor().copy_from(csv, tablename, sep=',')
        # if the first line in the csv contained the names of the columns, that row has to be deleted from the table
        if self.header:
            self.db_conn.cursor().execute("DELETE FROM {} WHERE ctid IN (SELECT ctid FROM {} LIMIT 1);".format(
                tablename, tablename))

        # make backup
        self.__make_backup(tablename)

    def __dump(self, filename):
        with open(filename, 'r') as dump:
            for command in dump.read().strip().split(';'):

                if re.search("CREATE TABLE.*\(.*\)", command, re.DOTALL | re.IGNORECASE):
                    tablename = command.split()[2]
                    # raise error if the table name is not alphanumeric, this is to not cause problems with url's
                    if tablename.isalnum():
                        raise ValueError("Table names should be alphanumeric")

                    self.db_conn.cursor().execute("SET search_path TO {};".format(self.setid))
                    self.db_conn.cursor().execute(command.replace("\n", ""))

                    self.__make_backup(tablename)

                elif re.search("INSERT INTO.*", command, re.DOTALL | re.IGNORECASE):
                    tablename = command.split()[2]
                    # raise error if the table name is not alphanumeric, this is to not cause problems with url's
                    if tablename.isalnum():
                        raise ValueError("Table names should be alphanumeric")

                    self.__make_backup(tablename)
                    self.db_conn.cursor().execute("SET search_path TO {};".format(self.setid))
                    self.db_conn.cursor().execute(command.replace("\n", ""))

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

    def __make_backup(self, tablename):
        self.db_conn.cursor().execute("SET search_path TO original_{};".format(self.setid))

        # copy table into the backup schema
        self.db_conn.cursor().execute(sql.SQL("SELECT * INTO {} FROM {}.{}").format(sql.Identifier(tablename),
                                                                                    sql.Identifier(str(self.setid)),
                                                                                    sql.Identifier(tablename)))

    def create_dataset(self, setname, description):
        # insert dataset entry for the current dataset
        self.db_conn.cursor().execute("INSERT INTO SYSTEM.datasets (setname, description) VALUES (%s, %s) "
                                      "RETURNING setid;", [setname, description])

        # get the setid of the current set
        self.setid = self.db_conn.cursor().fetchone()[0]
        print(self.setid)

        # create new schema with name setid
        self.db_conn.cursor().execute(sql.SQL("CREATE SCHEMA {};").format(sql.Identifier(str(self.setid))))

        # create the backup schema
        self.db_conn.cursor().execute("CREATE SCHEMA original_{};".format(self.setid))

        self.db_conn.commit()

    def delete_dataset(self):
        schema = sql.Identifier(str(self.setid))
        backup_schema = sql.Identifier("original_" + str(self.setid))
        # delete schema
        self.db_conn.cursor().execute(sql.SQL("DROP SCHEMA {} CASCADE;").format(schema))
        # delete backup schema
        self.db_conn.cursor().execute(sql.SQL("DROP SCHEMA {} CASCADE;").format(backup_schema))
        # delete datasets table entry
        self.db_conn.cursor().execute("DELETE FROM SYSTEM.datasets AS d WHERE d.setid = %s;", [self.setid])
        self.db_conn.commit()


if __name__ == "__main__":

    test = DataLoader(32)
    # test.create_dataset("demo", "dataset for the demo")
    # test.read_file("records.csv", True)
    # test.read_file("load_departments.dump", True)
    # test.delete_dataset()

    tf = TableTransformer(3, 32, test.db_conn, 1)
    tf.change_column_name("departments", "dept_no", "OwO")
