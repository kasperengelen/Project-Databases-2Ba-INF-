import zipfile
import os
import shutil
import psycopg2
from db_wrapper import DBConnection


class DataLoader:

    def __init__(self):
        # predefine attributes
        self.header = None
        self.setid = None
        # contains a list of all created tables
        self.tables = []
        # true if the first batch of files still has to be read
        self.first_batch = None

    def create_new(self, setname, description):
        self.first_batch = True

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

    def use_existing(self, setid):
        self.first_batch = False

        found = None
        with DBConnection() as db_conn:
            db_conn.cursor().execute("SELECT EXISTS (SELECT TRUE FROM SYSTEM.datasets WHERE setid = %s);", [setid])
            found = db_conn.cursor().fetchone()[0]

        if found:
            self.setid = setid

    def read_file(self, filename, header=False):
        """
        :param header: True if the first line in the csv file contains the column names
        """
        self.header = header

        try:
            if filename.endswith(".csv"):
                try:
                    self.__csv(filename)
                except psycopg2.ProgrammingError:
                    print("Didn't read file " + filename)

            elif filename.endswith(".zip"):
                self.__unzip(filename)

            elif filename.endswith(".dump"):
                # doesn't work yet
                self.__cancel()

            else:
                raise ValueError("file type not supported")

        except:
            if self.first_batch: self.__cancel()
            raise

    def end(self):
        if len(self.tables) == 0:
            self.__cancel()

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
        query = "CREATE TABLE " + tablename + "("
        for column in column_names:
            query += column + " VARCHAR, "
        query = query[:-2] + ");"

        with DBConnection() as db_conn:
            db_conn.cursor().execute("SET search_path TO {};".format(self.setid))
            db_conn.cursor().execute(query)
            csv = open(filename, 'r')
            db_conn.cursor().copy_from(csv, tablename, sep=',')
            # if the first line in the csv contained the names of the columns, that row has to be deleted from the table
            if self.header:
                db_conn.cursor().execute("DELETE FROM " + tablename + " WHERE ctid "
                                    "IN (SELECT ctid FROM " + tablename + " LIMIT 1);")

            self.tables.append(tablename)
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
                try:
                    self.__csv(sub_filename)
                except psycopg2.ProgrammingError:
                    print("Didn't read file " + sub_filename)

        # delete the temporary folder
        shutil.rmtree(".unzip_temp")

    def __cancel(self):
        with DBConnection() as db_conn:
            db_conn.cursor().execute("DROP SCHEMA \"" + str(self.setid) + "\" CASCADE;")
            db_conn.cursor().execute("DELETE FROM SYSTEM.datasets AS d WHERE d.setid = %s;", [self.setid])
            db_conn.commit()

    def join_tables(self, table1, table2, columns):
        # not implemented yet
        return


if __name__ == "__main__":
    test = DataLoader()
    test.create_new("zip", "zippy")
    # test.use_existing(0)
    test.read_file("test_csv.csv", True)
    test.end()
