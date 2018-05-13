from psycopg2 import sql
import csv
import os
import zipfile
import shutil
from Model.DatabaseConfiguration import DatabaseConfiguration

class TableDownloader():
    """Class that reads tables from a schema and puts them into a file"""

    def __init__(self, setid, db_connection):
        self.db_connection = db_connection
        self.cur = self.db_connection.cursor()
        self.schema = str(setid)

    def get_csv(self, tablename, foldername, delimiter=',', quotechar='"', null="NULL", original=False):
        """Convert a table from the dataset to a CSV file. The csv file will be stored
        in the specified folder. The filename will be the tablename followed by '.csv'."""

        filename = os.path.join(foldername, tablename + ".csv")

        with open(filename, 'w', encoding="utf-8") as outfile:
            outcsv = csv.writer(outfile, delimiter=delimiter, quotechar=quotechar)

            schema = self.__get_schema(original)

            self.cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = '{}' AND table_name = '{}'".format(
                    schema, tablename))

            # write header
            outcsv.writerow([x[0] for x in self.cur.fetchall()])

            self.cur.execute(
                sql.SQL("SELECT * FROM {}.{}").format(sql.Identifier(schema), sql.Identifier(tablename)))
            rows = self.cur.fetchall()

            # replace NULL values with parameter 'null'
            for i in range(len(rows)):
                rows[i] = list(rows[i])
                for j in range(len(rows[i])):
                    if rows[i][j] is None: rows[i][j] = null

            # write rows
            outcsv.writerows(rows)

    def get_csv_zip(self, foldername, delimiter=',', quotechar='"', null="NULL", original=False):
        """Converts all tables in a dataset to csv and puts them in a zip file called (setid).zip
        If original is true, the zip will contain folder 'original' and 'edited'"""
        # create temporary directory to put csv's in
        csv_temp_folder = os.path.join(foldername, "temp")
        os.mkdir(csv_temp_folder)

        if not original:
            self.__create_csvs(csv_temp_folder, delimiter, quotechar, null, original)
        else:
            # create the two subfolders
            edited_folder = os.path.join(csv_temp_folder, "edited")
            original_folder = os.path.join(csv_temp_folder, "original")
            os.mkdir(edited_folder)
            os.mkdir(original_folder)

            self.__create_csvs(edited_folder, delimiter, quotechar, null, False)
            self.__create_csvs(original_folder, delimiter, quotechar, null, True)

        # make a zip of all csv's
        shutil.make_archive(os.path.join(foldername + "/" + self.schema), 'zip', csv_temp_folder)

        shutil.rmtree(csv_temp_folder)

    def get_table_dump(self, tablename, foldername, original=False):
        """Create a dump file with name (tablename).dump and puts it in 'foldername'"""

        schema = self.__get_schema(original)

        filename = os.path.join(foldername, tablename + ".dump")
        with open(filename, 'w') as dumpfile:
            # create table statement
            dumpfile.write("CREATE TABLE {};\n".format(tablename))
            self.__insert_values(dumpfile, schema, tablename)

    def get_dataset_dump(self, foldername, original=False):
        """Create a dump file with name (setid).dump and puts it in 'foldername'"""

        schema = self.__get_schema(original)

        # fetch all tablenames
        self.cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s;",
                         [schema])
        result = self.cur.fetchall()
        table_names = [t[0] for t in result]

        filename = os.path.join(foldername, schema + ".dump")
        with open(filename, 'w') as dumpfile:
            # create table statements
            for table in table_names:
                dumpfile.write("CREATE TABLE {};\n".format(table))

            for table in table_names:
                self.__insert_values(dumpfile, schema, table)

    def __create_csvs(self, foldername, delimiter=',', quotechar='"', null="NULL", original=False):
        """Converts all tables in a dataset to csv and puts them in (foldername)"""
        schema = self.__get_schema(original)

        # fetch all tablenames
        self.cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s;",
                         [schema])
        result = self.cur.fetchall()
        table_names = [t[0] for t in result]

        # create all csv's
        for table in table_names:
            self.get_csv(table, foldername, delimiter, quotechar, null)

    def __insert_values(self, dumpfile, schema, tablename):
        """write all insert statements for a table"""
        # fetch data
        self.cur.execute(sql.SQL("SELECT * FROM {}.{}").format(sql.Identifier(schema), sql.Identifier(tablename)))
        # insert row statements
        for row in self.cur:
            dumpfile.write("INSERT INTO {} VALUES {};\n".format(tablename, str(row)))

    def __get_schema(self, original):
        return original * "original_" + self.schema



if __name__ == "__main__":
    DC = DatabaseConfiguration()
    DL = TableDownloader(37, DC.get_db())
    DL.get_dataset_dump("/home/atisha/mainrepo/client/test", original=True)