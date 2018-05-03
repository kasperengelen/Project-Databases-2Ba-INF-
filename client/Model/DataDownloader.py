from psycopg2 import sql
import csv
import os
import zipfile
import shutil

class DataDownloader():
    """Class that reads tables from a schema and puts them into a file"""

    def __init__(self, setid, db_connection):
        self.db_connection = db_connection
        self.cur = self.db_connection.cursor()
        self.schema = str(setid)

    def get_csv(self, tablename, foldername, delimiter=',', quotechar='"', null="NULL"):
        """Convert a table from the dataset to a CSV file. The csv file will be stored
        in the specified folder. The filename will be the tablename followed by '.csv'."""

        filename = os.path.join(foldername, tablename + ".csv")

        with open(filename, 'w', encoding="utf-8") as outfile:
            outcsv = csv.writer(outfile, delimiter=delimiter, quotechar=quotechar)

            self.cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = '{}' AND table_name = '{}'".format(
                    self.schema, tablename))

            # write header
            outcsv.writerow([x[0] for x in self.cur.fetchall()])

            self.cur.execute(
                sql.SQL("SELECT * FROM {}.{}").format(sql.Identifier(self.schema), sql.Identifier(tablename)))
            rows = self.cur.fetchall()

            # replace NULL values with parameter 'null'
            for i in range(len(rows)):
                rows[i] = list(rows[i])
                for j in range(len(rows[i])):
                    if rows[i][j] is None: rows[i][j] = null

            # write rows
            outcsv.writerows(rows)

    def get_csv_zip(self, foldername, delimiter=',', quotechar='"', null="NULL"):
        # create temporary directory to put csv's in
        csv_folder = ".csv_temp_" + self.schema
        csv_folder_complete = os.path.dirname(os.path.abspath(__file__)) + "/" + csv_folder

        # fetch all tablenames
        self.cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s;",
                         [str(self.setid)])
        result = self.cur.fetchall()
        table_names = [t[0] for t in result]

        # create all csv's
        for table in table_names:
            self.get_csv(table, csv_folder_complete, delimiter, quotechar, null)

        # make a zip of all csv's
        zip_file = zipfile.ZipFile(foldername + "_" + self.schema + ".zip", 'w', zipfile.ZIP_DEFLATED)

        for root, dirs, files, in os.walk(csv_folder_complete):
            for file in files:
                zip_file.write(os.path.join(root, file))

        zip_file.close()
        shutil.rmtree(csv_folder_complete)