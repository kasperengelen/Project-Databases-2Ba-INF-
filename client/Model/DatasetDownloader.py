from psycopg2 import sql
import csv
import os
import shutil
from Model.QueryManager import QueryManager


class DatasetDownloader:
    """Class that reads tables from a schema and puts them into a file"""

    def __init__(self, setid, db_connection):
        self.db_connection = db_connection
        self.cur = self.db_connection.cursor()
        self.schema = str(setid)
        self.query_man = QueryManager(self.db_connection, None)

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

        return tablename + ".csv"

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
        return self.schema + ".zip"

    def get_table_dump(self, tablename, foldername, original=False):
        """Create a dump file with name (tablename).dump and puts it in 'foldername'"""

        schema = self.__get_schema(original)

        filename = os.path.join(foldername, tablename + ".dump")
        with open(filename, 'w') as dumpfile:
            self.__create_table(dumpfile, schema, tablename)
            self.__insert_values(dumpfile, schema, tablename)

        return tablename + ".dump"

    def get_dataset_dump(self, foldername, original=False):
        """Create a dump file with name (setid).dump and puts it in 'foldername'"""

        # fetch all tablenames
        table_names = self.query_man.get_table_names(self.schema)

        dumpfolder = foldername
        # only used if original == True
        og_schema = self.__get_schema(original)

        if original:
            og_table_names = self.query_man.get_table_names(og_schema)
            dumpfolder = os.path.join(foldername, "temp_folder")
            os.mkdir(dumpfolder)

        with open(os.path.join(dumpfolder, self.schema + ".dump"), 'w') as dumpfile:
            for table in table_names:
                self.__create_table(dumpfile, self.schema, table)

            for table in table_names:
                self.__insert_values(dumpfile, self.schema, table)

        if not original:
            return self.schema + ".dump"
        else:
            with open(os.path.join(dumpfolder, og_schema + ".dump"), 'w') as dumpfile:
                for table in og_table_names:
                    self.__create_table(dumpfile, og_schema, table)

                for table in og_table_names:
                    self.__insert_values(dumpfile, og_schema, table)

            # make a zip of the two dump files
            shutil.make_archive(os.path.join(foldername + "/" + self.schema), 'zip', dumpfolder)

            shutil.rmtree(dumpfolder)
            return self.schema + ".zip"

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

    def __create_table(self, dumpfile, schema, tablename):
        create_table_str = "CREATE TABLE \"{}\" (\n".format(tablename)
        type_dict = self.query_man.get_col_types(schema, tablename)
        for col in type_dict:
            create_table_str += "\"{}\" {},\n".format(col, type_dict[col])
        create_table_str = create_table_str[:-2] + "\n);\n\n"
        dumpfile.write(create_table_str)

    def __insert_values(self, dumpfile, schema, tablename):
        """write all insert statements for a table"""
        # fetch data
        self.cur.execute(sql.SQL("SELECT * FROM {}.{}").format(sql.Identifier(schema), sql.Identifier(tablename)))
        # insert row statements
        dumpfile.write("INSERT INTO \"{}\" VALUES\n".format(tablename))
        rows = str()
        for row in self.cur:
            # turn every attribute into a string and escape single quotes the postgres way
            row_str = tuple([str(x).replace("'", "''") for x in row])
            # replace double quotes to single quotes for postgres compatibility
            row_str = str(row_str).replace('"', "'")
            rows += str(row_str) + ",\n"
        # replace last comma with a semicolon
        rows = rows[:-2] + ";\n"
        dumpfile.write(rows)


    def __get_schema(self, original):
        return original * "original_" + self.schema
