import pandas as pd
import zipfile
import os
import shutil
import datetime
from db_wrapper import DBConnection
from sqlalchemy import create_engine


class DataLoader(object):

    def __init__(self, filename, setname, description):
        self.engine = create_engine("postgresql://dbadmin:AdminPass123@localhost/projectdb18")
        self.filename = filename
        self.setname = setname
        self.description = description
        self.dataframes = []

        if self.filename.endswith(".csv"):
            self.__csv(self.filename)

        elif self.filename.endswith(".dump"):
            self.__dump(self.filename)

        elif self.filename.endswith(".zip"):
            self.__unzip(self.filename)

        else:
            raise ValueError("file type not suported")

    def __csv(self, filename):
        df = pd.read_csv(filename)

        # extract dataframe name
        tablename = filename.replace(".csv", "")
        # set the table name
        df.name = tablename

        self.dataframes.append(df)

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

    def join_tables(self, table1, table2, join_columns):
        # the first dataframe becomes the merged dataframe
        self.dataframes[table1] = pd.merge(self.dataframes[table1], self.dataframes[table2], on=join_columns)
        # the second dataframe is not needed anymore and is deleted
        del self.dataframes[table2]


    def to_database(self):

        timestamp = str(datetime.datetime.now())
        # insert a table entry for every dataframe
        with DBConnection() as db_conn:
            # get the highest setid and increment by 1 to get the new setid
            db_conn.cursor().execute("SELECT MAX(setid) FROM datasets")
            setid = db_conn.cursor().fetchone()[0]
            if setid is None:
                setid = 0
            else:
                setid += 1


            for i in range(len(self.dataframes)):
                # create the table name by using the setid and the table name
                tablename = str(setid) + "/" + self.dataframes[i].name
                db_conn.cursor().execute("INSERT INTO datasets (setid, setname, tablename, description, creation_data)"
                                        "VALUES (%s, %s, %s, %s, %s)", [setid, self.setname, tablename, self.description, timestamp])

                # insert the current dataframe into the database
                self.dataframes[i].to_sql(tablename, self.engine, if_exists="fail")


            db_conn.commit()


if __name__ == "__main__":
    test = DataLoader("test_csv.zip", "test", "a test dataset")
    test.to_database()