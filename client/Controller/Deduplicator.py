import json
import time
import recordlinkage as rl
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql
from Model.DatabaseConfiguration import DatabaseConfiguration
from Model.QueryManager import QueryManager
from Controller.DatasetHistoryManager import DatasetHistoryManager


class Deduplicator:
    """Singleton class that servers to maintain data in between callbacks.
    For each table data is stored by using a dict with key (setid, tablename)

    Data that is not used can only exist for 1 hour, afterwards, it is deleted to clean up memory"""

    __instance = None

    class __InnerClass:

        def __init__(self, db_connection, engine):
            """Class to deduplicate a table"""
            self.engine = engine
            self.db_connection = db_connection
            self.cur = self.db_connection.cursor()
            self.clusters = list()
            # entries that will be removed after submit() is called
            self.entries_to_remove = set()

            self.dataframes = dict()
            self.clusters = dict()
            self.entries_to_remove = dict()
            self.age = dict()
            self.log = list()

        def find_matches(self, setid, tablename, exact_match=list(), ignore=list()):
            """Function that finds entries that look alike
            :param exact_match: list of columns that need to have the exact same value
            :param ignore: list of columns that need to be ignored in the comparison
            :return a list of json objects that each represent a set of similar entries"""

            try:
                self.log.append("find_matches :: ENTER")
                self.__lifetime_management(setid, tablename)
                # update time
                self.age[(setid, tablename)] = time.time()

                # load table into dataframe
                schema = str(setid)
                query = "SELECT * FROM \"{}\".\"{}\";".format(schema, tablename)
                dataframe = pd.read_sql(query, self.engine)
                self.dataframes[(setid, tablename)] = dataframe

                col_names = list(dataframe)
                types_dict = QueryManager(self.db_connection, None).get_col_types(schema, tablename)

                # pair similar rows together
                index_pairs = self.__index_table(dataframe, exact_match)

                # compare columns
                num_cols = list()
                compare = rl.Compare()
                for i in range(len(col_names)):
                    column = col_names[i]
                    if column in ignore:
                        continue

                    if types_dict[column] == "date":
                        dataframe[column] = dataframe[column].astype(str)
                        continue

                    if np.issubdtype(dataframe[column].dtype, np.number):
                        # convert numeric attributes to text so we can compare them
                        dataframe[column] = dataframe[column].astype(str)
                        num_cols.append(column)

                    compare.string(column, column, label=column, method="damerau_levenshtein")

                # compute similarity between pairs
                potential_pairs = compare.compute(index_pairs, dataframe)

                # now that the comparison has happened, we can convert the numeric types back to their original types
                for col in num_cols:
                    dataframe[col] = pd.to_numeric(dataframe[col])

                # if only one pair was found to match, predict can't be called because it requires at least 2 pairs
                if len(potential_pairs) < 2:
                    results = potential_pairs.index.values
                else:
                    # predict what matches are true matches
                    kmeans = rl.KMeansClassifier()
                    kmeans.learn(potential_pairs)
                    results = kmeans.predict(potential_pairs)

                # update time
                self.__check_own_lifetime(setid, tablename)

                # cluster together similar pairs
                self.clusters[(setid, tablename)] = self.__cluster_pairs(results)

                # create list of json objects
                certain_paired_rows = list()
                for cluster in self.clusters[(setid, tablename)]:
                    rows = list()
                    for row_id in cluster:
                        rows.append(dataframe.ix[row_id])
                    paired_table = pd.DataFrame(rows)
                    certain_paired_rows.append(json.loads(paired_table.to_json(orient='values', date_format='iso')))

                self.entries_to_remove[(setid, tablename)] = set()

                # update time
                self.__check_own_lifetime(setid, tablename)

                return certain_paired_rows

            except Exception:
                self.clean_data(setid, tablename)
                raise

        def deduplicate_cluster(self, setid, tablename, cluster_id, entries_to_keep=None):
            """Removes duplicates in a cluster according to user_feedback
            :param cluster_id: index in self.clusters
            :param entries_to_keep: entries that should not be deduplicated (deleted)"""

            try:
                self.log.append("deduplicate_cluster :: ENTER")
                # update time
                self.__check_own_lifetime(setid, tablename)

                cluster = list(self.clusters[(setid, tablename)][cluster_id])

                # if no entries are specified to keep, only keep the first entry
                if entries_to_keep is None: entries_to_keep = [0]

                # remove the entries that should not be deduplicated
                for entry in reversed(sorted(entries_to_keep)):
                    del cluster[entry]

                self.entries_to_remove[(setid, tablename)] = self.entries_to_remove[(setid, tablename)].union(set(cluster))

                # if the current cluster is the last one, submit the changes
                if cluster_id == len(self.clusters[(setid, tablename)]) - 1:
                    self.__submit(setid, tablename)

            except Exception:
                self.clean_data(setid, tablename)
                raise

        def yes_to_all(self, setid, tablename, cluster_id):
            """Deduplicate_cluster automatically starting from cluster_id to the last cluster"""
            self.log.append("yes_to_all :: ENTER")

            try:
                # update time
                self.__check_own_lifetime(setid, tablename)

                for i in range(cluster_id, len(self.clusters[(setid, tablename)])):
                    self.deduplicate_cluster(setid, tablename, i)

            except Exception:
                self.clean_data(setid, tablename)
                raise

        def clean_data(self, setid, tablename):
            """Clean all data in Deduplicator associated with the table"""
            self.dataframes.pop((setid, tablename), None)
            self.clusters.pop((setid, tablename), None)
            self.entries_to_remove.pop((setid, tablename), None)
            self.age.pop((setid, tablename), None)

        def __submit(self, setid, tablename):
            """Deletes all duplicates and alters the table in the database"""
            schema = str(setid)

            # create json of data to delete before the data is deleted
            row_json = self.__history_json(setid, tablename)

            # delete all duplicates
            self.dataframes[(setid, tablename)] = self.dataframes[(setid, tablename)].drop(self.entries_to_remove[(setid, tablename)])

            dataframe = self.dataframes[(setid, tablename)]
            dataframe.to_sql(tablename, self.engine, schema=schema, if_exists="replace", index=False)

            dataset_history_manager = DatasetHistoryManager(setid, self.db_connection)
            dataset_history_manager.write_to_history(tablename, tablename, "", [row_json], 18)

            self.clean_data(setid, tablename)

        def __index_table(self, dataframe, columns):
            if len(columns) == 0:
                indexer = rl.FullIndex()
            elif len(columns) == 1:
                indexer = rl.SortedNeighbourhoodIndex(on=columns, window=3)
            else:
                indexer = rl.BlockIndex(on=columns)

            return indexer.index(dataframe)

        def __cluster_pairs(self, pairs):
            """Group pairs together, for example:
            (A, B) and (B, C) wil be grouped together as (A, B, C)"""
            clusters = list()
            cluster_indices = dict()
            # clusters that are not used anymore
            abandoned_clusters = set()

            current_cluster_id = 0
            for result in pairs:
                id_1 = result[0]
                id_2 = result[1]
                cluster_id_1 = int()
                cluster_id_2 = int()

                # check if the two entries are already in a cluster
                has_cluster_1 = id_1 in cluster_indices
                has_cluster_2 = id_2 in cluster_indices

                # fetch cluster_id if it exists
                if has_cluster_1: cluster_id_1 = cluster_indices[id_1]
                if has_cluster_2: cluster_id_2 = cluster_indices[id_2]

                # if only one is in a cluster
                if has_cluster_1 and not has_cluster_2:
                    cluster_indices[id_2] = cluster_indices[id_1]
                    clusters[cluster_id_1].add(id_2)

                # if only one is in a cluster
                elif has_cluster_2 and not has_cluster_1:
                    cluster_indices[id_1] = cluster_indices[id_2]
                    clusters[cluster_id_2].add(id_1)

                # if both are not in a cluster, create one
                elif not has_cluster_1 and not has_cluster_2:
                    clusters.append(set(result))
                    cluster_indices[id_1] = current_cluster_id
                    cluster_indices[id_2] = current_cluster_id
                    current_cluster_id += 1

                # if they are both in different cluster, merge them
                elif cluster_id_1 != cluster_id_2:
                    clusters[cluster_id_1] = clusters[cluster_id_1].union(clusters[cluster_id_2])

                    # change the id from cluster 2 to the id from cluster 1
                    for id in clusters[cluster_id_2]:
                        cluster_indices[id] = cluster_id_1

                    abandoned_clusters.add(cluster_id_2)

            # delete clusters that are not used anymore
            for cluster_id in sorted(abandoned_clusters, reverse=True):
                del clusters[cluster_id]

            return clusters

        def __history_json(self, setid, tablename):
            """Creates a json of all the rows that have to be deleted.
            The json can be used to reproduce the deduplication (datasetHistory)"""
            dataframe = self.dataframes[(setid, tablename)]
            rows = list()
            for row in self.entries_to_remove[(setid, tablename)]:
                row_list = [str(x) for x in dataframe.iloc[row].tolist()]
                rows.append(row_list)

            json_string = json.dumps(rows)
            # escape quotes to allow insertion into postgres
            escaped_json = json_string.replace('"', '\\"')

            return escaped_json

        def redo_dedup(self, setid, tablename, row_json):
            """Delete all rows specified in the json"""
            # unescape quotes in the json string
            row_json = row_json.replace('\\"', '"')
            rows = json.loads(row_json)
            query = sql.SQL("DELETE FROM {}.{} WHERE ctid IN (SELECT ctid FROM {}.{} WHERE ").format(sql.Identifier(str(setid)),
                                                                                                     sql.Identifier(tablename),
                                                                                                     sql.Identifier(str(setid)),
                                                                                                     sql.Identifier(tablename))
            attributes = QueryManager(self.db_connection, None).get_col_names(str(setid), tablename)
            for row in rows:
                current_query = query
                for i in range(len(attributes) - 1):
                    attribute = attributes[i]
                    value = row[i]
                    current_query += sql.SQL("{} = %s AND ").format(sql.Identifier(attribute))
                attribute = attributes[-1]
                value = row[-1]
                current_query += sql.SQL("{} = %s LIMIT 1)").format(sql.Identifier(attribute))

                self.cur.execute(current_query, row)

            self.db_connection.commit()

        def __lifetime_management(self, setid, tablename):
            """delete data that is older than 1 hour"""
            self.log.append("lifetime_management :: ENTER")
            current_time = time.time()
            for key in self.age:
                if (current_time - self.age[key]) > 3600:
                    self.log.append("lifetime_management :: DELETE " + str(key) + " age = " + str(self.age[key]))
                    self.clean_data(key[0], key[1])

        def __check_own_lifetime(self, setid, tablename):
            """Check if the lifetime of (tablename) has expired, if not, update table age"""
            self.log.append("check_own_lifetime :: ENTER")
            if (setid, tablename) not in self.age:
                raise TimeoutError("Data-deduplication session expired, please try again" + str(self.log))

            # update time
            self.age[(setid, tablename)] = time.time()
            self.log.append("check_own_lifetime :: UPDATED AGE")


    def __init__(self, db_connection, engine):
        if Deduplicator.__instance is None:
            Deduplicator.__instance = self.__InnerClass(db_connection, engine)

    def __getattr__(self, name):
        return getattr(self.__instance, name)


if __name__ == "__main__":
    DC = DatabaseConfiguration()
    dd = Deduplicator(DC.get_db(), DC.get_engine())
    result = dd.find_matches(37, "dataset1", ["postcode"], ["rec_id"])
    dd.deduplicate_cluster(37, "dataset1", 0)
    print(result)