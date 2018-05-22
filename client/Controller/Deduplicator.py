import json
import recordlinkage as rl
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql
from Model.DatabaseConfiguration import DatabaseConfiguration
from Model.QueryManager import QueryManager

class Deduplicator:
    """Singleton class that servers to maintain data in between callbacks.
    For each table data is stored by using a dict with key (setid, tablename)"""

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

        def find_matches(self, setid, tablename, exact_match=list(), ignore=list()):
            """Function that finds entries that look alike
            :param exact_match: list of columns that need to have the exact same value
            :param ignore: list of columns that need to be ignored in the comparison
            :return a list of json objects that each represent a set of similar entries"""

            try:
                # load table into dataframe
                schema = str(setid)
                query = "SELECT * FROM \"{}\".\"{}\";".format(schema, tablename)
                dataframe = pd.read_sql(query, self.engine)
                self.dataframes[(setid, tablename)] = dataframe

                col_names = list(dataframe)
                types_dict = QueryManager(self.db_connection, None).get_col_types(schema, tablename)

                # pair similar rows together
                index_pairs = self.__index_table(dataframe, exact_match)

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
                    print(results)
                else:
                    # predict what matches are true matches
                    kmeans = rl.KMeansClassifier()
                    kmeans.learn(potential_pairs)
                    results = kmeans.predict(potential_pairs)

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

                return certain_paired_rows

            except Exception:
                self.clean_data(setid, tablename)
                raise

        def deduplicate_cluster(self, setid, tablename, cluster_id, entries_to_keep=None):
            """Removes duplicates in a cluster according to user_feedback
            :param cluster_id: index in self.clusters
            :param entries_to_keep: entries that should not be deduplicated (deleted)"""

            try:
                cluster = list(self.clusters[(setid, tablename)][cluster_id])

                # if no entries are specified to keep, only keep the first entry
                if entries_to_keep is None: entries_to_keep = [cluster[0]]

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

            try:
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

        def __submit(self, setid, tablename):
            """Deletes all duplicates and alters the table in the database"""
            schema = str(setid)

            # delete all duplicates
            self.dataframes[(setid, tablename)] = self.dataframes[(setid, tablename)].drop(self.entries_to_remove[(setid, tablename)])

            dataframe = self.dataframes[(setid, tablename)]
            print(dataframe.head(50))
            dataframe.to_sql(tablename, self.engine, schema=schema, if_exists="replace", index=False)

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