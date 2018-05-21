import recordlinkage as rl
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql
from Model.DatabaseConfiguration import DatabaseConfiguration
from Model.QueryManager import QueryManager
from recordlinkage.datasets import load_krebsregister

class Deduplicator:

    class DataFrameWrapper:

        def __init__(self, dataframe):
            self.dataframe = dataframe

        def get_table_html(self):
            return self.dataframe.to_html()

        def get_entry_indices(self):
            return self.dataframe.index.values

    def __init__(self, setid, tablename, db_connection, engine):
        """Class to deduplicate a table"""
        self.schema = str(setid)
        self.tablename = tablename
        self.engine = engine
        self.db_connection = db_connection
        self.cur = self.db_connection.cursor()
        self.clusters = list()
        # entries that will be removed after submit() is called
        self.entries_to_remove = set()
        self.query_man = QueryManager(self.db_connection, None)

        query = "SELECT * FROM \"{}\".\"{}\";".format(self.schema, self.tablename)
        self.dataframe = pd.read_sql(query, self.engine)

    def find_matches(self, exact_match, ignore):
        """Function that finds entries that look alike
        :param exact_match: list of columns that need to have the exact same value
        :param ignore: list of columns that need to be ignored in the comparison
        :return a list of DataFrameWrappers that each represent a set of similar entries"""

        col_names = list(self.dataframe)
        types_dict = self.query_man.get_col_types(self.schema, self.tablename)

        indexer = rl.SortedNeighbourhoodIndex(on=exact_match, window=3)
        pairs = indexer.index(self.dataframe)

        num_cols = list()

        compare = rl.Compare()
        for i in range(len(col_names)):
            column = col_names[i]
            if column in ignore:
                continue

            if types_dict[column] == "date":
                self.dataframe[column] = pd.to_datetime(self.dataframe[column])
                compare.date(column, column, label=column)
                continue

            if np.issubdtype(self.dataframe[column].dtype, np.number):
                # convert numeric attributes to text so we can compare them
                self.dataframe[column] = self.dataframe[column].astype(str)
                num_cols.append(column)

            compare.string(column, column, label=column, method="damerau_levenshtein")

        potential_pairs = compare.compute(pairs, self.dataframe)
        # now that the comparison has happened, we can convert the numeric types back to their original
        for col in num_cols:
            self.dataframe[col] = pd.to_numeric(self.dataframe[col])

        kmeans = rl.KMeansClassifier()
        kmeans.learn(potential_pairs)
        results = kmeans.predict(potential_pairs)

        self.clusters = self.__cluster_pairs(results)
        certain_paired_rows = list()
        for cluster in self.clusters:
            rows = list()
            for row_id in cluster:
                rows.append(self.dataframe.ix[row_id])
            paired_table = self.DataFrameWrapper(pd.DataFrame(rows))
            certain_paired_rows.append(paired_table)

        return certain_paired_rows

    def deduplicate_cluster(self, cluster_id, entries_to_keep=None):
        """Removes duplicates in a cluster according to user_feedback
        :param cluster_id: index in self.clusters
        :param entries_to_keep: entries that should not be deduplicated"""

        cluster = self.clusters[cluster_id]

        # if no entries are specified to keep, only keep the first entry
        if entries_to_keep is None: entries_to_keep = [cluster[0]]

        # remove the entries that should not be deduplicated
        for entry in entries_to_keep:
            cluster.remove(entry)

        self.entries_to_remove.union(cluster)

    def submit(self):
        """Deletes all duplicates and alters the table in the database"""

        # delete all duplicates
        self.dataframe.drop([self.entries_to_remove])

        self.dataframe.to_sql(self.tablename, self.db_connection, if_exists="replace", index=False)

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


if __name__ == "__main__":
    DC = DatabaseConfiguration()
    dd = Deduplicator(37, "dataset1", DC.get_db(), DC.get_engine())
    result = dd.find_matches(["postcode"], ["rec_id"])
    print(len(result))