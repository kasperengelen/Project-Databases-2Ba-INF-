import recordlinkage as rl
import pandas as pd
import numpy as np
import psycopg2
from Model.DatabaseConfiguration import DatabaseConfiguration
from recordlinkage.datasets import load_krebsregister

class Deduplicator:

    def __init__(self, setid, tablename, db_connection, engine):
        self.schema = str(setid)
        self.engine = engine
        self.db_connection = db_connection
        self.cur = self.db_connection.cursor()
        self.clusters = list()
        # entries that will be removed after submit() is called
        self.entries_to_remove = set()

        table_name = "dataset1"
        query = "SELECT * FROM \"{}\".\"{}\";".format(self.schema, table_name)
        self.dataframe = pd.read_sql(query, self.engine)

    def test(self, exact_match, ignore):
        col_names = list(self.dataframe)

        self.cur.execute("SELECT column_name, data_type FROM information_schema.columns "
                            "WHERE table_schema = '{}' AND table_name = '{}'".format(self.schema, table_name))
        types = self.cur.fetchall()
        types_dict = dict()
        for tuple in types:
            types_dict[tuple[0]] = tuple[1]

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
                self.dataframe[column] = self.dataframe[column].astype(str)
                num_cols.append(column)

            compare.string(column, column, label=column, method="damerau_levenshtein")

        potential_pairs = compare.compute(pairs, self.dataframe)

        kmeans = rl.KMeansClassifier()
        result_kmeans = kmeans.learn(potential_pairs)
        cm_kmeans = rl.confusion_matrix(potential_pairs.index, result_kmeans, len(potential_pairs))

        # ecm = rl.ECMClassifier()
        # result_ecm = ecm.learn((potential_pairs > 0.8).astype(int))
        # conf_ecm = rl.confusion_matrix(potential_pairs.index, result_ecm, len(potential_pairs))
        results = kmeans.predict(potential_pairs)

        self.clusters = self.__cluster_pairs(results)
        certain_paired_rows = list()
        for cluster in self.clusters:
            rows = list()
            for row_id in cluster:
                rows.append(self.dataframe.ix[row_id])
            paired_table = pd.DataFrame(rows)
            certain_paired_rows.append(paired_table)

        return certain_paired_rows

    def deduplicate_cluster(self, cluster_id, correct_entry=None, bad_entries=list()):
        """Removes duplicates in a cluster according to user_feedback
        :param cluster_id: index in self.clusters
        :param correct_entry: index of entry that should represent the cluster
        :param bad_entries: indices of entries that don't belong in the cluster"""

        cluster = self.clusters[cluster_id]

        if correct_entry is None:
            correct_entry = cluster[0]

        # remove the entries that should not be deduplicated
        for bad_entry in bad_entries:
            cluster.remove(bad_entry)

        # remove the one entry that should remain
        cluster.remove(correct_entry)

        self.entries_to_remove.union(cluster)


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
    dd = Deduplicator(37, DC.get_db(), DC.get_engine())
    result = dd.test(["postcode"], ["rec_id"])
    print(len(result))