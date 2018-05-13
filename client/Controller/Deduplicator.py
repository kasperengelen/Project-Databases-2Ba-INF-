import recordlinkage as rl
import pandas as pd
import numpy as np
import psycopg2
from Model.DatabaseConfiguration import DatabaseConfiguration
from recordlinkage.datasets import load_krebsregister

class Deduplicator:

    def __init__(self, setid, db_connection, engine):
        self.schema = str(setid)
        self.engine = engine
        self.db_connection = db_connection
        self.cur = self.db_connection.cursor()

    def test2(self):
        query = "SELECT * FROM \"{}\".\"records\";".format(self.schema)
        test = pd.read_sql(query, self.engine)
        indexer = rl.BlockIndex(on=["Country", "Item_Type", "Order_Priority"])
        pairs = indexer.index(test)
        print(len(pairs))

        compare = rl.Compare()
        compare.exact('Sales_Channel', 'Sales_Channel', label='Sales_Channel')
        features = compare.compute(pairs, test)
        print(features.sum(axis=1).value_counts().sort_index(ascending=False))
        matches = features[features.sum(axis=1) == 1]

        kmeans = rl.KMeansClassifier()
        result_kmeans = kmeans.learn(matches)
        cm_kmeans = rl.confusion_matrix(matches.index, result_kmeans, len(matches))
        print(rl.fscore(cm_kmeans))

        # krebs_data, krebs_match = load_krebsregister(missing_values=0)
        # print(krebs_data.head(10))
        # print(krebs_match)

    def test(self, exact_match, ignore):
        table_name = "dataset1"
        query = "SELECT * FROM \"{}\".\"{}\";".format(self.schema, table_name)
        og_df = pd.read_sql(query, self.engine)
        col_names = list(og_df)

        self.cur.execute("SELECT column_name, data_type FROM information_schema.columns "
                            "WHERE table_schema = '{}' AND table_name = '{}'".format(self.schema, table_name))
        types = self.cur.fetchall()
        types_dict = dict()
        for tuple in types:
            types_dict[tuple[0]] = tuple[1]

        for column in exact_match:
            col_names.remove(column)
        print(col_names)

        indexer = rl.SortedNeighbourhoodIndex(on=exact_match, window=3)
        pairs = indexer.index(og_df)

        compare = rl.Compare()
        for i in range(len(col_names)):
            column = col_names[i]
            if column in ignore:
                continue

            if np.issubdtype(og_df[column].dtype, np.number):
                mean = og_df[column].mean()
                std = og_df[column].std()
                print(column + ": " + str(mean) + ", " + str(std))
                compare.numeric(column, column, label=column, method="linear", origin=mean, offset=std, scale=std)
            elif types_dict[column] == "date":
                og_df[column] = pd.to_datetime(og_df[column])
                compare.date(column, column, label=column)
            else:
                compare.string(column, column, label=column, method="damerau_levenshtein")

            # if types_dict[column] == "date":
            #     og_df[column] = pd.to_datetime(og_df[column])
            #     compare.date(column, column, label=column)
            # else:
            #     compare.string(column, column, label=column, method="damerau_levenshtein")

        potential_pairs = compare.compute(pairs, og_df)

        kmeans = rl.KMeansClassifier()
        result_kmeans = kmeans.learn(potential_pairs)
        cm_kmeans = rl.confusion_matrix(potential_pairs.index, result_kmeans, len(potential_pairs))

        # ecm = rl.ECMClassifier()
        # result_ecm = ecm.learn((potential_pairs > 0.8).astype(int))
        # conf_ecm = rl.confusion_matrix(potential_pairs.index, result_ecm, len(potential_pairs))
        percentage_exact_matches = rl.fscore(cm_kmeans)
        print(percentage_exact_matches)

        similarity_df = pd.DataFrame(index=potential_pairs.index)
        similarity_df["mean"] = potential_pairs.mean(axis=1)
        # similarity_df = similarity_df.sort_values("mean", axis=0, ascending=False)
        exact_matches_amount = int(len(similarity_df) * percentage_exact_matches)
        similarity_df = similarity_df.nlargest(exact_matches_amount, "mean")

        certain_matches = potential_pairs.loc[similarity_df[similarity_df["mean"] > 0.875].index]
        uncertain_matches = potential_pairs.loc[similarity_df[similarity_df["mean"] <= 0.875].index]
        print(exact_matches_amount)
        print(len(certain_matches))
        print(len(uncertain_matches))

        certain_paired_rows = list()
        for pair in certain_matches.index:
            row1 = og_df.ix[pair[0]]
            row2 = og_df.ix[pair[1]]
            paired_table = pd.DataFrame([row1, row2])
            certain_paired_rows.append(paired_table)

        uncertain_paired_rows = list()
        for pair in uncertain_matches.index:
            row1 = og_df.ix[pair[0]]
            row2 = og_df.ix[pair[1]]
            paired_table = pd.DataFrame([row1, row2])
            uncertain_paired_rows.append(paired_table)

        # print(certain_paired_rows[309])
        # print(uncertain_paired_rows[0])
        # print(len(paired_rows))
        # print(similarity_df.ix[6, 150])
        # print(certain_paired_rows)


if __name__ == "__main__":
    DC = DatabaseConfiguration()
    dd = Deduplicator(37, DC.get_db(), DC.get_engine())
    dd.test(["given_name"], ["birth_date"])