from Model.DatabaseConfiguration import DatabaseConfiguration
from Controller.QueryExecutor import QueryExecutor
from Controller.TableViewer import TableViewer
from Controller.DatasetHistoryManager import DatasetHistoryManager
import psycopg2
import pandas as pd

db = DatabaseConfiguration().get_db()
eg = DatabaseConfiguration().get_engine()
qe = QueryExecutor(370, db, eg, True)
a = qe.execute_transaction('SELECT * from records LIMIT 1;')

print(a)


