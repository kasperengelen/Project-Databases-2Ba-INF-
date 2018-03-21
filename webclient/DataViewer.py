import pandas as pd
from sqlalchemy import create_engine
import math

class DataViewer:

    def __init__(self):
        self.engine = self.engine = create_engine("postgresql://dbadmin:AdminPass123@localhost/projectdb18")
        self.maxrows = None

        
    #Given a setid this method returns a list of all the tables within this dataset
    def get_tablenames(self, setid):
        sql_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'" % str(setid)
        query_result = pd.read_sql(sql_query, self.engine)
        tablenames = query_result['table_name'].tolist()
        return tablenames


    def get_attributes(self, setid, tablename):
        SQL_query = "SELECT * FROM \"%s\".\"%s\" LIMIT 1" % (str(setid), tablename)
        data_frame = pd.read_sql(SQL_query, self.engine)
        return data_frame.columns.values.tolist()
        
    
    #Given a a number of rows to display this functions returns a list of possible page indices.
    def get_page_indices(self, setid, tablename, display_nr, page_nr=1):
        count_query  = "SELECT COUNT(*) FROM \"%s\".\"%s\"" % (setid, tablename)
        query_result = pd.read_sql(count_query, self.engine)
        table_size = query_result.iat[0, 0]
        self.maxrows = table_size
        max_index = math.ceil(table_size / display_nr)
        #At this point the table is too large to just show all the indices, we have to minimize clutter
        if(max_index > 12):
            if page_nr > 2:
                indices = ['1', '...', ]
                start = page_nr
            else:
                indices = []
                start = 1

            end = start + 11 #Show 10 indices past start
            if (end > max_index + 1):
                end = max_index + 1 

        else:
            indices = []
            start = 1
            end = max_index + 1
            
        for i in range(start, end):
            indices.append(str(i))

        if(end < max_index):
            indices.append('...')
            indices.append(str(max_index))

        return indices


    #Method that calculates whether a index is in range of table with display of nr_rows.
    #Make sure get_page_indices is always called before this method for properly setting self.maxrows
    def is_in_range(self, setid, tablename, page_nr, nr_rows):
        if self.maxrows is None:
            count_query  = "SELECT COUNT(*) FROM \"%s\".\"%s\"" % (setid, tablename)
            query_result = pd.read_sql(count_query, self.engine)
            table_size = query_result.iat[0, 0]
            self.maxrows = table_size
            
        if((page_nr - 1) * nr_rows >= self.maxrows):
            return False
        else:
            return True

    #This method returns a html table representagtion given the setid, tablename and page number and how many rows per page
    def render_table(self, setid, tablename, page_nr, nr_rows):
        offset = 0
        offset = (page_nr - 1) * nr_rows
        SQL_query = "SELECT * FROM \"%s\".\"%s\" LIMIT %s OFFSET %s" % (str(setid), tablename, nr_rows, offset)
        data_frame = pd.read_sql(SQL_query, self.engine)
        html_table = data_frame.to_html(None, None, None, True, False)
        return html_table

if __name__ == '__main__':
    dv = DataViewer()
    #lol = dv.get_tablenames(1)
    lol = dv.get_attributes(1, 'clients3')
    print(lol)
    
