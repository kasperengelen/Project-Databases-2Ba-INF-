import pandas as pd
from sqlalchemy import create_engine
import math
import re

class TableViewer:

    def __init__(self, setid, tablename, engine):
        self.engine = engine
        self.setid = setid
        self.tablename = tablename
        #query_result = pd.read_sql(sql_query, self.engine)
        self.maxrows = None

        
    #Given a setid this method returns a list of all the tables within this dataset
    def get_tablenames(self):
        sql_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'" % str(self.setid)
        query_result = pd.read_sql(sql_query, self.engine)
        tablenames = query_result['table_name'].tolist()
        return tablenames


    def get_attributes(self):
        SQL_query = "SELECT * FROM \"%s\".\"%s\" LIMIT 1" % (str(self.setid), self.tablename)
        data_frame = pd.read_sql(SQL_query, self.engine)
        return data_frame.columns.values.tolist()
        
    
    #Given a a number of rows to display this functions returns a list of possible page indices.
    def get_page_indices(self, display_nr, page_nr=1):
        if self.maxrows is None:
            #This method will set the maxrows
            self.is_in_range(1, 1)
    
        table_size = self.maxrows
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
    def is_in_range(self, page_nr, nr_rows):
        count_query  = "SELECT COUNT(*) FROM \"%s\".\"%s\"" % (self.setid, self.tablename)
        query_result = pd.read_sql(count_query, self.engine)
        table_size = query_result.iat[0, 0]
        self.maxrows = table_size
            
        if((page_nr - 1) * nr_rows >= self.maxrows):
            return False
        else:
            return True

    #This method returns a html table representagtion given the setid, tablename and page number and how many rows per page
    def render_table(self, page_nr, nr_rows):
        offset = 0
        offset = (page_nr - 1) * nr_rows
        SQL_query = "SELECT * FROM \"%s\".\"%s\" LIMIT %s OFFSET %s" % (str(self.setid), self.tablename, nr_rows, offset)
        data_frame = pd.read_sql(SQL_query, self.engine)
        html_table = re.sub(' mytable', '" id="mytable', data_frame.to_html(None, None, None, True, False, classes='mytable'))
        return html_table

if __name__ == '__main__':
    
    pass
