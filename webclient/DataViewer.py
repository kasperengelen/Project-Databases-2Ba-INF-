import pandas as pd
from sqlalchemy import create_engine

class DataViewer:

    def __init__(self):
        self.engine = self.engine = create_engine("postgresql://dbadmin:AdminPass123@localhost/projectdb18")

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
    string = dv.render_table(4, 'tmp', 1, 20)
    print(string)
    
