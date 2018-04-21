import psycopg2
from psycopg2 import sql






class DatasetHistoryManager:
    """Class that manages the transformation history of a dataset.

    Attributes:
        setid: The id of the dataset that the manager has to access.
        db_connection: psycopg2 database connection to execute SQL queries
        engine: SQLalchemy engine to use pandas functionality
    """

    def __init__(self, setid, db_connection, engine):
        self.setid = setid
        self.db_connection = db_connection
        self.engine = engine
        self.entry_count = None


    def __python_list_to_postgres_array(self, py_list):
        """Method that represents a python list as a postgres array for inserting into a PostreSQL database."""
        param_array = "{"
        nr_elements = len(py_list)
        
        if nr_elements == 0: #Return an empty postgres array string
            return "{}"
        
        for i in range(nr_elements-1):
            param_array += "'%s', "
        if nr_elements > 1:
            param_array += "'%s'"

        param_array += "}"
        param_array % parameters
        return param_array
        


    def write_to_history(self, table_name, origin_table, attribute, parameters, transformation_type):
        """Method thar writes an entry to the dataset history table for a performed transformation.

        Parameters:
            table_name: Name of the table containing the results of the transformation.
            origin_table: Name of the table that was used for the transformation.
            attribute: Name of attribute that was use for the transformation.
            parameters: List of parameters used with the transformation
            transformation_type: Integer representing the transformation used.
        """
        param_array = self.__python_list_to_postgres_array(parameters)
        cur = self.db_connection.cursor()
        query = 'INSERT INTO SYSTEM.DATASET_HISTORY VALUES (%s, %s, %s, %s, %s, %s)'
        cur.execute(sql.SQL(query), [self.setid, table_name, attribute, transformation_type, param_array, origin_table])
        self.db_connection.commit()
        
        
        


    def get_page_indices(self, display_nr, page_nr=1):
        """Method that returns the relevant indices for the history table that's being viewed.

        Parameters:
            display_nr: Integer specifying how much rows of a table have to be shown per page.
            page_nr: Integer indicating which page we're viewing to extract the right rows.
        """
        if self.entry_count is None:
            #This method will set the maxrows
            raise RuntimeError("Method is_in_range() was not called prior to get_page_indices, causing this failed operation.")
    
        table_size = self.maxrows
        self.maxrows = table_size
        max_index = math.ceil(table_size / display_nr)
        #At this point the table is too large to just show all the indices, we have to minimize clutter
        if(max_index > 5):
            if page_nr > 4:
                indices = ['1', '...', ]
                start = page_nr - 1 #Get index before current
            else:
                indices = []
                start = 1

                

            end = start + 3 #Show 3 indices including current page
            if(start == 1):
                end += 1
                if(page_nr == 4): #At this point only index = 2 will be '...', we only want to skip 2 or more values.
                    end += 1

            elif(page_nr == (max_index - 3)): #At this point the last 4 indices should always be shown
               start = page_nr - 1
               end = max_index + 1
               
            
            elif (end >= max_index):
                start = max_index -3 #Keep last pages from being isolated
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

    def is_in_range(self, page_nr, nr_rows, show_all=True, table_name=""):
        """Method that returns True if a page index is in range and False if it's not in range.

        Parameters:
            page_nr: Integer indicating which page we're trying to view
            nr_rows: The number of rows that are being showed per page.
            show_all: Boolean indicating if all entries for the dataset should be shown.
                      False implies only the entries for a specific table should be shown.
            table_name: Name of the table that has to be shown.
        """
        cur = self.db_connection.cursor()
        if show_all is False:
            query = "SELECT COUNT(*) FROM system.dataset_history WHERE setid = %s AND (table_name = %s OR origin_table = %s)"
            cur.execute(sql.SQL(query), [self.setid, tablename, tablename])
        else:
            query = "SELECT COUNT(*) FROM system.dataset_history WHERE setid = %s"
            cur.execute(sql.SQL(query), [self.setid])
        

        self.entry_count = cur.fetchone()[0]
            
        if((page_nr - 1) * nr_rows >= self.entry_count):
            #In case it's an empty table, the first page should still be in range
            if (self.entry_count == 0) and (page_nr == 1):
                return True
            else:
                return False
        else:
            return True

    def __entry_to_table_row(self, dict_row):
        """Method that translates a row result of a query to an tr element of the html table."""
        


    def render_history_table(self, page_nr, nr_rows, show_all=True, table_name=""):
        """This method returns a html table representing the page of the history table.

        Parameters:
            page_nr: Integer indicating which page we're viewing.
            nr_rows: The number of rows that are being showed per page.
            show_all: Boolean indicating if all entries for the dataset should be shown.
                      False implies only the entries for a specific table should be shown.
            table_name: Name of the table that has to be shown.
        """
        offset = (page_nr - 1) * nr_rows
        dict_cur = self.db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

        if show_all is False:
            query = ("SELECT COUNT(*) FROM system.dataset_history WHERE setid = %s AND (table_name = %s OR origin_table = %s)"
                     " LIMIT %s OFFSET %s")
            dict_cur.execute(sql.SQL(query), [self.setid, tablename, tablename, nr_rows, offset])
        else:
            query = "SELECT COUNT(*) FROM system.dataset_history WHERE setid = %s LIMIT %s OFFSET %s"
            dict_cur.execute(sql.SQL(query), [self.setid, nr_rows, offset])

        all_rows = cur.fetchall()
        html_table = '<table border = "1">\n'
        html_table += '<thead> + \n + <tr style="text-align: right;">'
        html_table += '<th>Transformation info</th>\n<th>Date</th>\n</tr>\n</thead>\n'
        html_table += '<tbody>\n'
        
        
    
