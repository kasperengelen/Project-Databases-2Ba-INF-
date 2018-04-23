import psycopg2
import psycopg2.extras
from psycopg2 import sql






class DatasetHistoryManager:
    """Class that manages the transformation history of a dataset.

    Attributes:
        setid: The id of the dataset that the manager has to access.
        db_connection: psycopg2 database connection to execute SQL queries.
        engine: SQLalchemy engine to use pandas functionality.
        track: Boolean indicating if the history has to be tracked and written to the history table.
    """

    def __init__(self, setid, db_connection, engine, track=True):
        self.setid = setid
        self.db_connection = db_connection
        self.engine = engine
        self.track = track
        self.entry_count = None
        self.choice_dict = None
        


    def write_to_history(self, table_name, origin_table, attribute, parameters, transformation_type):
        """Method thar writes an entry to the dataset history table for a performed transformation.

        Parameters:
            table_name: Name of the table containing the results of the transformation.
            origin_table: Name of the table that was used for the transformation.
            attribute: Name of attribute that was use for the transformation.
            parameters: List of parameters used with the transformation
            transformation_type: Integer representing the transformation used.
        """

        if self.track is False:
            return None
        
        param_array = self.__python_list_to_postgres_array(parameters, transformation_type)
        cur = self.db_connection.cursor()
        query = 'INSERT INTO SYSTEM.DATASET_HISTORY VALUES (%s, %s, %s, %s, %s, %s)'
        cur.execute(sql.SQL(query), [self.setid, table_name, attribute, transformation_type, param_array, origin_table])
        self.db_connection.commit()

        #self.render_history_table(1, 100)
        {'', '', }


    def __python_list_to_postgres_array(self, py_list, transformation_type):
        """Method that represents a python list as a postgres array for inserting into a PostreSQL database."""
        param_array = ""
        nr_elements = len(py_list)
        
        if nr_elements == 0: #Return an empty postgres array string
            return "{}"

        if transformation_type == 15: #The predicate is already quoted and only element in the list
            param_array = "{" + py_list[0] + "}"
            return param_array
            
        param_array = "'{}'"
        if nr_elements > 1:
            for i in range(nr_elements-1):
                param_array += ", '{}'"

        print('########################################################################')
        print(py_list)
        print(param_array)
        param_array = param_array.format(*py_list)
        print(param_array)
        param_array = "{" + param_array  + "}"
        print(param_array)
        print('########################################################################')

        return param_array

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


    def __generate_choice_dict(self):
        """Generate the dictionary used to write away history table entries."""
        if self.choice_dict is not None:
            return
        
        choice_dict = {
            1  : self.__rowstring_generator1,
            2  : self.__rowstring_generator2,
            3  : self.__rowstring_generator3,
            4  : self.__rowstring_generator4,
            5  : self.__rowstring_generator5,
            6  : self.__rowstring_generator6,
            7  : self.__rowstring_generator7,
            8  : self.__rowstring_generator8,
            9  : self.__rowstring_generator9,
            10 : self.__rowstring_generator10,
            11 : self.__rowstring_generator11,
            12 : self.__rowstring_generator12,
            13 : self.__rowstring_generator13,
            14 : self.__rowstring_generator14,
            15 : self.__rowstring_generator15
            }
        
        self.choice_dict =  choice_dict

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
        #dict_cur = self.db_connection.dict_cursor()
        dict_cur = self.db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

        if show_all is False:
            query = ("SELECT * FROM system.dataset_history WHERE setid = %s AND (table_name = %s OR origin_table = %s)"
                     " LIMIT %s OFFSET %s")
            dict_cur.execute(sql.SQL(query), [self.setid, tablename, tablename, nr_rows, offset])
        else:
            query = "SELECT * FROM system.dataset_history WHERE setid = %s LIMIT %s OFFSET %s"
            dict_cur.execute(sql.SQL(query), [self.setid, nr_rows, offset])


        all_rows = dict_cur.fetchall()
        self.__generate_choice_dict()
        print("#######################################################################")
        print(all_rows)
        for elem in all_rows:
            a = int(elem['transformation_type'])
            print(self.choice_dict[a](elem))
        print("#######################################################################")

        all_rows = dict_cur.fetchall()
        html_table = '<table id="mytable" border = "1">\n'
        html_table += '<thead> + \n + <tr style="text-align: right;">'
        html_table += '<th>Transformation info</th>\n<th>Date</th>\n</tr>\n</thead>\n'
        html_table += '<tbody>\n'



    def __rowstring_generator1(self, dict_obj):
        param = dict_obj['parameters']
        rowstring = 'Converted attribute "{}" of table {} to type {}.'
        rowstring = rowstring.format(dict_obj['attribute'], dict_object['origin_table'], param[0])
        return rowstring

    def __rowstring_generator2(self, dict_obj):
        rowstring = 'Deleted attribute "{}" of table {}'.format(dict_obj['attribute'], dict_obj['origin_table'])

    def __rowstring_generator3(self, dict_obj):
        param = dict_obj['parameters']
        if param[0] == True:
            operand = 'larger'
        else:
            operand = 'smaller'

        rowstring = 'Deleted outliers {} than {} from attribute "{}" of table "{}".'
        rowstring = rowstring.format(operand, param[1], dict_obj['attribute'], ['origin_table'])
        return rowstring

    def __rowstring_generator4(self, dict_obj):
        param = dict_obj['parameters']
        rowstring = 'Discretized attribute "{}" of table "{}" using custom ranges ( {} ).'
        rowstring = rowstring.format(dict_obj['attribute'], dict_obj['attribute'], param[0])
        return rowstring

    def __rowstring_generator5(self, dict_obj):
        rowstring = 'Discretized attribute "{}" of table "{}" in equi-frequent intervals.'
        rowstring = rowstring.format(dict_obj['attribute'], dict_obj['attribute'])
        return rowstring

    def __rowstring_generator6(self, dict_obj):
        rowstring = 'Discretized attribute "{}" of table "{}" in equi-distant intervals.'
        rowstring = rowstring.format(dict_obj['attribute'], dict_obj['attribute'])
        return rowstring

    def __rowstring_generator7(self, dict_obj):
        param = dict_obj['parameters']
        rowstring = 'Extracted {} from attribute "{}" of table "{}".'
        rowstring = rowstring.format(param[0], dict_obj['attribute'], dict_obj['origin_table'])
        return rowstring   

    def __rowstring_generator8(self, dict_obj):
        rowstring = 'Performed find-and-replace on attribute "{}" of table "{}" looking for "{}", {} substring matches and replacing the {} with "{}".'
        param = dict_obj['parameters']
        replacement_style = 'whole string'
        if param[2] == 'True':
            option = 'not allowing'
        else:
            option = 'allowing'
            if param[3] == 'False':
                replacement_style = 'substring'

        rowstring = rowstring.format(dict_obj['attribute'], dict_obj['origin_table'], param[0], option, replacement_style, param[1])
        return rowstring

    def __rowstring_generator9(self, dict_obj):
        param = dict_obj['parameters']
        if param[2] == 'True':
            sens_option = 'case sensitive'
        else:
            sens_option = 'case insensitive'
        rowstring = 'Performed find-and-replace on attribute "{}" of table "{}" with the regular expression "{}" ({}) replacing the matches with "{}".'
        rowstring = rowstring.format(dict_obj['attribute'], dict_obj['origin_table'], param[0], sens_option, param[1])
        return rowstring

    def __rowstring_generator10(self, dict_obj):
        param = dict_obj['parameters']
        rowstring = 'Filled null values with provide value {} in attribute "{}" of table "{}".'
        rowstring = rowstring.format(param[0], dict_obj['attribute'], dict_obj['origin_table'])
        return rowstring

    def __rowstring_generator11(self, dict_obj):
        param = dict_obj['parameters']
        rowstring = 'Filled null values with the mean ({}) in attribute "{}" of table "{}".'
        rowstring = rowstring.format(param[0], dict_obj['attribute'], dict_obj['origin_table'])
        return rowstring

    def __rowstring_generator12(self, dict_obj):
        param = dict_obj['parameters']
        rowstring = 'Filled null values with the mean ({}) in attribute "{}" of table "{}".'
        rowstring = rowstring.format(param[0], dict_obj['attribute'], dict_obj['origin_table'])
        return rowstring

    def __rowstring_generator13(self, dict_obj):
        rowstring = 'Normalized attribute "{}" of table "{}" in range [0-1] using the Z-score.'
        rowstring = rowstring.format(dict_obj['attribute'], dict_obj['origin_table'])
        return rowstring

    def __rowstring_generator14(self, dict_obj):
        rowstring = 'Performed One-hot-encoding using attribute "{}" of table "{}".'
        rowstring = rowstring.format(dict_obj['attribute'], dict_obj['origin_table'])
        return rowstring

    def __rowstring_generator15(self, dict_obj):
        param = dict_obj['parameters']
        rowstring = 'Delete rows from table "{}" using the following predicate: {}.'
        rowstring = rowstring.format(dict_obj['origin_table'], param[0])
        return rowstring


if __name__ == '__main__':
    connection = psycopg2.connect("dbname='projectdb18' user='postgres' host='localhost' password='Sch00l2k17'")
    obj = DatasetHistoryManager(3, connection, None, True)
    obj.render_history_table(1, 100, True)



        
        
            
        
        
    
