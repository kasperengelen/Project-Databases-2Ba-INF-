
class QueryManager:

    def __init__(self, db_conn, engine):
        self.db_conn = db_conn

    def getUser(self, *args, **kwargs):
        """Method to retrieve user entries from the SYSTEM.user_accounts table.
        Fields:
            userid (int),
            fname  (str),
            lname  (str),
            email  (str),
            passwd (str),
            admin  (bool),
            active (bool),
            register_data (list[d (int), m (int), y (int)])
        """
        # ex: getUser(userid=5,fname="Peter"), getUser(lname="Selie"), etc
        pass

    def getDataset(self):
        pass

    def existsUser(self):
        pass

    def existsDataset(self):
        pass

    