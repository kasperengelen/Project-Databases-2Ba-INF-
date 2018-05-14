
class QueryManager:

    def getUser(*args, **kwargs):
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

    def getDataset():
        pass

    def existsUser():
        pass

    def existsDataset():
        pass

    