import unittest

from Model.DatabaseConfiguration import TestConnection
from unit_tests.test_TableTransformer import TestTableTransformer
from unit_tests.test_TableTransformerCopy import TestTransformerCopy
from unit_tests.test_TableViewer import TestTableViewer
from unit_tests.test_UserManager import TestUserManager
from unit_tests.test_DatasetManager import TestDatasetManager
from unit_tests.test_DatasetPermissionsManager import TestDatasetPermissionsManager
from unit_tests.ProjectTester import ProjectTester
from unit_tests.test_TableLoader import TestTableLoader


if __name__ == "__main__":
    tests = [TestDatasetPermissionsManager, TestTableLoader, TestTableTransformer, TestTransformerCopy, TestTableViewer, TestUserManager, TestDatasetManager]
    tester = ProjectTester(tests)
    try:
        #Let's try to establish a connection to the testing database
        TestConnection()
    except:
        raise RuntimeError('Failed to connect to the testing database. Please check that the test_config.ini is correct.')
    tester.run()
