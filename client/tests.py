import unittest

from unit_tests.ProjectTester import ProjectTester
from Model.DatabaseConfiguration import TestConnection

from unit_tests.test_UserManager import TestUserManager
from unit_tests.test_DatasetManager import TestDatasetManager
from unit_tests.test_DatasetPermissionsManager import TestDatasetPermissionsManager
from unit_tests.test_QueryManager import TestQueryManager

from unit_tests.test_TableTransformer import TestTableTransformer
from unit_tests.test_TableTransformerCopy import TestTransformerCopy
from unit_tests.test_TableViewer import TestTableViewer

from unit_tests.test_TableUploader import TestTableUploader

if __name__ == "__main__":
    tests = []

    # comment uit wat je niet gebruikt
    tests.append(TestUserManager)
    tests.append(TestDatasetManager)
    tests.append(TestDatasetPermissionsManager)
    tests.append(TestQueryManager)
    #tests.append(TestTableTransformer)
    #tests.append(TestTransformerCopy)
    #tests.append(TestTableViewer)
    #tests.append(TestTableUploader)

    tester = ProjectTester(tests)
    
    try:
        #Let's try to establish a connection to the testing database
        TestConnection()
    except:
        raise RuntimeError('Failed to connect to the testing database. Please check that the test_config.ini is correct.')
    
    tester.run()
