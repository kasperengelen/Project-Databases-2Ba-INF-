from unit_tests.test_TableTransformer import TestTableTransformer
from unit_tests.test_TableTransformerCopy import TestTransformerCopy
from unit_tests.test_TableViewer import TestTableViewer
from unit_tests.test_UserManager import TestUserManager
from unit_tests.test_DatasetManager import TestDatasetManager
from unit_tests.test_DatasetInfo import TestDatasetInfo
from unit_tests.ProjectTester import ProjectTester
from unit_tests.test_DataLoader import TestDataLoader

import unittest

def suite():
    suite = unittest.TestSuite()

    suite.addTest(TestUserManager('test_validateLogin'))

if __name__ == "__main__":
    #tests = [TestDatasetInfo, TestDataLoader, TestTableTransformer, TestTransformerCopy, TestTableViewer, TestUserManager, TestDatasetManager]
    tests = [TestTableTransformer, TestTransformerCopy, TestDataLoader]
    tester = ProjectTester(tests)
    tester.run()
