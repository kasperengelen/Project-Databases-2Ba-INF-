from unit_tests import test_TableTransformer
from unit_tests import test_TableTransformerCopy
from unit_tests import test_TableViewer
from unit_tests import test_UserManager
from unit_tests import test_DatasetManager
from unit_tests import test_DatasetInfo

import unittest

def suite():
    suite = unittest.TestSuite()

    suite.addTest(TestUserManager('test_validateLogin'))

if __name__ == "__main__":
    unittest.main()
    #runner = unittest.TextTestRunner()
    #runner.run(suite())
