import sys

sys.path.append('./View/')
sys.path.append('./Model/')
sys.path.append('./Controller/')

#from unit_tests import test_TableTransformer
#from unit_tests import test_TableTransformerCopy
#from unit_tests import test_TableViewer
from unit_tests import test_UserManager

import unittest

if __name__ == "__main__":
    unittest.main()
