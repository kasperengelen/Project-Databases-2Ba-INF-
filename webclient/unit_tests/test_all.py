import unittest
import test_TableTransformer as t1
import test_TableTransformerCopy as t2
import test_TableViewer as t3


def create_suite():
    suite1 = unittest.TestLoader().loadTestsFromTestCase(t1.TestTableTransformer)
    suite2 = unittest.TestLoader().loadTestsFromTestCase(t2.TestTransformerCopy)
    suite3 = unittest.TestLoader().loadTestsFromTestCase(t3.TestTableViewer)
    #suite 4 = 
    suites = [suite1, suite2, suite3]
    test_suite = unittest.TestSuite(suites)
    return test_suite

if __name__ == '__main__':
    #Run all the unit tests for this project with one script.
    suite = create_suite()

    runner = unittest.TextTestRunner()
    runner.run(suite)
