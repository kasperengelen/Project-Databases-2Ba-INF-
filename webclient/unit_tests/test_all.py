import unittest
import test_TableTransformer as t1


def create_suite():
    suite1 = unittest.TestLoader().loadTestsFromTestCase(t1.TestTableTransformer)
    #suite2 = ....
    suites = [suite1]
    test_suite = unittest.TestSuite(suites)
    return test_suite

if __name__ == '__main__':
    #Run all the unit tests for this project with one script.
    suite = create_suite()

    runner = unittest.TextTestRunner()
    runner.run(suite)
