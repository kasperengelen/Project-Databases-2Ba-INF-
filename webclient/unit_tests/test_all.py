import unittest
import ProjectTester as pt
import test_TableTransformer as t1
import test_TableTransformerCopy as t2
import test_TableViewer as t3



if __name__ == '__main__':
    #Small script to run all tests of the project using the ProjectTester class
    tests = [t1.TestTableTransformer, t2.TestTransformerCopy]
    tester = pt.ProjectTester(tests)
    tester.run()
