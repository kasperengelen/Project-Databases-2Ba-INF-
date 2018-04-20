import unittest

class ProjectTester:
    """Class that tests all the units of the project by executing the unit tests of the project that are
        that provided to this class upon initialization.

        Attributes:
            test_list: This a python list of all the modules that contain unit tests that need to be executed.
    """

    def __init__(self, test_list):
        self.test_list = test_list
        self.project_runner = unittest.TextTestRunner()


    def create_suite(self):
        """Method that creates a test suite for the project that is going to be used by the ProjectTester class."""
        suites = []
        for module in self.test_list:
            cur_suite = unittest.TestLoader().loadTestsFromTestCase(module) #Load suite from module
            suites.append(cur_suite)

        project_suite = unittest.TestSuite(suites)
        return project_suite


    def run(self):
        """Method that makes the ProjectTester run all the tests it has been provided with."""
        project_suite = self.create_suite()
        self.project_runner.run(project_suite)
