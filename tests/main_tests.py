import unittest
from nose.tools import assert_equal
from elasticizer import __main__


class MyTest(unittest.TestCase):

    def backup0_test(self):
        with self.assertRaises(Exception):
            __main__.backup_type(0)
