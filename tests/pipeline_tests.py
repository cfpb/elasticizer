import unittest
from nose.tools import assert_equal
from elasticizer import Load
from nose_parameterized import parameterized
import collections

class TestIndexNamer(unittest.TestCase):

    @parameterized.expand([
    ( (1, 'name', collections.namedtuple('Indexes', 'v1, alias')), 
        ('name', '') ),
    ( (2, 'name', collections.namedtuple('Indexes', 'v1, v2, alias')), 
        ('name-v1', 'name-v2', 'name') ),
    ( (3, 'asdf', collections.namedtuple('Indexes', 'v1, v2, v3, alias')),
        ('asdf-v1', 'asdf-v2', 'asdf-v3', 'asdf') )
    ])
    def Indextuple_test(self, input, expected):
        n_versions = input[0]
        name = input[1]
        Indexes = input[2]
        expected = Indexes(*expected)
        index_names = Load.label_indices(n_versions, name)
        assert_equal(index_names, expected)

