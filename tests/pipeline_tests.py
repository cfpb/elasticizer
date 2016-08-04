import unittest
from nose.tools import assert_equal
from elasticizer.pipelines.extract_postgres import Extract
from elasticizer import Load, ValidSettings, ExternalLocalTarget, Format
from nose_parameterized import parameterized
import collections
from mock import mock_open, patch

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

class TestExternalLocalTarget(unittest.TestCase):
    def init_test(self):
        pass
    def remove_test(self):
        pass

class TestExtract(unittest.TestCase):
    def query_test(self):
        extract = Extract("table")
        assert_equal(extract.query, 'SELECT * FROM table')

class TestFormat(unittest.TestCase):

    def fields_from_mapping_test(self):
        with patch('__builtin__.open', mock_open(read_data='bibble'), create=True) as m:            
            format = Format("mapping_file", "docs_file", "table")
            format._fields_from_mapping()
        m.assert_called_once_with('mapping_file')

# >>> with patch('__main__.open', mock_open(read_data='bibble'), create=True) as m:
# ...     with open('foo') as h:
# ...         result = h.read()
# ...
# >>> m.assert_called_once_with('foo')
# >>> assert result == 'bibble'

class TestValidSettings(unittest.TestCase):

    def requires_test(self):
        v_settings = ValidSettings("mapping_file")
        assert_equal(v_settings.requires(), [])

    def output_test(self):
        v_settings = ValidSettings("mapping_file")
        self.assertIsInstance(v_settings.output(), ExternalLocalTarget)




