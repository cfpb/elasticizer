import unittest
from nose.tools import assert_equal
from elasticizer.pipelines.extract_postgres import Extract, ValidMapping, ValidSettings, ExternalLocalTarget, Format
from elasticizer import Load
from nose_parameterized import parameterized
import collections
from luigi import LocalTarget
# from luigi.test.postgre_test import MockPostgresCursor
from mock import mock_open, patch, Mock, MagicMock, mock#, mock_calls
from collections import namedtuple

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
        data ='''
        {
            "_all": {
                "type": "string",
                "analyzer": "med_search_analyzer",
                "search_analyzer": "med_search_analyzer"
            },
            "properties": {
                "id_cfpb": {
                    "type": "string",
                    "copy_to": "all_query"
                },
                "name": {
                    "type": "string",
                    "copy_to": "all_suggester"
                },
                "all_suggester": {
                    "type": "string",
                    "analyzer": "autocomplete",
                    "search_analyzer": "standard"
                },
                "all_query": {
                    "type": "string",
                    "analyzer": "autocomplete",
                    "search_analyzer": "standard"
                }
            }
        }
        '''
        with patch('__builtin__.open', mock_open(read_data=data), create=True) as m:            
            format = Format("mapping_file", "docs_file", "table")
            result = format._fields_from_mapping()
        m.assert_called_once_with('mapping_file', 'r')
        assert_equal([u'id_cfpb', u'name'], result)

    def projection_test(self):
        pass

    @patch('elasticizer.pipelines.extract_postgres.ValidMapping')
    @patch('elasticizer.pipelines.extract_postgres.Extract')
    def requires_test(self, mock_extract, mock_valid_mapping):
        mock_extract.__class__ = Extract
        mock_valid_mapping.__class__ = ValidMapping
        format = Format("mapping_file", "docs_file", "table")
        result = format.requires()
        assert mock_extract.called
        assert mock_valid_mapping.called
        assert_equal(len(result), 2)
        # self.assertIsInstance(result[0], Extract)
        # self.assertIsInstance(result[1], ValidMapping)
        # self.assertTrue(isinstance(result[0], Extract))
        # @TODO: Check the result[0] is extract instance
        # @TODO: Check the result[1] is valid mapping instance

    def output_test(self):
        format = Format("mapping_file", "docs_file", "table")
        result = format.output()
        self.assertIsInstance(result, LocalTarget)
        # @TODO: Check mapping file is passed into LocalTarget

    @patch.object(Format, '_projection')
    @patch.object(Format, 'input')
    @patch.object(Format, '_fields_from_mapping')
    def run_test(self, mock_field_map, mock_input, mock_projection):
        mock_field_map.return_value = [u'id_cfpb', u'name']

        mock_cursor_attrs = {'execute.return_value': True}
        mock_cursor = MagicMock(**mock_cursor_attrs)
        mock_cursor_enter_return = MagicMock(return_value=["row1", "row2", "row3"], **mock_cursor_attrs)
        mock_cursor_enter_return.__iter__.return_value = ['row1', 'row2', 'row3']
        mock_cursor.__enter__ = Mock(return_value=mock_cursor_enter_return)
        mock_connect_attrs = {'cursor.return_value': mock_cursor}
        mock_connect = Mock(**mock_connect_attrs)
        mock_extractor_attrs = {'connect.return_value': mock_connect}
        mock_extractor = Mock(table="table", **mock_extractor_attrs)
        mock_input.return_value = [mock_extractor]

        mock_projection.return_value = "1"
        format = Format("mapping_file", "docs_file", "table")
        format.run()

        mock_field_map.assert_called_once()
        assert_equal(mock_projection.call_count, 3)
        mock_projection.assert_has_calls([mock.call("row1", mock_field_map.return_value), 
                                            mock.call("row2", mock_field_map.return_value),
                                            mock.call("row3", mock_field_map.return_value)])

        # @TODO: mock open and json dump

class TestValidSettings(unittest.TestCase):

    def requires_test(self):
        v_settings = ValidSettings("mapping_file")
        assert_equal(v_settings.requires(), [])

    def output_test(self):
        v_settings = ValidSettings("mapping_file")
        self.assertIsInstance(v_settings.output(), ExternalLocalTarget)




