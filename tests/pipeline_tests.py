import unittest
from nose.tools import assert_equal
from elasticizer.pipelines.extract_postgres import Extract, ValidMapping,\
    ValidSettings, ExternalLocalTarget, Format
from elasticizer import Load
from nose_parameterized import parameterized
import collections
from luigi import LocalTarget
# from luigi.test.postgre_test import MockPostgresCursor
from mock import mock_open, patch, Mock, MagicMock, mock  # , mock_calls
from collections import namedtuple
import datetime


class TestIndexNamer(unittest.TestCase):

    @parameterized.expand([
        ((1, 'name', collections.namedtuple('Indexes', 'v1, alias')),
         ('name', '')),
        ((2, 'name', collections.namedtuple('Indexes', 'v1, v2, alias')),
         ('name-v1', 'name-v2', 'name')),
        ((3, 'asdf', collections.namedtuple('Indexes', 'v1, v2, v3, alias')),
         ('asdf-v1', 'asdf-v2', 'asdf-v3', 'asdf'))
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
        assert_equal(extract.query, 'SELECT * FROM table LIMIT 10')


class TestFormat(unittest.TestCase):

    def fields_from_mapping_test(self):
        data = '''
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
            format = Format("mapping_file", "docs_file", "table",
                            sql_filter="WHERE X", marker_table=True)
            result = format._fields_from_mapping()
        m.assert_called_once_with('mapping_file', 'r')
        assert_equal([u'id_cfpb', u'name'], result)

    @parameterized.expand([
        ((['dinosaurs', 'bugs'], ['Stegasaurus', 'Cricket']),
            {'dinosaurs': 'Stegasaurus', 'bugs': 'Cricket'}),
        ((['dinosaurs', 'bugs', 'cats'], [101, 222, 333]),
            {'dinosaurs': 101, 'bugs': 222, 'cats': 333}),
        ((['dinosaurs_date', 'bugs'], [datetime.date(2002, 12, 4), 222]),
            {'dinosaurs_date': '2002-12-04', 'bugs': 222}),
    ])
    def projection_test(self, input, expected):
        row = input[0]
        fields = input[1]
        results = Format._projection(row, fields)
        assert_equal(results, expected)

    @parameterized.expand([
        (('etad', 111), 111),
        (('1date1', datetime.date(2002, 12, 4)), '2002-12-04')
    ])
    def format_values_test(self, input, expected):
        field = input[0]
        value = input[1]
        fvalue = Format._format_values(field, value)
        assert_equal(fvalue, expected)

    @patch('elasticizer.pipelines.extract_postgres.ValidMapping')
    @patch('elasticizer.pipelines.extract_postgres.Extract')
    def requires_test(self, mock_extract, mock_valid_mapping):
        mock_extract.__class__ = Extract
        mock_valid_mapping.__class__ = ValidMapping
        format = Format("mapping_file", "docs_file", "table",
                        sql_filter="WHERE X", marker_table=True)
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
        format = Format("mapping_file", "docs_file", "table",
                        sql_filter="WHERE X", marker_table=True)
        result = format.output()
        self.assertIsInstance(result, LocalTarget)
        # @TODO: Check mapping file is passed into LocalTarget

    @patch.object(Format, '_projection')
    @patch.object(Extract, 'output')
    @patch.object(Format, '_build_sql_query')
    @patch.object(Format, '_fields_from_mapping')
    def run_test(self, mock_field_map, mock_build_sql_query,
                 mock_ext_output, mock_projection):
        mock_field_map.return_value = [u'id_cfpb', u'name']
        mock_build_sql_query.return_value = 'SELECT blah blah'

        mock_cursor_attrs = {'execute.return_value': True}
        mock_cursor = MagicMock(**mock_cursor_attrs)
        mock_cursor_enter_return = MagicMock(
            return_value=["row1", "row2", "row3"], **mock_cursor_attrs)
        mock_cursor_enter_return.__iter__.return_value = [
            'row1', 'row2', 'row3']
        mock_cursor.__enter__ = Mock(return_value=mock_cursor_enter_return)
        mock_connect_attrs = {'cursor.return_value': mock_cursor}
        mock_connect = Mock(**mock_connect_attrs)
        mock_extractor_attrs = {'connect.return_value': mock_connect}
        mock_extractor = Mock(table="table", **mock_extractor_attrs)
        mock_ext_output.return_value = mock_extractor

        mock_projection.return_value = "1"
        format = Format("mapping_file", "docs_file", "table",
                        sql_filter="WHERE id_cfpb is not NULL", marker_table=True)
        format.run()

        mock_field_map.assert_called_once()
        assert_equal(mock_projection.call_count, 3)
        mock_projection.assert_has_calls([
            mock.call(mock_field_map.return_value, "row1"),
            mock.call(mock_field_map.return_value, "row2"),
            mock.call(mock_field_map.return_value, "row3")
        ])

        # @TODO: mock open and json dump


class TestValidSettings(unittest.TestCase):

    def requires_test(self):
        v_settings = ValidSettings("mapping_file")
        assert_equal(v_settings.requires(), [])

    def output_test(self):
        v_settings = ValidSettings("mapping_file")
        self.assertIsInstance(v_settings.output(), ExternalLocalTarget)
