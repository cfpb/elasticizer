import unittest
from nose.tools import assert_equal
from nose_parameterized import parameterized
from elasticizer import __main__


class TestBackupType(unittest.TestCase):

    @parameterized.expand([
        ("int more than or equal to 1", 1, 1),
        ("str more than or equal to 1", "1", 1),
    ])
    def test_backup(self, _, bu_type, expected):
        assert_equal(__main__.backup_type(bu_type), expected)

    @parameterized.expand([
        ("int less than 1", 0),
        ("str less than 1", "0"),
    ])
    def test_backup_with_exception(self, _, bu_type):
        with self.assertRaises(Exception):
            __main__.backup_type(bu_type)
