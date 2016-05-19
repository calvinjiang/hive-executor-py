# coding=UTF-8

import unittest
import sys
import os
import logging
import logging.config
sys.path.append("..")
from hive.executor import HiveExecutor
from hive.executor import CommandResult
from hive.exceptions import HiveUnfoundError
from hive.exceptions import HiveCommandExecuteError
import test_data

#CONF_LOG = "../conf/logging.conf"
# logging.config.fileConfig(CONF_LOG)

logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')


class TestHiveExecutorMethods(unittest.TestCase):

    def setUp(self):
        status = os.system('which hive')

        if status == 0:
            self.hive_enable = True
        else:
            self.hive_enable = False

        self.assertRaises(ValueError, HiveExecutor, None)
        if not self.hive_enable:
            self.assertRaises(HiveUnfoundError, HiveExecutor, "hive")
            self.executor = HiveExecutor("ls")
        else:
            self.executor = HiveExecutor("hive")

    def test_execute_system_command(self):
        rc = self.executor._execute_system_command("ls ")
        self.assertTrue(isinstance(rc, CommandResult),
                        "return value type error,not a CommandResult instance.")

    def test_parse_hive_partition(self):
        result = self.executor._parse_partitions(
            test_data.HIVE_PARTITIONS_TEST_DATA)
        self.assertEqual(result, test_data.HIVE_PARTITIONS_TEST_DATA_PARSE_RESULT,
                         "the method HiveExecutor._parse_hive_partition failed!")
        result = self.executor._parse_partitions(None)
        self.assertEqual([], result)

    def test_parse_lines_to_sequence(self):
        result = self.executor._parse_lines_to_sequence(
            test_data.HIVE_TABLES_TEST_DATA)
        self.assertEqual(result, test_data.HIVE_TABLES_TEST_DATA_PARSE_RESULT,
                         "the method HiveExecutor._parse_hive_tables failed!")
        result = self.executor._parse_lines_to_sequence(None)
        self.assertEqual([], result)

    def test_parse_table(self):
        result = self.executor._parse_table(
            test_data.HIVE_DESC_TABLE_TEST_DATA)
        self.assertEqual(result, test_data.HIVE_DESC_TABLE_TEST_RESULT,
                         "the method HiveExecutor._parse_table() failed!")

    def test_show_partitions(self):
        self.assertRaises(HiveCommandExecuteError,
                          self.executor.show_partitions, "test_db_name", "test_table_name")

    def test_show_tables(self):
        self.assertRaises(HiveCommandExecuteError,
                          self.executor.show_tables, "test_db_name")

    def tearDown(self):
        self.executor = None


if __name__ == '__main__':
    unittest.main()
