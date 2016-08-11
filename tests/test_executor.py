# coding=UTF-8

import unittest
import sys
import os
import logging
import logging.config
from utils.cmd import CommandResult
from utils.cmd import CommandExecutor

sys.path.append("..")
from hive.executor import HiveExecutor
from hive.exceptions import HiveUnfoundError
from hive.exceptions import HiveCommandExecuteError
import test_data

#CONF_LOG = "../conf/logging.conf"
#logging.config.fileConfig(CONF_LOG)

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
        rc = CommandExecutor.system("ls ")
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

    def test_parse_database(self):
        result = self.executor._parse_database(
            test_data.HIVE_DESC_DATABASE_TEST_DATA)

        self.assertEqual(result, test_data.HIVE_DESC_DATABASE_TEST_RESULT,
                         "the method HiveExecutor._parse_database() failed!")

    def test_show_partitions(self):
        self.assertRaises(HiveCommandExecuteError,
                          self.executor.show_partitions, "test_db_name", "test_table_name")

    def test_show_tables(self):
        self.assertRaises(HiveCommandExecuteError,
                          self.executor.show_tables, "test_db_name")

    def test_show_databases(self):
        if self.hive_enable:
            self.assertEqual([], self.executor.show_databases("notexists_db_name"))

    def test_drop_table(self):
        if self.hive_enable:
            self.assertEqual(True, self.executor.drop_table(
                "test_db_name", "test_table_name"))
        else:
            self.assertRaises(
                HiveCommandExecuteError, self.executor.drop_table, "test_db_name", "test_table_name")

    def test_build_partitions(self):
        result = self.executor._build_partitions(
            test_data.HIVE_BUILD_PARTITIONS_TEST_DATA)

        self.assertEqual(result, test_data.HIVE_BUILD_PARTITIONS_TEST_RESULT,
                         "the method HiveExecutor._build_partitions() failed!")

        self.assertEqual([], self.executor._build_partitions([]),
                         "the method HiveExecutor._build_partitions() failed!")

    def tearDown(self):
        self.executor = None


if __name__ == '__main__':
    unittest.main()
