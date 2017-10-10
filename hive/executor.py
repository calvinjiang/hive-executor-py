#####################################
# Author: Hua Jiang
# Date: 2016-04-23
# Desc:
# This module includes two classes below.
# An instance of the `CommandResult` class
# may be looked as a DTO(Data Transfer Object).Some of methods in the `HiveExecutor`
# class return result that is an instance of the `CommandResult` class.
# You can use straight the object of `HiveExecutor` class to operate hive.
######################################

import os
import sys
import subprocess
import re
import logging
from collections import OrderedDict
from utils.calendar import Calendar
from utils.cmd import CommandExecutor
from utils.cmd import CommandResult

from hive.exceptions import (HiveUnfoundError,
                             HiveCommandExecuteError)


_logger = logging.getLogger(__name__)


class HiveExecutor(object):
    """HiveExecutor may be looked as a wrapper of HiveCLI.

    You can create an object of HiveExecutor,then execute most of Hive commands
    by the class methods.When you instantiate the class,firstly check if hive
    client is available.So you can use it as a substitute for Hive Client.

    Attributes
    ----------
    hive_cmd_path : str
        The path of hive client.
    enable_verbose_mode : Optional[bool]
        If True, the class will use -v parameter when executing hive client.

    Parameters
    ----------
    hive_cmd_path : Optional[str]
        The path of hive client.
        Default is 'hive'.
    hive_init_settings : Optional[sequence]
        The settings of hive client.
        Default is [].An empty sequence.
    verbose : Optional[bool]
        Default is False.

    Raises
    ------
    ValueError
        If `hive_cmd_path` is None or an empty string.
    HiveUnfoundError
        If ``which`` command dosn't found `hive_cmd_path`.

    Examples
    --------
    >>> from hive import HiveExecutor
    >>> client=HiveExecutor("hive")
    >>> databases=client.show_databases()
    >>> print(databases)
    ['default', 'test']
    >>> databases=client.show_databases('defau*')
    >>> print(databases)
    ['default']

    >>> tables=client.show_tables('default')
    ['table1', 'table2']

    >>> init_settings=[]
    >>> init_settings.append("set mapred.job.queue.name=your_queue_name")
    >>> init_settings.append("set hive.exec.dynamic.partition.mode=nonstrict")
    >>> client=HiveExecutor(hive_cmd_path="hive",hive_init_settings=init_settings)

    """

    def __init__(self, hive_cmd_path="hive", hive_init_settings=[], verbose=False):
        if hive_cmd_path is None or len(hive_cmd_path) == 0:
            raise ValueError(
                "When you passed the argument of hive_cmd_path,it should have a value.")

        cmd = "which %s" % (hive_cmd_path)
        result = CommandExecutor.system(cmd)

        if result.status != 0:
            raise HiveUnfoundError(
                "the hive command path:%s is not exists." % (hive_cmd_path))

        self.hive_cmd_path = hive_cmd_path
        self.enable_verbose_mode = verbose
        self.hive_init_settings = hive_init_settings
        self.__default_hive_command = self.hive_cmd_path + " -S "

    def has_partitions(self, db_name, table_name, check_partitions):
        partitions = self.show_partitions(
            db_name, table_name, check_partitions)
        if len(partitions) > 0:
            return True
        else:
            return False

    def last_partitions(self, db_name, table_name):
        partitions = self.show_partitions(db_name, table_name)
        if partitions and len(partitions) > 0:
            return partitions[-1]
            # return partitions[-1:]
        else:
            return []

    def show_partitions(self, db_name, table_name, search_partitions=None):
        "substitute for `show partitions`"
        explicit_partition = ""
        if search_partitions:
            if isinstance(search_partitions, OrderedDict):
                temp = []
                for key, value in search_partitions.items():
                    if isinstance(value, basestring):
                        value = "'%s'" % (value)
                    temp.append("=".join([key, value]))
                if len(temp) > 0:
                    explicit_partition = " partition(%s) " % (",".join(temp))
            else:
                raise ValueError(
                    "The passed argument partition must be OrderedDict type.")

        hive_sql = "use %s;show partitions %s %s;" % (
            db_name, table_name, explicit_partition)

        _logger.debug("the function show_partitions() execute:%s" % (hive_sql))

        cr = self.execute(sql=hive_sql)

        if cr.status == 0:
            return self._parse_partitions(cr.stdout_text)
        else:
            _logger.error(cr.stderr_text)
            raise HiveCommandExecuteError(
                "the hive command:%s error! error info:%s" % (hive_sql, cr.stderr_text))

        return None

    def add_partitions(self, db_name, table_name, partitions):
        "substitute for `alter table db_name.table_name add if not exists partition(dt='',hour='');`"
        hive_sql = "alter table %s.%s add if not exists \n" % (
            db_name, table_name)

        built_partitions = self._build_partitions(partitions)

        if len(built_partitions) > 0:
            hive_sql = hive_sql + "\n".join(built_partitions) + ";"

            _logger.debug("executed hive sql:%s" % (hive_sql))
            cr = self.execute(sql=hive_sql)

            if cr.status == 0:
                return True
            else:
                _logger.error(cr.stderr_text)
                raise HiveCommandExecuteError(
                    "the hive command:%s error! error info:%s" % (hive_sql, cr.stderr_text))
            return False

    def drop_partitions(self, db_name, table_name, partitions):
        """
        substitute for `alter table db_name.table_name drop if exists
        partition(dt='',hour='');`
        """
        hive_sql = "alter table %s.%s drop if exists \n" % (
            db_name, table_name)

        built_partitions = self._build_partitions(partitions)

        if len(built_partitions) > 0:
            hive_sql = hive_sql + "\n".join(built_partitions) + ";"

            _logger.debug("executed hive sql:%s" % (hive_sql))
            cr = self.execute(sql=hive_sql)

            if cr.status == 0:
                return True
            else:
                _logger.error(cr.stderr_text)
                raise HiveCommandExecuteError(
                    "the hive command:%s error! error info:%s" % (hive_sql, cr.stderr_text))
            return False

    def _build_partitions(self, partitions):
        built_partitions = []
        for partition in partitions:
            values = []
            for key, value in partition.items():
                if isinstance(value, int):
                    values.append("%s=%s" % (key, value))
                else:
                    values.append("%s='%s'" % (key, value))
            built_partitions.append("partition(%s)" % (",".join(values)))
        return built_partitions

    def has_table(self, db_name, table_name):
        tables = self.show_tables(db_name)
        for table in tables:
            if table == table_name:
                return True

        return False

    def show_tables(self, db_name, like_parttern=None):
        "substitute for `show tables`"
        if like_parttern:
            like_parttern = "like '%s'" % (like_parttern)
        else:
            like_parttern = ""

        hive_sql = "use %s;show tables %s;" % (db_name, like_parttern)

        _logger.debug("executed hive sql:%s" % (hive_sql))
        cr = self.execute(sql=hive_sql)

        if cr.status == 0:
            return self._parse_lines_to_sequence(cr.stdout_text)
        else:
            _logger.error(cr.stderr_text)
            raise HiveCommandExecuteError(
                "the hive command:%s error! error info:%s" % (hive_sql, cr.stderr_text))

    def show_functions(self):
        "substitute for `show functions`"
        hive_sql = "show functions;"

        _logger.debug("executed hive sql:%s" % (hive_sql))
        cr = self.execute(sql=hive_sql)

        if cr.status == 0:
            return self._parse_lines_to_sequence(cr.stdout_text)
        else:
            _logger.error(cr.stderr_text)
            raise HiveCommandExecuteError(
                "the hive command:%s error! error info:%s" % (hive_sql, cr.stderr_text))

    def has_function(self, function_name):
        functions = self.show_functions()
        for function in functions:
            if function == function_name:
                return True

        return False

    def show_create_table(self, db_name, table_name):
        "substitute for `show create table`"
        hive_sql = "show create table %s.%s;" % (db_name, table_name)

        _logger.debug("executed hive sql:%s" % (hive_sql))
        cr = self.execute(sql=hive_sql)

        if cr.status == 0:
            return cr.stdout_text
        else:
            _logger.error(cr.stderr_text)
            raise HiveCommandExecuteError(
                "the hive command:%s error! error info:%s" % (hive_sql, cr.stderr_text))

    def desc_table(self, db_name, table_name, extended=False):
        "substitute for `desc table_name`"

        if extended:
            hive_sql = "desc formatted %s.%s;" % (db_name, table_name)
        else:
            hive_sql = "desc %s.%s;" % (db_name, table_name)

        _logger.debug("executed hive sql:%s" % (hive_sql))
        cr = self.execute(sql=hive_sql)

        if cr.status == 0:
            if extended:
                return self._parse_formatted_table(cr.stdout_text)
            else:
                return self._parse_table(cr.stdout_text)
        else:
            _logger.error(cr.stderr_text)
            raise HiveCommandExecuteError(
                "the hive command:%s error! error info:%s" % (hive_sql, cr.stderr_text))

    def _parse_extended_table(self, text):
        "parse output of the hive command desc extended table to dict type."
        table_info = {"tableName": None, "dbName": None, "owner": None, "createTime": None,
                      "lastAccessTime": None, "retention": None,
                      "viewOriginalText": None, "viewExpandedText": None, "tableType": None,
                      "location": None, "inputFormat": None, "outputFormat": None,
                      "compressed": None, "numBuckets": None,
                      "line.delim": None, "field.delim": None, "serialization.format": None,
                      "fields": [], "partitions": [], "bucketCols": [], "sortCols": [],
                      }

        if text:
            m = re.search('.*Table\(([\s\S]*)\)', text)
            if m is not None:
                text = m.groups()[0]

                end_tag = "],"
                search_rules = {
                    "fields": "sd:StorageDescriptor(cols:[", "partitions": "partitionKeys:[", "bucketCols": "bucketCols:[", "sortCols": "sortCols:["}

                for key, search_tag in search_rules.items():
                    sidx = text.find(search_tag)
                    eidx = text.find(end_tag, sidx)
                    fields_text = text[sidx + len(search_tag):eidx + 1]
                    # generate fields info
                    for field_schema in fields_text.split("FieldSchema("):
                        values = field_schema.strip()[0:-2].split(",")
                        if len(values) >= 2:
                            field_dict = {}
                            for value in values:
                                field = value.strip().split(":")
                                if len(field) > 1:
                                    field_dict[field[0].strip()] = "".join(
                                        field[1:])
                            table_info[key].append(field_dict)

                values = re.split("[,})]", text)

                for key, _ in table_info.items():
                    if key not in search_rules.keys():
                        for value in values:
                            value = value.lstrip()
                            if value.startswith(key):
                                table_info[key] = value[len(key) + 1:]

                if table_info['field.delim'] is None:
                    table_info['field.delim'] = '\0001'
                else:
                    print(len(table_info['field.delim']))
                    if len(table_info['field.delim']) == 1:
                        table_info['field.delim'] = chr(
                            ord(table_info['field.delim']))

                if table_info['line.delim'] is None:
                    table_info['line.delim'] = '\n'
                else:
                    if len(table_info['line.delim']) == 1:
                        table_info['line.delim'] = chr(
                            ord(table_info['line.delim']))

        return table_info

    def _parse_table(self, text):
        "parse output of the hive command desc table to dict type."
        fields_part_info = []
        partitions_part_info = []
        table_info = {"fields": fields_part_info,
                      "partitions": partitions_part_info}

        if text:
            partition_info_flag = False
            for line in text.strip().split("\n"):
                if line.startswith("#"):
                    partition_info_flag = True
                    continue

                if line.find("col_name") >= 0 and line.find("data_type") >= 0:
                    continue

                line = line.strip()
                if len(line) == 0:
                    continue

                elements = None
                m = re.match('(\S+)\s+(\S+)\s+(.*)', line)
                if m is None:
                    m = re.match('(\S+)\s+(\S+)', line)

                if m:
                    elements = m.groups()

                if elements:
                    if partition_info_flag:
                        partitions_part_info.append(elements)
                    else:
                        fields_part_info.append(elements)
        return table_info

    def _parse_formatted_table(self, text):
        "parse output of the hive command desc extended table to dict type."
        fields_part_info = []
        partitions_part_info = []

        table_formatted_info = {"owner": None,
                                "lastAccessTime": None, "retention": None,"tableType": None,
                                "location": None, "inputFormat": None, "outputFormat": None,
                                "compressed": None, "numBuckets": None,
                                "line.delim": None, "field.delim": None, "serialization.format": None,
                                "fields": fields_part_info, "partitions": partitions_part_info, "bucketColumns": [], "sortColumns": [],
                                }

        if text:
            partition_info_flag = False
            detailed_table_info_flag = False
            storage_info_flag = False
            lines = text.strip().split("\n")
            if len(lines) > 2:
                lines = lines[2:]

            for line in lines:
                if line.startswith("# Partition Information"):
                    partition_info_flag = True
                    continue

                if line.startswith("# Detailed Table Information"):
                    partition_info_flag=False
                    detailed_table_info_flag = True
                    continue

                if line.startswith("# Storage Information"):
                    partition_info_flag=False
                    detailed_table_info_flag = False
                    storage_info_flag = True
                    continue

                if line.find("col_name") >= 0 and line.find("data_type") >= 0:
                    continue

                if len(line.strip()) == 0:
                    continue

                elements = None
                line = line.lstrip()
                if detailed_table_info_flag or storage_info_flag:
                    #print(repr(line))
                    m = re.match(r'((?:\S+\s?)+\S+)\:?[ \t\v]+(\S+|[\n\r\f]?)', line)
                else:
                    m = re.match(r'(\S+)\s+(\S+)\s+(.*)', line)
                    if m is None:
                        m = re.match(r'(\S+)\s+(\S+)', line)

                if m:
                    elements = m.groups()
                    #print(elements)

                if elements:
                    if partition_info_flag:
                        if len(elements)>=2:
                            partitions_part_info.append(elements[:2])
                    elif detailed_table_info_flag or storage_info_flag:
                        if len(elements) == 2:
                            key=elements[0]
                            value=elements[1]
                            pattern=re.compile(r"[\s|:]+")
                            key= key[:1].lower()+re.sub(pattern,'',key[1:])
                            if table_formatted_info.has_key(key):
                                if key=="line.delim" and value=="":
                                    value = r"\n"
                                elif key=="bucketColumns" or key=="sortColumns":
                                    p=re.compile("\[|\]")
                                    value=re.sub(p,'',value).split(",")
                                table_formatted_info[key] = value
                    else:
                        if len(elements)>=2:
                            fields_part_info.append(elements[:2])

        return table_formatted_info

    def show_databases(self, like_parttern=None):
        "substitute for `show databases`"
        if like_parttern:
            like_parttern = "like '%s'" % (like_parttern)
        else:
            like_parttern = ""

        hive_sql = "show databases %s;" % (like_parttern)

        _logger.debug("executed hive sql:%s" % (hive_sql))
        cr = self.execute(sql=hive_sql)

        if cr.status == 0:
            return self._parse_lines_to_sequence(cr.stdout_text)
        else:
            _logger.error(cr.stderr_text)
            raise HiveCommandExecuteError(
                "the hive command:%s error! error info:%s" % (hive_sql, cr.stderr_text))

    def desc_database(self, db_name):
        "substitute for `desc database`"
        hive_sql = "set hive.cli.print.header=true;desc database %s;" % (
            db_name)

        _logger.debug("executed hive sql:%s" % (hive_sql))
        cr = self.execute(sql=hive_sql)

        if cr.status == 0:
            return self._parse_database(cr.stdout_text)
        else:
            _logger.error(cr.stderr_text)
            raise HiveCommandExecuteError(
                "the hive command:%s error! error info:%s" % (hive_sql, cr.stderr_text))

    def _parse_database(self, text):
        db_info = {}
        if text:
            lines = text.strip().split("\n")
            if len(lines) == 2:
                header_names = re.split("\s+", lines[0])
                values = re.split("\s+", lines[1])
                for idx, value in enumerate(header_names):
                    if len(values) > idx:
                        if value == "comment" and values[idx].startswith("hdfs://"):
                            values.insert(idx, "")
                        db_info[value] = values[idx]
                    else:
                        db_info[value] = ""

        return db_info

    def desc_function(self, db_name, table_name, extended=False):
        "unsupported"
        pass

    def show_roles(self):
        "substitute for `show roles`"
        hive_sql = "show roles;"

        _logger.debug("executed hive sql:%s" % (hive_sql))
        cr = self.execute(sql=hive_sql)

        if cr.status == 0:
            return self._parse_lines_to_sequence(cr.stdout_text)
        else:
            _logger.error(cr.stderr_text)
            raise HiveCommandExecuteError(
                "the hive command:%s error! error info:%s" % (hive_sql, cr.stderr_text))

    def drop_table(self, db_name, table_name):
        "substitute for `drop table db_name.table_name`"
        hive_sql = "drop table if exists %s.%s;" % (db_name, table_name)

        _logger.debug("executed hive sql:%s" % (hive_sql))
        cr = self.execute(sql=hive_sql)

        if cr.status == 0:
            return True
        else:
            _logger.error(cr.stderr_text)
            raise HiveCommandExecuteError(
                "the hive command:%s error! error info:%s" % (hive_sql, cr.stderr_text))
        return False

    def load_data(self, inpath, db, table_name, partitions=None, local=True, overwrite=True):
        """
        substitute for `load data [local] inpath ''
        [overwrite] into TABLE table_name
        partition (dt='20160501',hour='12')`
        """
        hive_sql = ""
        if local:
            hive_sql = "load data local inpath '%s'" % (inpath)
        else:
            hive_sql = "load data inpath '%s'" % (inpath)

        if overwrite:
            hive_sql = "%s overwrite into table %s.%s" % (
                hive_sql, db, table_name)
        else:
            hive_sql = "%s into table %s.%s" % (hive_sql, db, table_name)

        if partitions:
            partition_seq = []
            for key, value in partitions.items():
                partition_seq.append("%s='%s'" % (key, value))
            hive_sql = "%s partition (%s);" % (
                hive_sql, ",".join(partition_seq))
        else:
            hive_sql = "%s;" % (hive_sql)

        _logger.debug("executed hive sql:%s" % (hive_sql))
        cr = self.execute(sql=hive_sql)

        if cr.status == 0:
            return True
        else:
            _logger.error(cr.stderr_text)
            raise HiveCommandExecuteError(
                "the hive command:%s error! error info:%s" % (hive_sql, cr.stderr_text))
        return False

    def _parse_partitions(self, text):
        partitions = []
        if text:
            lines = text.strip().split("\n")
            for line in lines:
                if len(line) > 0:
                    pairs = line.split("/")
                    partition_dict = OrderedDict()
                    for pair in pairs:
                        key, value = tuple(pair.split("="))
                        partition_dict[key] = value
                    if len(partition_dict) > 0:
                        partitions.append(partition_dict)
        return partitions

    def _parse_lines_to_sequence(self, text):
        tables = []
        if text:
            lines = text.strip().split("\n")
            for line in lines:
                if len(line) > 0:
                    tables.append(line)

        return tables

    def execute(self, variable_substitution=None, init_sql_file=None, sql_file=None, sql=None, output_file=None):
        """this method can be used to execute hive cliet commands.

        Parameters
        ----------
        variable_substitution : Optional[str]
            The parameter that contains key-value pairs is a dict type variant.
        init_sql_file : Optional[str]
            The path of the initialization sql file
        sql_file : Optional[str]
            The path of the hive sql
        sql : Optional[str]
            The hive sql,if this parameter is required,the parameter of the sql_file will be disable.
        output_file : Optional[str]
            When passed the parameter 'sql',this parameter can be used.

        Raises
        ------
        ValueError
            The parameters violate the rules below.

        """

        # validate the parameters
        if variable_substitution and not isinstance(variable_substitution, dict):
            raise ValueError(
                "the function execute_hive_sql:The argument variable_substitution must be dict type.")

        if sql_file is None and sql is None:
            raise ValueError(
                "the function execute_hive_sql:At least one argument of sql_file and sql passed.")

        if init_sql_file and not os.path.exists(init_sql_file):
            raise ValueError(
                "the function execute_hive_sql:The initialization sql file:%s doesn't exists." % (init_sql_file))

        if sql_file and not os.path.exists(sql_file):
            raise ValueError(
                "The hive sql file:%s doesn't exists." % (sql_file))

        if output_file and sql is None:
            raise ValueError(
                "the function execute_hive_sql:Just when the sql parameter passed,the output file parameter can be used.")

        hive_vars = ""
        if variable_substitution:
            for element in variable_substitution.items():
                hive_vars = "".join(
                    [hive_vars, " -d ", element[0], "=", "\"", element[1], "\""])

        hive_init_sql_file = ""
        if init_sql_file:
            hive_init_sql_file = " -i %s" % (init_sql_file)

        hive_sql_file = ""
        hive_sql = ""
        if sql:
            if len(self.hive_init_settings) > 0:
                hive_sql = " -e \"%s;%s\"" % (
                    ";".join(self.hive_init_settings), sql)
            else:
                hive_sql = " -e \"%s\"" % (sql)
        else:
            if sql_file:
                hive_sql_file = " -f %s" % (sql_file)

        hive_output_file = ""
        if sql and output_file:
            hive_output_file = " > %s" % (output_file)

        hive_verbose = ""
        if self.enable_verbose_mode:
            hive_verbose = " -v "

        execute_cmd = "".join([self.__default_hive_command, hive_verbose, hive_init_sql_file,
                               hive_vars, hive_sql_file, hive_sql, hive_output_file])

        _logger.info("the function execute:%s" % (execute_cmd))

        cr = CommandExecutor.system(execute_cmd)

        return cr
