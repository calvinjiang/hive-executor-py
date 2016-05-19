#####################################
# Author: Hua Jiang
# Date: 2016-04-23
# Desc:
#
######################################

import os
import subprocess
import re
import logging
from collections import OrderedDict
from hive.exceptions import (HiveUnfoundError,
                             SystemCommandExecuteError,
                             HiveCommandExecuteError,
                             )

_logger = logging.getLogger(__name__)



class CommandResult(object):
    """The class will be used to stored result of some methods in HivExecutor.

    Up to now the class only be used in the method of _execute_system_command
    in the class HiveExecutor.

    Attributes
    ----------
    stdout_text : str
        The result of standart out.
    stderr_text : str
        The result of standart error.
    status : int
        The status of system command returns.

    """

    def __init__(self, stdout_text, stderr_text, status):
        self.stdout_text = stdout_text
        self.stderr_text = stderr_text
        self.status = status


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
    >>> import HiveExecutor
    >>> hive=HiveExecutor("hive")
    >>> databases=hive.show_databases()
    >>> print(databases)
    ['default', 'test']
    >>> databases=hive.show_databases('defau*')
    >>> print(databases)
    ['default']

    >>> tables=hive.show_tables('default')
    ['table1', 'table2']

    """

    def __init__(self, hive_cmd_path="hive", verbose=False):
        if hive_cmd_path is None or len(hive_cmd_path) == 0:
            raise ValueError(
                "When you passed the argument of hive_cmd_path,it should have a value.")

        cmd = "which %s" % (hive_cmd_path)
        result = self._execute_system_command(cmd)

        if result.status != 0:
            raise HiveUnfoundError(
                "the hive command path:%s is not exists." % (hive_cmd_path))

        self.hive_cmd_path = hive_cmd_path
        self.enable_verbose_mode = verbose
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
        else:
            return []

    def has_table(self, db_name, table_name):
        tables = self.show_tables(db_name)
        for table in tables:
            if table == table_name:
                return True

        return False

    def show_tables(self, db_name, like_parttern=None):

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


    def show_partitions(self, db_name, table_name, search_partitions=None):
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

        hive_sql = "use %s;show partitions %s %s;" % (db_name, table_name, explicit_partition)

        _logger.debug("the function show_partitions() execute:%s" % (hive_sql))

        cr = self.execute(sql=hive_sql)

        if cr.status == 0:
            return self._parse_partitions(cr.stdout_text)
        else:
            _logger.error(cr.stderr_text)
            raise HiveCommandExecuteError(
                "the hive command:%s error! error info:%s" % (hive_sql, cr.stderr_text))

        return None


    def show_functions(self):
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
        hive_sql = "desc %s.%s;" % (db_name, table_name)

        _logger.debug("executed hive sql:%s" % (hive_sql))
        cr = self.execute(sql=hive_sql)

        if cr.status == 0:
            return self._parse_table(cr.stdout_text)
        else:
            _logger.error(cr.stderr_text)
            raise HiveCommandExecuteError(
                "the hive command:%s error! error info:%s" % (hive_sql, cr.stderr_text))

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
                if len(line)==0:
                    continue

                field_info = {}
                elements=None
                m=re.match('(\S+)\s+(\S+)\s+(.*)',line)
                if m is None:
                    m=re.match('(\S+)\s+(\S+)',line)

                if m:
                    elements=m.groups()

                if elements:
                    if partition_info_flag:
                        partitions_part_info.append(elements)
                    else:
                        fields_part_info.append(elements)
        return table_info

    def show_databases(self,like_parttern=None):
        if like_parttern:
            like_parttern = "like '%s'" % (like_parttern)
        else:
            like_parttern = ""

        hive_sql="show databases %s;" %(like_parttern)

        _logger.debug("executed hive sql:%s" % (hive_sql))
        cr = self.execute(sql=hive_sql)

        if cr.status == 0:
            return self._parse_lines_to_sequence(cr.stdout_text)
        else:
            _logger.error(cr.stderr_text)
            raise HiveCommandExecuteError(
                "the hive command:%s error! error info:%s" % (hive_sql, cr.stderr_text))


    def desc_database(self, db_name, extended=False):
        hive_sql = "set hive.cli.print.header=true;desc database %s;" % (db_name)

        _logger.debug("executed hive sql:%s" % (hive_sql))
        cr = self.execute(sql=hive_sql)

        if cr.status == 0:
            return self._parse_database(cr.stdout_text)
        else:
            _logger.error(cr.stderr_text)
            raise HiveCommandExecuteError(
                "the hive command:%s error! error info:%s" % (hive_sql, cr.stderr_text))


    def _parse_database(self,text):
        return text


    def desc_function(self, db_name, table_name, extended=False):
        pass

    def desc_formatted_table(self,db_name,table_name):
        pass

    def show_roles(self):
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
        pass

    def create_table(self, create_table_sql):
        pass

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

    def _execute_system_command(self, command):
        status = 0
        process = subprocess.Popen(
            command, shell=True, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, err = process.communicate()
        status = process.poll()
        return CommandResult(output, err, status)

    def execute(self, variable_substitution=None, init_sql_file=None, sql_file=None, sql=None, output_file=None):
        """
        Parameters:
        variable_substitution:The parameter that contains key-value pairs is a dict type variant.
        init_sql_file:the path of the initialization sql file
        sql_file:the path of the hive sql
        sql:the hive sql,if this parameter is required,the parameter of the sql_file will be disable.
        output_file:when passed the parameter 'sql',this parameter can be used.
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

        cr = self._execute_system_command(execute_cmd)

        return cr
