# hive-executor-py

Install package:
pip install hive-executor-py
or
easy_install hive-executor-py

Requirement:
* common-utils package
pip install common-utils
or
easy_install common-utils

* Hive client environment

Usage:

```
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
```