# -*- coding:utf-8 -*-
__author__ = 'frem.zhao@corp.to8to.com'

import MysqlTransferAll_config

table_name = 'to8to_yuyue_yyhh_zxgj'  # 设置表名

table = MysqlTransferAll_config.table
columns = table['to8to'][table_name]['columns']
columns_str = '"' + '", "'.join(columns) + '"'
columns_str = columns_str.replace(', "NULL"', '')
print columns_str