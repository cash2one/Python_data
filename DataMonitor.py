#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = 'frem.zhao@corp.to8to.com'

import sys
import requests
from com.utils import MyAlarm
from com.utils import MySQL
from com.utils import MyTool
from com.utils import MyTime


def get_report_sql(report_cid, args_dict):
    """
    设置报表cid，获取报表查询sql，时间变量用${date_start}，${date_end}
    :param report_cid:
    :param args_dict get参数dict,同data platform页面url传参格式
    :return:
    """
    api_url = 'http://192.168.1.119:8889/DataApi/GetReportSQL/'
    post_args = {
        'app': 'office.DataMonitor',
        'token': '6d44a776091069dad798de3d3df5653f',
        'cid': report_cid,
        'date_start': '${date_start}',
        'date_end': '${date_end}'
    }
    post_args = dict(post_args, **args_dict)

    try:
        req = requests.post(url=api_url, data=post_args)

        if req.status_code == 200:
            result = req.json()
            if result['status'] == 'success':
                return result['value']['sql']
            else:
                log_str = 'request report api data status is not success'
                print MyTime.get_local_time(), '==>', log_str
                return False
        else:
            log_str = 'request report url status is not 200'
            print MyTime.get_local_time(), '==>', log_str
            return False

    except Exception, ex:
        log_str = 'connect %s error ' % api_url + str(ex)
        print MyTime.get_local_time(), '==>', log_str


def get_report_data(monitor_config, date_args):
    """
    从data.to8to.com配置报表获取数据
    :param monitor_config: 表monitor_config字段config值
    :date_args: 日期参数 {'date_start': '20160505', 'date_end': '20160509'}
    :return: list 查询结果二维数据
    """
    conf_db = monitor_config['db']
    conf_sql = monitor_config['sql']
    query = conf_sql.replace('${date_start}', date_args['date_start'])
    query = query.replace('${date_end}', date_args['date_end'])
    col_name_list = monitor_config['col_name']

    conn = MySQL.Connection(host=conf_db['host'], database=conf_db['database'],
                            user=conf_db['user'], password=conf_db['password'])
    _result = conn.query(query)
    return MyTool.get_list_value_by_key(_result, col_name_list)


if __name__ == '__main__':
    if not len(sys.argv) == 4:
        print "invalid parameter! use like:"
        print "arg1:报表名称(随意指定);arg2:报表cid;arg3:监控字段;"
        print "python DataMonitor.py \"项目日报\" \"7790\" \"登记_总数,新增_登记\""
        exit()
    else:
        pass

    report_name = sys.argv[1]
    sql = get_report_sql(sys.argv[2], {})
    col_name = sys.argv[3]
    col_name = col_name.replace(' ', '').split(',')

    mn_config = {
        'db': {
            'host': '192.168.1.118',
            'database': 'to8to_result',
            'user': 'datacenter',
            'password': 'LoveTo8toData'
        },
        'sql': sql,
        'col_name': col_name
    }

    today_date = MyTime.get_date(1)
    today_date = today_date.replace('-', '')
    dt_args = {
        'date_start': today_date,
        'date_end': today_date
    }

    result = get_report_data(mn_config, dt_args)

    format_str = ''
    for key in result:
        format_str += key + ':' + str(result[key][0]) + ';'
    format_str = report_name + ':[' + format_str + ']'
    
    MyAlarm.send_mail_sms(format_str)
