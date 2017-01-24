#!/usr/bin/env python
#-*- coding: utf-8 -*-
import os
import sys
src_path = os.getcwd()
sys.path.append(src_path)

import time
import datetime
import MysqlTransferAll_config as config
file_list = list()


#数据库配置
db_config = {'cube': {'host': '192.168.1.119', 'port': '3306', 'user': 'frem', 'password': '123456', 'charset': 'utf8'} }

#生成临时txt文件和tar.gz文件路径
txt_path = src_path + '/tmp/bi/Mysql/'
tar_path = src_path + '/tmp/bi/Mysql/'
tar_name_rule = 'bi_mysql_all_xxxx-xx-xx.tar.gz'

#1、生成临时txt文件和tar.gz文件路径, 如果有就跳过
def init_file_path():
    if not os.path.isdir(txt_path):
        os.system("mkdir -p " + txt_path)
    if not os.path.isdir(tar_path):
        os.system("mkdir -p " + tar_path)
    os.system("rm -rf " + txt_path + "*.txt")

#2、主要是如果昨天及历史数据导失败，可以设置时间重新导，主要是用于增量数据，注意增量的时间字段不一样（注意fitter）
def get_date(day_before):
    return str(datetime.date.today() - datetime.timedelta(days=day_before))


#3、从数据库导出数据保存至txt。设计上可以从多个数据库导数据
def save_file(date, db, table, table_config_value):
    global file_list, txt_path, db_config
    db_host = db_config[db]['host']
    db_port = db_config[db]['port']
    db_user = db_config[db]['user']
    db_pwd = db_config[db]['password']

    columns = table_config_value['columns']                                  #columns=['rid', 'yid', 'ctime', 'cid', 'eventtype', 'status', 'nexttime', 'ts_object', 'ts_type', 'complaints', 'wl_id', 'md5(md5(ccic_uniqueid))', 'ts_level', 'ts_handler', 'ts_pl', 'ts_brand']
    select_column = ",".join(columns)
    sql = "set names utf8;select " + select_column + " from " + table + ";"

    if 'fitter' in table_config_value:                                       #增量导条件
        fitter = table_config_value['fitter']                                #table['to8to']['to8to_yuyue_yyhh']['fitter']='callbacktime>=UNIX_TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 2 DAY)) and callbacktime<UNIX_TIMESTAMP(CURDATE())'
        sql = "set names utf8;select " + select_column + " from " + table + " where " + fitter + ";"

    mysql_bin = "mysql -N -h" + db_host + " -P" + db_port + " -u" + db_user + " -p" + db_pwd + " -D" + db + " -A"
    file_txt_path = txt_path + db + "." + table + "_" + date + ".txt"
    shell = "echo \"" + sql + "\" | " + mysql_bin + " >> " + file_txt_path   # file_txt_path = '/tmp/bi/Mysql/to8to.to8to_yuyue_yyhh_2016-12-15.txt'
    print shell

    if os.path.exists(file_txt_path):
        try:
            os.remove(file_txt_path)
        except Exception, ex:
            print ex

    print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + " " + db + "." + table + " begin"
    os.system(shell)                                           #执行shell命令
    print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + " " + db + "." + table + " success"
    file_list.append(file_txt_path)

#4、把txt文件压缩，并删除txt
def tar_file_create(date):
    global file_list
    tar_file_name = tar_name_rule.replace('xxxx-xx-xx', date)   # tar_name_rule = 'bi_mysql_all_xxxx-xx-xx.tar.gz'
    tar_file_path = tar_path + tar_file_name                    #tar_path = '/tmp/bi/Mysql/'
    command = "cd " + txt_path + ";tar -zcvf " + tar_file_path + " *.txt"    #txt_path = '/tmp/bi/Mysql/'   tar -zcvf /tmp/bi/Mysql/bi_mysql_all_xxxx-xx-xx.tar.gz *.txt
    os.system(command)
    for file in file_list:
        os.system("rm -rf " + file)                             #删除根目录及其子目录下所有文件，删除后文件很难恢复，慎用


if __name__ == '__main__':
    print '======================================================================>from Mysql get data'
    init_file_path()                                            #生成临时txt文件和tar.gz文件路径
    date = get_date(1)                                          #参数是1，生成的日期是1天前
    if len(sys.argv) == 2:
        date = sys.argv[1]                                      #假如执行脚本时，附带参数，表示按指定日期去回滚

    table_config = config.table                                 #所有表字段

    for db in table_config:
        for table in table_config[db]:
            try:
                table_config_value = table_config[db][table]    #赋值类似：{'columns': ['tid', 'uid', 'yid', 'type', 'num', 'now_num', 'puttime', 'now_num_2', 'now_num_3', 'now_num_4', 'typeid', 'now_num_5', 'now_num_6', 'now_num_7']}
            except KeyError:
                print '======================================================================>config file error'
                continue
            save_file(date, db, table, table_config_value)      #  table_config[db][table] = table['to8to']['to8to_dw_item_record']  如：table['to8to']['to8to_dw_item_record']
                                                                #{'columns': ['rid', 'yid', 'ctime', 'cid', 'eventtype', 'status', 'nexttime', 'ts_object', 'ts_type', 'complaints', 'wl_id', 'md5(md5(ccic_uniqueid))', 'ts_level', 'ts_handler', 'ts_pl', 'ts_brand']}
    tar_file_create(date)



    #1.1、以上是从mysql提取数据

    #1.2、传输数据

    #1.3、导入hive
    print '======================================================================>data to HIVE'
    mysql_to8to_shell = 'python /data1/bi/usertest/user/lincoln/data/MysqlAllToHive.py ' + date
    os.system(mysql_to8to_shell)

    #2.1、从mongdb抓数据并传输

    #2.1、解压、解析、并导入HIVE（用点击流做示例）
    print '======================================================================>liu to HIVE'
    clickstream_to8to_shell = 'python /data1/bi/usertest/user/lincoln/data/jun/ClickStreamSub.py' #+ date
    os.system(clickstream_to8to_shell)


#    1002 客服
#    1003 APP
#    1004 项目预约表和项目轮单表
#    1005 客服相关（电话坐席）
#    1008 微信
#    1009 设计本、土巴兔、图满意、3D、APP
#    1010 大概是APP的日志
#    1011 大量的表
#    SEM  百度、神马


#文件传输后存放处：    /data1/bi/platform/rawdata/1011/                                 传输方式rsync
#解压导入HIVE脚本路径：/data1/bi/platform/scripts/BI/Mysql/MysqlAll/MysqlAllToHive.py   脚本执行日志：/data1/log/platform/ToHive_2016-12-20.log
#压缩包解压文件存放处  /data1/bi/platform/tar/2016-12-19


#/data1/bi/platform/scripts/BI/DataTransfer/weixin/LoadWeixinMongo.py  微信源数据执行脚本

#/data1/bi/platform/rawdata/SEM/Baidu/Operate/2016-12-20  百度账户消费源数据