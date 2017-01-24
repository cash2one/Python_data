# -*- coding:utf-8 -*-
__author__ = 'fremcode@gmail.com'

import os
import sys
import time
import commands
from cube import MyTime
from cube import MyAlarm
from cube import MyTool
from cube import MyLog

TABLE_DICT = {
    'user': 'to8to_user',
    'group': 'to8to_group',
    'activity': 'to8to_activity',
    'channel': 'channel',
    'news': 'to8to_news',
    'msg': 'to8to_msg',
    'msglog': 'request_log',
    'rule': 'to8to_rule',
    'activityuser': 'to8to_activity_user',
    'qrcode': 'qrcode',
    'qrcode_log': 'to8to_qrcode_log',
    'to8to_kf_msg': 'to8to_kf_msg',
    'to8to_temp_qrcode': 'to8to_temp_qrcode'
}
TAR_PATH = "/data1/bi/platform/tar/xxxx-xx-xx/weixin/"
PROJECT_PATH = MyTool.current_file_directory() + os.sep
LOG_PATH = "/data1/log/platform/"
log = MyLog.MyLog(path=LOG_PATH, name='ToHive', type='WinxinMongo', level='DEBUG')


def un_tar(date):
    global TAR_PATH
    tar_path = TAR_PATH.replace('xxxx-xx-xx', date)
    if not os.path.exists(tar_path):
        os.system("mkdir " + tar_path)

    tar_file = "weixin" + date + ".tar.gz"
    un_tar_command = "tar zxvf /data1/bi/platform/rawdata/1008/" + tar_file + " -C " + tar_path
    print un_tar_command
    (status, output) = commands.getstatusoutput(un_tar_command)
    if status == 0:
        log_str = "Transfer-Mongodb Weixin-un tar file-" + tar_file + "-status:ok"
        log.info(log_str)
        return True
    else:
        log_str = "Transfer-Mongodb Weixin-un tar file-" + tar_file + "-status:fail"
        print log_str
        log.info(log_str)
        MyAlarm.send_mail_sms(log_str)
        return False


def tar_to_mongodb(date):
    global TAR_PATH
    tar_path = TAR_PATH.replace('xxxx-xx-xx', date)
    where_tar = tar_path + "opt/script/data/" + date + '/en/'
    for table in TABLE_DICT:
        filename = table + date + ".json"
        load_command = "mongoimport --upsert --host 192.168.11.11:27018 -d weixin -c " + TABLE_DICT[table] + " " + where_tar + filename
        print load_command
        (status, output) = commands.getstatusoutput(load_command)
        if status == 0:
            continue
        else:
            log_str = "Transfer-Mongodb Weixin-load to mongodb-" + TABLE_DICT[table] + "-status:fail"
            print log_str
            log.error(log_str)
    return True


def mongodb_to_mysql(date):
    global PROJECT_PATH, LOG_PATH
    (status, output) = commands.getstatusoutput('which java')
    if status == 0:
        java_bin = output
    else:
        print "==========================================>get java bin path status:fail"
        return False

    java_command = java_bin + ' -jar -Xms512m -Xmx1024m '
    java_command = java_command + PROJECT_PATH + 'dataToMysql.jar 0 ' + date + ' >> ' + LOG_PATH + 'weixin_mongodb_to_mysql.log'

    log_str = "Transfer-Mongodb Weixin-load to MySQL-status:"
    print time.strftime("%H:%M:%S", time.localtime()), "==========================================>", log_str, "begin"
    log.info(log_str + "begin")

    print time.strftime("%H:%M:%S", time.localtime()), "==========>", java_command

    (status, output) = commands.getstatusoutput(java_command)

    if status == 0:
        print time.strftime("%H:%M:%S", time.localtime()), "==========================================>", log_str, "ok"
        log.info(log_str + "ok")
        return True
    else:
        print time.strftime("%H:%M:%S", time.localtime()), "==========================================>", log_str, "fail"
        log.info(log_str + "fail")
        return True
    #java except also return false,so set this function always return true;


def mysql_to_hive():
    shell_command = 'sh /data1/bi/platform/scripts/weixin/load_mongo_weixin.sh >> ' + LOG_PATH + 'weixin_mongodb_to_mysql.log'
    log_str = "Transfer-Mongodb Weixin-load to Hive-status:"
    print time.strftime("%H:%M:%S", time.localtime()), "==========================================>", log_str, "begin"
    log.info(log_str + "begin")

    (status, output) = commands.getstatusoutput(shell_command)

    if status == 0:
        print time.strftime("%H:%M:%S", time.localtime()), "==========================================>", log_str, "ok"
        log.info(log_str + "ok")
        return True
    else:
        print time.strftime("%H:%M:%S", time.localtime()), "==========================================>", log_str, "fail"
        log.info(log_str + "fail")
        return False

if __name__ == '__main__':
    date_day = MyTime.get_date(1)
    if len(sys.argv) == 2:
        date_day = sys.argv[1]

    print time.strftime("%H:%M:%S", time.localtime()), "==========================================>un tar begin"
    if un_tar(date_day):
        print time.strftime("%H:%M:%S", time.localtime()), "==========================================>load to mongodb begin"
        if tar_to_mongodb(date_day):
            print time.strftime("%H:%M:%S", time.localtime()), "==========================================>load to mongodb end"
            if mongodb_to_mysql(date_day):
                time.sleep(10)
                print time.strftime("%H:%M:%S", time.localtime()), "==========================================>load to hive begin"
                if mysql_to_hive():
                    print time.strftime("%H:%M:%S", time.localtime()), "==========================================>load to hive end"
