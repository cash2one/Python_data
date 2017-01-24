# -*- coding:utf-8 -*-
__author__ = 'fremcode@gmail.com'

import os
import time
import sys
from cube import MyTime
from cube import MyLog
from cube import MyTool
from cube import MyHiveBin
from cube import MyAlarm

latest_time = 'xxxx-xx-xx 20:00:00'
time_rate = 120
tar_src = "/data1/bi/platform/rawdata/1004/hive_mysql_actual_xxxx-xx-xx.tar.gz"
tar_des = "/data1/bi/platform/tar/xxxx-xx-xx/MysqlActual/"
next_script = "sh /data1/bi/datacenter/rep/shell/rep_waste_19.sh &>> /data1/bi/datacenter/logs/shell/rep_waste_19.sh.log"

log = MyLog.MyLog(path='/data1/log/platform/', name='ToHive', type='MysqlActual', level='DEBUG')


def file_modify_stat(file_path):
    stat1 = os.stat(file_path)
    time.sleep(60)
    stat2 = os.stat(file_path)
    if stat2 > stat1:
        return False
    else:
        return True


def load_file(date, today):
    global tar_src
    global tar_des
    global latest_time
    tar_src = tar_src.replace('xxxx-xx-xx', date)
    tar_des = tar_des.replace('xxxx-xx-xx', date)
    print tar_src
    latest_time_stamp = MyTime.datetime_timestamp(latest_time.replace('xxxx-xx-xx', today))
    if not os.path.exists(tar_des):
        shell = "mkdir -p " + tar_des
        os.system(shell)
    while 1:
        if os.path.exists(tar_src):
            if file_modify_stat(tar_src):
                if MyTool.tar_file(tar_src, tar_des):
                    for root, dirs, files in os.walk(tar_des):
                        for d in dirs:
                            print os.path.join(root, d)
                        for f in files:
                            file = os.path.join(root, f)
                            shell = '/usr/bin/dos2unix ' + file
                            os.popen(shell)
                            hive = MyHiveBin.HiveBin()
                            hive.load_file_single_overwrite('to8to_rawdata', file)
                    log.info('Mysql actual kefu yuyue to hive status:ok')
                    break
        else:
            log.info('Mysql actual tar file not exists')
            now_time_stamp = MyTime.datetime_timestamp(MyTime.get_local_time())
            if now_time_stamp > latest_time_stamp:
                log.critical('not find ' + tar_src)
                MyAlarm.send_mail_sms('Get Mysql actual tar file status:fail!')
                return False

        time.sleep(time_rate)
    return True

if __name__ == '__main__':
    date = MyTime.get_date(1)
    if len(sys.argv) == 2:
        date = sys.argv[1]
    today = MyTime.get_date(0)
    if load_file(date, today):
        os.system(next_script)
