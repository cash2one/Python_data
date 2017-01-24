# -*- coding:utf-8 -*-
__author__ = 'fremcode@gmail.com'

import sys
import os
import time
import shutil
from cube import MyTime
from cube import MyHiveBin
from cube import MyTool
from cube import MyAlarm

data_base_path = '/data1/bi/platform/rawdata/SEM/Shenma/SemShenmaReport/'
tmp_base_path = '/tmp/frem/sem_sm/'
hive_table_list = ['sem_sm_keyword', 'sem_sm_diyu']
wait_time = 3600

def load_all(date):
    global data_base_path, hive_table_list
    hive = MyHiveBin.HiveBin()
    dt = date.replace('-', '')
    for table in hive_table_list:
        del_command = 'use to8to_rawdata;alter table ' + table + ' drop partition(dt=' + dt + ')'
        hive.execute(del_command)

    data_path = tmp_base_path + date + '/'
    try:
        shutil.rmtree(data_path)
    except Exception, ex:
        print ex
    try:
        os.mkdir(data_path)
    except Exception, ex:
        print ex

    tar_path = data_base_path + 'sem_sm_keyword_' + date + '.tar.gz'
    
    leave_time =0 
    while 1:
        if MyTool.tar_file(tar_path, data_path):
            partition_dict = {'dt': dt}
            for root, dirs, files in os.walk(data_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    if 'sem_sm_keyword' in file:
                        hive.load_file('to8to_rawdata', 'sem_sm_keyword', file_path, partition_dict)
                    elif 'sem_sm_diyu' in file:
                        hive.load_file('to8to_rawdata', 'sem_sm_diyu', file_path, partition_dict)
                    else:
                        pass
            try:
                shutil.rmtree(data_path)
            except Exception, ex:
                print ex

            return True
        else:
            pass
        
        leave_time += 300
        if leave_time > wait_time:
            return False
        time.sleep(300)


if __name__ == '__main__':
    date = MyTime.get_date(1)
    if len(sys.argv) == 2:
        date = sys.argv[1]
    if not load_all(date):
        MyAlarm.send_mail_sms('load sem shenma keyword to hive status:fail')        
