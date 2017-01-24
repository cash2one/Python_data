#!/usr/bin/env python
# -*- coding:utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )

import datetime
import os
import sys
import urllib
import tarfile
#from cube import MyLog
from cube import MyAlarm
from cube import MyTool
from cube import MyHiveBin
from cube import MyTime

src_path = os.getcwd()
sys.path.append(src_path)

sys.path.append(src_path+'/module')
import MyLog
log = MyLog.MyLog(path=src_path + '/', name='ToHive', type='MySqlAll', level='DEBUG')


class toHive():

    #1、定义一些时间函数，要加密、要解码、要保留分区的表
    def __init__(self, date): 
        date_before = MyTime.date_before(date)                                             #如果date是昨天的日期，那么 date_before=1
        self.dbName = 'to8to_rawdata'
        self.lastDate = datetime.date.today() - datetime.timedelta(days=date_before)       # self.lastDate = datetime.date(2016, 12, 19)  str(self.lastDate)='2016-12-19'
        print "load mysql all file begin--------------------", self.lastDate
        self.last2Date = datetime.date.today() - datetime.timedelta(days=date_before + 1)
        self.last7Date = datetime.date.today() - datetime.timedelta(days=date_before + 10)
        self.last7DateFormat = str(self.last7Date).replace('-', '')
        self.lastDateFormat = str(self.lastDate).replace('-', '')
        self.data_src = src_path + "/tmp/bi/Mysql/"  # /data1/bi/platform/rawdata/1011/
        self.data_des = src_path + "/tar/"
        self.txtPath = self.data_des + str(self.lastDate) + os.sep + 'Mysql' + os.sep
        self.doPath = self.txtPath
        self.delPath = self.data_des + str(self.last2Date) + os.sep + 'Mysql' + os.sep
        #定义要加密(替换为空)的txt文件的列
        self.cutCol = {
                            "to8to_fcom" : [9,10,11,12,13],
                            "to8to_jj_smt_zb" : [6,8,26],
                            "to8to_yuyue_apply" : [18,42],
                            "to8to_fcom_info" : [6,23,24],
                            "to8to_to8toyw_contact" : [7,8],
                            "to8to_to8toyw_back" : [4,5,6],
                            "to8to_mcom" : [13,14,15,27],
                            "to8to_tuori" : [15,16,20,21],
                            #"to8to_huodong_yanfang" : [2,3],
                            "to8to_huodong_apply" : [2],
                            "to8to_gongdi" : [13]
                        }
        #定义要解码url的字段
        self.unquote = {
                            "to8to_project_src" : [3]
                        }
        #定义保留分区的表
        self.reserve = ('to8to_fcom','to8to_yuyue_apply_fp','to8to_zxb','to8to_yuyue_apply','to8to_dw_login_record','to8to_yuyue_apply_record','to8to_nps_record','to8to_item_condition','ts_record','to8to_jj_jianli_clock','to8to_yuyue_yyhh_shkf','to8to_apply_config_node','to8to_apply_config','to8to_credit_log','sem_360_diyu')
    
    #2、解压tar.gz文件
    def tarFile(self):
        #Mysql-tarfile
        tarMysqlName = 'bi_mysql_all_' + str(self.lastDate) + ".tar.gz"   #bi_mysql_daily_
        tarMysql = self.data_src + tarMysqlName                             # tarMysql ='/data1/bi/platform/rawdata/1011/bi_mysql_daily_2016-12-19.tar.gz'
        tar_status = MyTool.tar_file(tarMysql, self.txtPath)                # 解压模块  MyTool.tar_file

        if tar_status == 'IOError':
            logString = 'Mysql file:' + tarMysqlName + ' not found-status:fail'
            log.debug(logString)
            #MyAlarm.send_mail_sms(logString)
            return False
        elif tar_status == False:
            logString = 'tar ' + tarMysqlName + '-status:fail'
            log.info(logString)
            #MyAlarm.send_mail_sms(logString)
            return False
        else:
            logString = 'tar ' + tarMysqlName + '-status:ok'               #  tarMysqlName = 'bi_mysql_daily_2016-12-19.tar.gz'
            log.info(logString)
            return True

    #3、替换隐私信息为null
    def cutTxt(self):
        list_dirs = os.walk(self.doPath)            # self.doPath = '/data1/bi/platform/tar/2016-12-19/Mysql/'    os.walk(self.doPath) 遍历目录中的文件名，包括子目录，可以设置遍历顺序
        for root, dirs, files in list_dirs:
            for d in dirs:
                #print os.path.join(root, d)
                pass
            for f in files:                        # f= 'to8to.to8to_dw_item_oper_2016-12-20.txt'
                table = f.replace('_' + str(self.lastDate) + '.txt', '').split('.')[1]   # str(self.lastDate)='2016-12-19'    'to8to.to8to_yuyue_apply_2016-12-19.txt' 改变为 'to8to_yuyue_apply'
                print table


                #if self.cutCol.has_key(table):                                          ##定义要加密(替换为空)的txt文件  可以用以下几行
                #    newFile = os.path.join(root, f + 'b')
                #    fp = open(newFile , 'a')
                #    file = os.path.join(root, f)
                #    txt = open(file, 'rb').readlines()
                #    for line in txt:
                #        out = ''
                #        lineList = line.split('\t')
                #        for p in self.cutCol[table]:
                #            lineList[p] = '0'              #注意，这一点与下面不一样，lineList[p] = '0' ， 而 lineList[p] = str(urllib.unquote(lineList[p])) 是解析URL
                #        for list in lineList:
                #            out = out +list + '\t'
                #            out = out.replace('\n', '')
                #        print >>fp, out
                #    fp.close()
                #    os.remove(file)
                #    os.rename(newFile,newFile.replace('.txtb', '.txt'))


                #重点 os.path.join 只起到连接的作用，往里面写东西才能生成文件，os.mkdir( os.path.join(root, f + 'b') )可以直接生成文件

                if self.unquote.has_key(table):                                  # 要解码url的字段   unquote = {'to8to_project_src': [3]}   unquote.has_key('to8to_yuyue_apply')  结果为 False
                    newFile = os.path.join(root, f + 'b')                        # f = 'to8to.to8to_yuyue_apply_2016-12-19.txt'  root = '/data1/bi/platform/tar/2016-12-19/Mysql/'
                    fp = open(newFile , 'a')                                     # newFile= '/data1/bi/platform/tar/2016-12-19/Mysql/to8to.to8to_yuyue_apply_2016-12-19.txtb'
                    file = os.path.join(root, f)                                 # os.path.join：给文件加上路径  file = '/data1/bi/platform/tar/2016-12-19/Mysql/to8to.to8to_yuyue_apply_2016-12-19.txt'
                    txt = open(file, 'rb').readlines()
                    for line in txt:
                        out = ''
                        lineList = line.split('\t')                              #每一行生成一个列表
                        for p in self.unquote[table]:                            #与上面的 unquote.has_key(table) 不同，上面已经判断是否有键值，已经判断有了，所以 unquote[table] 返回的是值（列数）
                            lineList[p] = str(urllib.unquote(lineList[p]))       # lineList[p] 表示第几列    #注意，urllib.unquote 解码操作

                        for list in lineList:
                            out = out +list + '\t'
                            out = out.replace('\n', '')
                        print >>fp, out                                         #特别注意  print >> 用来重定向输出

                    fp.close()
                    os.remove(file)                                             #删除原TXT文件
                    os.rename(newFile,newFile.replace('.txtb', '.txt'))         #重命名txtb文件
                else:
                    pass
                                

    #4、替换后的txt文件存入hive
    def txtToHive(self):
        hive = MyHiveBin.HiveBin()
        list_dirs = os.walk(self.doPath)                                        # self.doPath = '/data1/bi/platform/tar/2016-12-21/Mysql/'
        for root, dirs, files in list_dirs: 
            for d in dirs: 
                print os.path.join(root, d)  #没有处
            for f in files: 
                file = os.path.join(root, f)                                    #  给文件名加路径 ， file = '/data1/bi/platform/tar/2016-12-21/Mysql/to8to.to8to_dw_worker_oper_2016-12-21.txt'
                commond = '/usr/bin/dos2unix ' + file
                os.popen(commond)                                               # 特别注意：dos2unix 是将DOS格式的文本文件转成UNIX格式的，DOS以\r\n为断行、UNIX以\n为断行。  注意os.popen()与os.system()的区别
                table = f.replace('_' + str(self.lastDate) + '.txt', '').split('.')[1]

                #导入数据到hive
                try:
                    loadData = "load data local inpath '"+ file + "' overwrite into table " + self.dbName +'.'+ table + " partition (dt=" + self.lastDateFormat + ")"  #self.lastDateFormat = str(self.lastDate).replace('-', '')
                    print loadData
                    if hive.execute(loadData):
                        print table + ' ok'
                        logString = 'Load ' + f + ' to hive status:ok'
                        log.info(logString)
                except Exception,ex:
                    print ex
                    logString = 'Load ' + f + ' to hive status:fail'
                    log.debug(logString)
                    #MyAlarm.send_mail_sms(logString)

                #删除分区
                try:
                    if table not in self.reserve:
                        dropData = 'use to8to_rawdata;alter table ' + table + ' drop partition (dt=' +  self.last7DateFormat + ')'   # self.last7DateFormat = str(self.last7Date).replace('-', '') 删除7天前的分区
                        hive.execute(dropData)

                    #删除掉当天txt文件
                    #os.remove(file)
                except Exception,ex:
                    pass
        try:
            rmCmd = 'rm -rf ' + self.delPath     # 删除当天的分区 self.doPath = '/data1/bi/platform/tar/2016-12-21/Mysql/'
            os.system(rmCmd)
        except Exception,ex:
            pass

if __name__ == '__main__':
    date = MyTime.get_date(1)      #自动取昨天日期
    if len(sys.argv) == 2:
        date = sys.argv[1]
    hive = toHive(date)
    if hive.tarFile():             #解压tar.gz文件
        hive.cutTxt()              #替换隐私信息为null
        hive.txtToHive()           #替换后的txt文件存入hive
    #os.system('python /data1/bi/platform/scripts/BI/BISub/bi_yewu_caiwu_argv.py ' + date)
    #os.system('python /data1/bi/platform/scripts/BI/BISub/bi_zxgs_yunyingjibie.py ' + date)

