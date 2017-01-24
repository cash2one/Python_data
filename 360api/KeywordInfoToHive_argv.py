#!/usr/bin/env python
#-*- coding: utf-8 -*-
import os
import sys
import datetime
from cube import MyLog

sys.path.append("/home/lincoln.hao/module")
import ServiceCore
import ServiceAccount

log = MyLog.MyLog(path='/home/lincoln.hao/log/', name='ToHive', type='360Operate', level='DEBUG')

#生成新表，统一由/home/hadoop/scripts/BI/weixin脚本导入到hive
def union_keyword():

    db = ServiceCore.DBConnect()

    try:
        sql_init = 'TRUNCATE TABLE ' + db.dbName + '.sem_360_keyword'
        db.cursor.execute(sql_init)
        db.conn.commit()
    except Exception, ex:
        print ex

    sql_create = 'INSERT INTO ' + db.dbName +'.sem_360_keyword ' \
                 'SELECT b.planName,b.groupName,a.keyword,a.views,a.clicks,a.totalCost,a.avgPosition,b.accountName,a.platform ' \
                 'FROM ' + db.dbName + '.sem_360_keyword_info AS a LEFT JOIN ' + db.dbName + '.sem_360_group_info AS b ' \
                 'ON a.accountName=b.accountName and a.groupId=b.groupId'
    db.cursor.execute(sql_create)
    db.conn.commit()
    logString = 'Create table sem_360_keyword status:ok'
    log.info(logString)

if __name__ == '__main__':
    #更新今日数据前先清空昨天数据
    db = ServiceCore.DBConnect()   #db是一个类
    db.init_city_stat()  #先清空表
    #db.init_keyword_stat()    #清空关键词信息表 cube.sem_360_keyword_info

    #默认更新昨天数据
    startDate = endDate = str(datetime.date.today() - datetime.timedelta(days=1))
    #startDate = endDate = sys.argv[1]
    dt = startDate.replace('-', '')
    print startDate

    #遍历账户信息(account_1,account_2)
    account = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    for accountId in account:
        #更新推广组信息
        #print str(startDate) + 'update account_'+ str(accountId)+ ' group info begin!'
        #i = 0
        #while 1:
        #    i += 1
        #    if ServiceAccount.update_group_info(accountId, startDate, endDate) or i > 3: #重复登录次数多，账号会被封掉
        #        break

        #更新关键词信息
        #print str(startDate) + 'update account_' + str(accountId) + ' keyword info begin!'
        #k = 0
        #while 1:
        #    k += 1
        #    if ServiceAccount.update_keyword_info(accountId, startDate, endDate) or k > 3:
        #        break

        #跟新城市信息
        print str(startDate) + 'update account_' + str(accountId) + ' keyword info begin!'
        k = 0
        while 1:
            k += 1
            if ServiceAccount.update_city_info(accountId, startDate, endDate) or k > 3:
                break

    #关联关键词,计划名称,推广组等信息
    #union_keyword()
    #os.system('sh /var/frem/script/SEM/360/Sem360Operate/LoadTable360_argv.sh ' + dt)
