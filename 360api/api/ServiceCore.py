#!/usr/bin/env python
#-*- coding: utf-8 -*-
import os,sys
import time
import json
import ConfigParser
import requests
requests.packages.urllib3.disable_warnings()
import random
import MySQLdb
from cube import MyLog

sys.path.append("/home/lincoln.hao/module")
import function
log = MyLog.MyLog(path='/home/lincoln.hao/log/', name='GetData', type='360', level='DEBUG')

basePath = '/home/lincoln.hao/module/'
# basePath = 'F:\project\Sem360Operate' + os.sep


class AccountLogin:
    def __init__(self, account_id):
        global basePath
        self.basePath = basePath
        self.config = ConfigParser.ConfigParser()
        self.config.readfp(open(self.basePath + 'config.ini', 'rb'))

        #获取配置文件的账号
        self.accountNameId = 'account_' + str(account_id)

        self.nickName = self.config.get(self.accountNameId, 'nickName')
        self.userName = self.config.get(self.accountNameId, 'userName')
        self.passWord = self.config.get(self.accountNameId, 'passWord')
        self.apiKey = self.config.get(self.accountNameId, 'apiKey')
        self.apiSecret = self.config.get(self.accountNameId, 'apiSecret')
        self.dayBefore = self.config.get('type', 'dayBefore')
        self.appId = 'to8to.1.0'
        self.format = self.config.get('type', 'format')

        #set header
        self.header = {
            #360 header
            'apiKey': self.apiKey,
            'appId': self.appId,
            'sessionToken': '',
            'accessToken': '',
            'serveToken': '',
            #agent header
            'User-Agent': 'User-Agent: Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)',
            'Connection': 'keep-alive',
            'Host': 'e.360.cn',
            'Referer': 'www.360.cn',
            'Cache-Control': 'max-age=0',
            'Accept-Language': 'zh-CN',
            'Accept-Encoding': 'zh-CN,zh;q=0.8,en;q=0.6',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }

    def login(self):
        print self.nickName
        try:
            userName = self.userName.encode('utf-8')
        except Exception:
            userName = self.userName

        postData = {
            'username': userName,
            'passwd': self.passWord.encode('utf-8')
        }

        loginHeader = self.header
        loginHeader['serveToken'] = str(random.randint(100000,999999)) + str(int(time.time()))
        del loginHeader['sessionToken']
        del loginHeader['accessToken']
        del loginHeader['appId']

        postUrl = 'https://api.e.360.cn/account/clientLogin?format=json'

        try:
            req = requests.post(postUrl, data=postData, headers=loginHeader, verify=False)
            print userName
            print req.headers
            print req.text
        except Exception, ex:
            print ex
            logString = 'LOGIN 360 account_' + self.accountNameId + ' operator requests status:fail'
            log.info(logString)
            return False

        cookies = dict()
        if req.status_code == 200:
            try:
                req.encoding = 'utf-8'
                textJson = json.loads(req.text)

                #登录失败
                if textJson.has_key('failures'):
                    print 'login fail'
                    return False

                accessToken = textJson['accessToken']
                sessionToken = textJson['sessionToken']
                cookies = function.add_cookie(cookies, req.cookies)
                loginTime = int(time.time())

                self.config.set(self.accountNameId, 'cookies', cookies)
                self.config.set(self.accountNameId, 'loginTime', loginTime)
                self.config.set(self.accountNameId, 'accessToken', accessToken)
                self.config.set(self.accountNameId, 'sessionToken', sessionToken)
                self.config.write(open(self.basePath + 'config.ini', 'w'))
                logString = 'LOGIN 360 account_' + self.accountNameId + ' operator requests status:ok'
                log.info(logString)
                return True
            except Exception, ex:
                print ex
                logString = 'LOGIN 360 account_' + self.accountNameId + ' operator status:fail'
                log.info(logString)
                return False
        else:
            return False

    def get_login_info(self):
        loginTimeSet = self.config.get(self.accountNameId, 'loginTime')    #accountNameId = 'account_1'  logintime = 1484189405
        loginTimeDiff = int(time.time()) - int(loginTimeSet)
        #判断登陆时间差是否大于10小时,大于重新登陆
        if loginTimeDiff > 35000:
            self.login()
        else:
            pass

        data = dict()
        data['accessToken'] = self.config.get(self.accountNameId, 'accessToken')
        data['sessionToken'] = self.config.get(self.accountNameId, 'sessionToken')
        data['cookies'] = eval(str(self.config.get(self.accountNameId, 'cookies')))

        return data


class DBConnect:
    def __init__(self):
        global basePath
        #Init account and login
        config = ConfigParser.ConfigParser()
        config.readfp(open(basePath + 'config.ini', 'rb'))   #读取文件，转为类似字典类的格式

        dbHost = config.get('db', 'host')
        dbPort = int(config.get('db', 'port'))
        dbUser = config.get('db', 'user')
        dbPassWD = config.get('db', 'passwd')
        self.dbName = config.get('db', 'dbName')  # dbName = cube
        dbCharset = config.get('db', 'charset')

        conn = MySQLdb.connect(host=dbHost, port = dbPort, user = dbUser, passwd = dbPassWD, db = self.dbName, charset = dbCharset)
        cursor = conn.cursor()
        self.conn = conn
        self.cursor = cursor

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def init_plan_info(self):
        #清空360推广计划信息表
        sql_init = 'TRUNCATE TABLE ' + self.dbName + '.sem_360_plan_info'
        self.cursor.execute(sql_init)
        self.conn.commit()

    def insert_plan_info(self, id, name, updateTime, status):
        #插入新数据
        sql_param = (id, name, updateTime, status)
        sql_insert = 'insert into ' + self.dbName + '.sem_360_plan_info(id, name, updateTime, status) values(%s, %s, %s, %s)'
        self.cursor.execute(sql_insert,sql_param)
        self.conn.commit()

    def update_plan_info(self, id, name, updateTime, status):
        #更新数据
        sql_update = "update "+self.dbName+".sem_360_plan_info set name='"+name+"',updateTime='"+updateTime+"',status='"+status+"' where id="+str(id)
        self.cursor.execute(sql_update)
        self.conn.commit()

    def replace_plan_info(self, id, name, updateTime, status):
        #插入新数据
        sql_param = (id, name, updateTime, status)
        sql_replace = 'replace into ' + self.dbName + '.sem_360_plan_info(id, name, updateTime, status) values(%s, %s, %s, %s)'
        self.cursor.execute(sql_replace,sql_param)
        self.conn.commit()

    def get_plan_info(self):
        sql_get = 'select id,name,updateTime,status from ' + self.dbName + '.sem_360_plan_info order by updateTime desc'
        self.cursor.execute(sql_get)
        result = self.cursor.fetchall()
        return result

    #清空关键词信息表
    def init_keyword_stat(self):
        sql_init = 'TRUNCATE TABLE ' + self.dbName + '.sem_360_keyword_info'
        self.cursor.execute(sql_init)
        self.conn.commit()

        sql_init_del = 'TRUNCATE TABLE ' + self.dbName + '.sem_360_keyword'
        self.cursor.execute(sql_init_del)
        self.conn.commit()

    #插入所有关键词信息
    def insert_keyword_stat(self, resultList):
        sql_insert = 'insert into ' + self.dbName + '.sem_360_keyword_info(accountName, campaignId, groupId, keywordId, keyword, views, clicks, totalCost, avgPosition, platform) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        self.cursor.executemany(sql_insert, resultList)
        self.conn.commit()

    #清空关键词信息表
    def init_group_stat(self):
        sql_init = 'TRUNCATE TABLE ' + self.dbName + '.sem_360_group_info'
        self.cursor.execute(sql_init)
        self.conn.commit()

    #插入推广计划和推广组对应关系信息
    def insert_group_stat(self, accountName, planId, planName, groupId, groupName):
        sql_param = (accountName, planId, planName, groupId, groupName)
        sql_insert = 'insert into ' + self.dbName + '.sem_360_group_info(accountName, planId, planName, groupId, groupName) values(%s,%s,%s,%s,%s)'
        self.cursor.execute(sql_insert, sql_param)
        self.conn.commit()

    def replace_group_stat(self, resultList):
        sql_replace = 'replace into ' + self.dbName + '.sem_360_group_info(accountName, planId, planName, groupId, groupName) values(%s,%s,%s,%s,%s)'
        self.cursor.executemany(sql_replace, resultList)   #executemany是批量插入，resultList包含多条记录
        self.conn.commit()
        #replace into 插入的数据表必须有主键或唯一索引，否则会导致重复数据

    def update_group_stat(self, planId,planName, groupId, groupName):
        sql_update = "update " + self.dbName + ".sem_360_group_info set planId="+str(planId)+",planName='"+planName+"',groupName='"+groupName+"' where groupId="+str(groupId)
        self.cursor.execute(sql_update)
        self.conn.commit()

    def init_city_stat(self):
        sql_init = 'TRUNCATE TABLE ' + self.dbName + '.sem_360_diyu'
        self.cursor.execute(sql_init)
        self.conn.commit()

    def insert_city_stat(self,resultList):
        sql_insert = 'insert into ' + self.dbName + '.sem_360_diyu(stat_date,accountName,cityName,provinceName,groupId,campaignId,groupName,campaignName,clicks,views,totalCost,platform) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        self.cursor.executemany(sql_insert, resultList)
        self.conn.commit()



