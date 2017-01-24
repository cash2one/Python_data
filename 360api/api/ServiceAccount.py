#!/usr/bin/env python
#-*- coding: utf-8 -*-
import os,sys
import time
import json
import ConfigParser
import requests
requests.packages.urllib3.disable_warnings()
import random
import datetime
from cube import MyLog

sys.path.append("/home/lincoln.hao/module")
import function
import ServiceCore

log = MyLog.MyLog(path='/home/lincoln.hao/log/', name='GetData', type='360Operate', level='DEBUG')

class Account:
    def __init__(self, accountId, accessToken, sessionToken, cookies, startDate, endDate):
        self.accessToken = accessToken
        self.sessionToken = sessionToken
        self.cookies = cookies
        #init request header
        init = ServiceCore.AccountLogin(accountId)
        self.header = init.header
        self.header['sessionToken'] = self.sessionToken
        self.header['accessToken'] = self.accessToken
        self.format = init.format
        self.dayBefore = int(init.dayBefore)
        self.startDate = startDate
        self.endDate = endDate

    #account.getCampaignIdList,查询用户所有的计划id
    def get_all_plan_id(self):
        serverToken = function.random_server_token()
        reqHeader = self.header
        reqHeader['Referer'] = 'api.e.360.cn'
        reqHeader['serveToken'] = serverToken
        reqUrl = 'https://api.e.360.cn/2.0/account/getCampaignIdList?format=' + self.format
        req = requests.get(reqUrl, headers=reqHeader, cookies=self.cookies, verify=False)

        if(req.status_code == 200):
            req.encoding = 'utf-8'
            data = json.loads(req.text)
            campaignIdList = data['campaignIdList']
            return campaignIdList
        else:
            return False

    #campaign.getInfoByIdList,批量获取该推广计划的信息
    #传入参数为组id，可为单个元素的list
    def get_plan_info(self, planIdList):
        planIdList = json.dumps(planIdList)
        serverToken = function.random_server_token()
        reqHeader = self.header
        reqHeader['Referer'] = 'api.e.360.cn'
        reqHeader['serveToken'] = serverToken
        postData = {
            'format': self.format,
            'idList': planIdList
        }

        reqUrl = 'https://api.e.360.cn/2.0/campaign/getInfoByIdList'
        req = requests.post(reqUrl, data= postData, headers = reqHeader, cookies = self.cookies, verify = False)

        if(req.status_code == 200):
            req.encoding = 'utf-8'
            data = json.loads(req.text)
            try:
                planInfoList = data['campaignList']
                return planInfoList
            except KeyError:
                return False
        else:
            return False

    #group.getIdListByCampaignId,根据推广计划id查询所有或满足条件的广告组id
    def get_group_by_plan(self, planId):
        serverToken = function.random_server_token()
        reqHeader = self.header
        reqHeader['Referer'] = 'api.e.360.cn'
        reqHeader['serveToken'] = serverToken
        lastDateTime = str(datetime.date.today() - datetime.timedelta(days = self.dayBefore)) + ' 00:00:00'

        reqUrl = 'https://api.e.360.cn/2.0/group/getIdListByCampaignId?format=' + self.format + \
                 '&campaignId=' + str(planId)
        req = requests.get(reqUrl, headers = reqHeader, cookies = self.cookies, verify = False)

        if(req.status_code == 200):
            req.encoding = 'utf-8'
            data = json.loads(req.text)
            groupIdList = data['groupIdList']
            return groupIdList
        else:
            return False

    #group.getInfoByIdList 根据groupId查询推广组的信息
    def get_group_info(self, groupIdList):
        groupIdList = json.dumps(groupIdList)
        serverToken = function.random_server_token()
        reqHeader = self.header
        reqHeader['Referer'] = 'api.e.360.cn'
        reqHeader['serveToken'] = serverToken
        postData = {
            'format' : self.format,
            'idList' : groupIdList
        }

        reqUrl = 'https://api.e.360.cn/2.0/group/getInfoByIdList'
        req = requests.post(reqUrl, data = postData, headers = reqHeader, cookies = self.cookies, verify = False)
        if(req.status_code == 200):
            req.encoding = 'utf-8'
            data = json.loads(req.text)
            try:
                groupList = data['groupList']
                return groupList
            except KeyError:
                return False
        else:
            return False

    #返回推广组ID的关键字ID列表
    def get_keyword_by_group(self, groupId):
        serverToken = function.random_server_token()
        reqHeader = self.header
        reqHeader['Referer'] = 'api.e.360.cn'
        reqHeader['serveToken'] = serverToken

        reqUrl = 'https://api.e.360.cn/2.0/keyword/getIdListByGroupId?format=' + self.format + \
                 '&groupId=' + str(groupId)
        req = requests.get(reqUrl, headers = reqHeader, cookies = self.cookies, verify = False)

        if(req.status_code == 200):
            req.encoding = 'utf-8'
            data = json.loads(req.text)
            keywordIdList = data['keywordIdList']
            return keywordIdList
        else:
            return False

    #keyword.getInfoByIdList #获取keyword属性
    def get_keyword_info(self, keywordIdList):
        keywordId = json.dumps(keywordIdList)
        serverToken = function.random_server_token()
        reqHeader = self.header
        reqHeader['Referer'] = 'api.e.360.cn'
        reqHeader['serveToken'] = serverToken
        postData = {
            'format' : self.format,
            'idList' : keywordId
        }

        reqUrl = 'https://api.e.360.cn/2.0/keyword/getInfoByIdList'
        req = requests.post(reqUrl, data = postData, headers = reqHeader, cookies = self.cookies, verify = False)

        if(req.status_code == 200):
            req.encoding = 'utf-8'
            data = json.loads(req.text)
            keywordLists = data['keywordList']
            return keywordLists
        else:
            return False

    #report.keywordCount,关键词报告记录数量
    #返回值{u'totalPage': 5, u'totalNumber': u'4516'}
    def get_keyword_count(self, level = 'account', idList = None):
        serverToken = function.random_server_token()
        reqHeader = self.header
        reqHeader['Referer'] = 'api.e.360.cn'
        reqHeader['serveToken'] = serverToken
        postData = {
            'format' : self.format,
            'startDate' : self.startDate,
            'endDate' : self.endDate,
            'level' : level,
            'idList' : idList
        }

        if(level == 'account'):
            del postData['idList']

        reqUrl = 'https://api.e.360.cn/2.0/report/keywordCount'
        req = requests.post(reqUrl, data = postData, headers = reqHeader, cookies = self.cookies, verify = False)

        if(req.status_code == 200):
            req.encoding = 'utf-8'
            data = json.loads(req.text)
            return data
        else:
            return False


    #report.keyword,生成关键词报告
    #返回值{totalCost,keyword,views,campaignId,groupId,avgPosition,keywordId,date,clicks}
    def get_keyword_report(self, level = 'account', idList = None, page = 1):
        idList = json.dumps(idList)
        serverToken = function.random_server_token()
        reqHeader = self.header
        reqHeader['Referer'] = 'api.e.360.cn'
        reqHeader['serveToken'] = serverToken
        postData = {
            'format' : self.format,
            'startDate' : self.startDate,
            'endDate' : self.endDate,
            'level' : level,
            'idList' : idList,
            'page' : page
        }

        if(level == 'account'):
            del postData['idList']

        reqUrl = 'https://api.e.360.cn/2.0/report/keyword'
        req = requests.post(reqUrl, data=postData, headers=reqHeader, cookies=self.cookies, verify=False)

        if(req.status_code == 200):
            req.encoding = 'utf-8'
            data = json.loads(req.text)
            keywordIdList = data['keywordList']
            return keywordIdList
        else:
            return False

    #keyword.update #更新keyword属性 #keywordIdList必须小于500个
    def set_keywords_url(self, keywordIdList, planName, groupName):
        keywordIdList = json.dumps(keywordIdList)
        serverToken = function.random_server_token()
        reqHeader = self.header
        reqHeader['Referer'] = 'api.e.360.cn'
        reqHeader['serveToken'] = serverToken

        #先请求获取keyword原始信息
        postData = {
            'format' : self.format,
            'idList' : keywordIdList
        }

        reqUrl = 'https://api.e.360.cn/2.0/keyword/getInfoByIdList'
        req = requests.post(reqUrl, data = postData, headers = reqHeader, cookies = self.cookies, verify = False)
        if(req.status_code == 200):
            req.encoding = 'utf-8'
            data = json.loads(req.text)
            keywordList = data['keywordList']
            updateList = list()
            for keyword in keywordList:
                destinationUrl = keyword['destinationUrl']
                baseUrl = destinationUrl.split(r'&utm_content=')[0]
                utm_content = '&utm_content=' + groupName
                utm_campaign = '&utm_campaign=' + planName
                keyword['url'] = baseUrl + utm_content + utm_campaign

                #删除掉不必要的信息
                # print keyword['word']
                # print keyword['groupId']
                # print keyword['destinationUrl']
                del keyword['status']
                del keyword['updateTime']
                del keyword['word']
                del keyword['addTime']
                del keyword['price']
                del keyword['groupId']
                del keyword['matchType']
                del keyword['destinationUrl']
                #更新updateList
                updateList.append(keyword)

            #格式化
            updateList = str(json.dumps(updateList))

            serverToken = function.random_server_token()
            updateHeader = self.header
            updateHeader['Referer'] = 'api.e.360.cn'
            updateHeader['serveToken'] = serverToken
            updateData = {
                'format' : self.format,
                'keywords' : updateList
            }

            updateUrl = 'https://api.e.360.cn/2.0/keyword/update'
            update = requests.post(updateUrl, data = updateData, headers = updateHeader, cookies = self.cookies, verify = False)
            if(update.status_code == 200):
                update.encoding = 'utf-8'
                data = json.loads(update.text)
                affectedRecords = data['affectedRecords']
                failKeywordIds = data['failKeywordIds']
                result = {
                    'affectedRecords' : affectedRecords,
                    'failKeywordIds' : failKeywordIds
                }
                return result
            else:
                return False
        else:
            return False


    #测试：获取城市信息
    #返回值{u'totalPage':5, u'totalNumber': u'4516'}
    def get_city_count(self):
        serverToken = function.random_server_token()
        reqHeader = self.header
        reqHeader['Referer'] = 'api.e.360.cn'
        reqHeader['serveToken'] = serverToken
        postData = {
            'format': self.format,
            'startDate': self.startDate,
            'endDate': self.endDate
            }
        reqUrl = 'https://api.e.360.cn/2.0/report/cityCount'
        req = requests.post(reqUrl, data=postData, headers=reqHeader, cookies=self.cookies, verify=False)
        if (req.status_code == 200):
            req.encoding = 'utf-8'
            data = json.loads(req.text)
            return data
        else:
            return False


    #report.city,生成关键词报告
    #返回值{cityName, groupId, campaignId, groupName, campaignName, clicks, views,totalCost,platform, date}
    def get_city_report(self, page=1):
        serverToken = function.random_server_token()
        reqHeader = self.header
        reqHeader['Referer'] = 'api.e.360.cn'
        reqHeader['serveToken']=serverToken
        postData = {
            'format':self.format,
            'startDate':self.startDate,
            'endDate':self.endDate
              }
        reqUrl = 'https://api.e.360.cn/2.0/report/city'
        req = requests.post(reqUrl, data=postData, headers=reqHeader, cookies=self.cookies, verify=False)
        if(req.status_code == 200):
            req.encoding = 'utf-8'
            data = json.loads(req.text)
            cityList = data['cityList']
            return cityList
        else:
            return False



#根据输入的类型('plan' or 'group'), 对应的id
def set_keyword_url(type, id, accountId, startDate, endDate):
    accessToken = ''
    sessionToken = ''
    cookies = ''

    #初始化账户信息
    account = ServiceCore.AccountLogin(accountId)
    loginData = account.get_login_info()
    if(loginData):
        accessToken = loginData['accessToken']
        sessionToken = loginData['sessionToken']
        cookies = loginData['cookies']

    #初始化更新数据方法
    client = Account(accountId, accessToken, sessionToken, cookies, startDate, endDate)

    #根据计划id获取组id
    if(type == 'plan'):

        planInfo = client.get_plan_info([id])
        planName = planInfo[0]['name']
        print '----------------------------------------------------------------'
        print planName

        groupIdList = client.get_group_by_plan(id)
        print groupIdList
        for groupId in groupIdList:
            groupInfo = client.get_group_info([groupId])
            groupName = groupInfo[0]['name']
            print groupName

            keywordIdList = client.get_keyword_by_group(groupId)

            for i in range(0, len(keywordIdList), 400):
                keywordList = keywordIdList[i:i+400]
                client.set_keywords_url(keywordList, planName, groupName)

            time.sleep(5)

    elif(type == 'group'):

        groupInfo = client.get_group_info([id])
        groupName = groupInfo[0]['name']

        planId = groupInfo[0]['campaignId']
        planInfo = client.get_plan_info([planId])
        planName = planInfo[0]['name']

        keywordIdList = client.get_keyword_by_group(id)

        for i in range(0, len(keywordIdList), 400):
            keywordList = keywordIdList[i:i+400]
            client.set_keywords_url(keywordList, planName, groupName)

#批量设置所有关键字的url，不要轻易使用
def set_all_url(accountId, startDate, endDate):
    accessToken = ''
    sessionToken = ''
    cookies = ''

    #初始化账户信息
    account = ServiceCore.AccountLogin(accountId)
    loginData = account.get_login_info()
    if(loginData):
        accessToken = loginData['accessToken']
        sessionToken = loginData['sessionToken']
        cookies = loginData['cookies']

    #初始化更新数据方法
    client = Account(accountId, accessToken, sessionToken, cookies, startDate, endDate)

    planIdList = client.get_all_plan_id()
    for planId in planIdList:
        set_keyword_url('plan', planId, accountId, startDate, endDate)
        time.sleep(5)

#更新计划信息到数据库
def update_plan_info(accountId, startDate, endDate):
    accessToken = ''
    sessionToken = ''
    cookies = ''

    #初始化账户信息
    account = ServiceCore.AccountLogin(accountId)
    loginData = account.get_login_info()
    if(loginData):
        accessToken = loginData['accessToken']
        sessionToken = loginData['sessionToken']
        cookies = loginData['cookies']

    #初始化更新数据方法
    client = Account(accountId, accessToken, sessionToken, cookies, startDate, endDate)

    planIdList = client.get_all_plan_id()           #lincoln 重要
    planInfoAll = client.get_plan_info(planIdList)  #lincoln 重要

    #初始化插入数据库方法
    dbConnect = ServiceCore.DBConnect()

    #清空sem_360_plan_info表
    #dbConnect.init_plan_info()
    if(planInfoAll):
        for planInfo in planInfoAll:
            id = planInfo['id']
            name = planInfo['name']
            updateTime = planInfo['updateTime']
            status = planInfo['status']
            dbConnect.replace_plan_info(id, name, updateTime, status)

#更新组信息
def update_group_info(accountId, startDate, endDate):
    try:
        #初始化账户信息
        accessToken = ''
        sessionToken = ''
        cookies = ''
        account = ServiceCore.AccountLogin(accountId)
        accountName = account.nickName                 # nickname = 彬讯科技
        loginData = account.get_login_info()
        if(loginData):
            accessToken = loginData['accessToken']
            sessionToken = loginData['sessionToken']
            cookies = loginData['cookies']
        else:
            return False

        #初始化更新数据方法
        client = Account(accountId, accessToken, sessionToken, cookies, startDate, endDate)
        planIdList = client.get_all_plan_id()
        planInfoAll = client.get_plan_info(planIdList)

        db = ServiceCore.DBConnect()
        #db.init_group_stat()
        if(planInfoAll):
            for plan in planInfoAll:
                resultList = list()
                planId = plan['id']
                planName = plan['name'].encode('utf-8')
                groupList = client.get_group_by_plan(planId)
                groupInfoAll = client.get_group_info(groupList)
                if(groupInfoAll):
                    for group in groupInfoAll:
                        groupId = group['id']
                        groupName = group['name'].encode('utf-8')

                        tempTuple = (accountName, planId, planName, groupId, groupName)
                        resultList.append(tempTuple)
                    db.replace_group_stat(resultList)     #导入数据库
        logString = 'Update group info status:ok'
        log.info(logString)
        return True
    except Exception, e:
        logString = 'Update group info status:fail -' + str(e)
        log.debug(logString)
        print e
        return False


#获取关键词展现量,点击量，价格
def update_keyword_info(accountId, startDate, endDate):
    #初始化账户信息
    accessToken = ''
    sessionToken = ''
    cookies = ''
    account = ServiceCore.AccountLogin(accountId)
    accountName = account.nickName
    loginData = account.get_login_info()
    if(loginData):
        accessToken = loginData['accessToken']
        sessionToken = loginData['sessionToken']
        cookies = loginData['cookies']
    else:
        return False

    #初始化更新数据方法
    client = Account(accountId, accessToken, sessionToken, cookies, startDate, endDate)
    count = client.get_keyword_count(level='account')

    pages = 1
    try:
        pages = int(count['totalPage']) + 1
    except KeyError:
        print '更新关键词属性失败'
        logString = 'GET 360 operator account_' + str(accountId) + ' totalPage status:fail'
        log.debug(logString)
        return False

    #初始化插入数据库方法
    db = ServiceCore.DBConnect()

    try:
        for page in range(1, pages):
            resultList = list()
            data = client.get_keyword_report(level = 'account', page=page)
            for v in data:
                #totalCost,keyword,views,campaignId,groupId,avgPosition,keywordId,date,clicks
                campaignId = v['campaignId']
                groupId = v['groupId']
                keywordId = v['keywordId']
                keyword = v['keyword'].encode('utf-8')
                views = v['views']
                clicks = v['clicks']
                totalCost = v['totalCost']
                avgPosition = v['avgPosition']
                platform = v['type']
                tempTuple = (accountName,campaignId,groupId,keywordId,keyword,views,clicks,totalCost,avgPosition,platform)
                resultList.append(tempTuple)
            db.insert_keyword_stat(resultList)
        logString = 'Update keyword info status:ok'
        log.info(logString)
        return True
    except Exception, ex:
        logString = 'Update keyword info status:fail -' + str(ex)
        log.debug(logString)
        print ex
        return False



#测试
#获取城市信息
def update_city_info(accountId, startDate, endDate):
    #初始化账户信息
    accessToken = ''
    sessionToken = ''
    cookies = ''
    account = ServiceCore.AccountLogin(accountId)
    accountName = account.nickName
    loginData = account.get_login_info()
    if(loginData):
        accessToken = loginData['accessToken']
        sessionToken = loginData['sessionToken']
        cookies = loginData['cookies']
    else:
        return False
    #初始化更新数据方法
    client = Account(accountId, accessToken, sessionToken, cookies, startDate, endDate)
    count = client.get_city_count()
    print '=====================================================' + str(count)
    pages =1

    try:
        pages = int(count['totalPage']) + 1
        print '=====================================================' + str(pages)
    except KeyError:
        print '更新城市属性失败'
        logString = 'GET 360 operator account_' + str(accountId) + ' totalPage status:fail'
        log.debug(logString)
        return False
    #初始化插入数据库方法
    db = ServiceCore.DBConnect()

    try:
        for page in range(1,pages):
            resultList = list()
            data = client.get_city_report(page=page)
            for v in data:
                stat_date = startDate.replace('-', '')
                cityName = v['cityName'].encode('utf-8')
                provinceName = v['provinceName'].encode('utf-8')
                groupId = v['groupId']
                campaignId = v['campaignId']
                groupName = v['groupName'].encode('utf-8')
                campaignName = v['campaignName'].encode('utf-8')
                clicks = v['clicks']
                views = v['views']
                totalCost = v['totalCost']
                platform='computer' #v['type']
                tempTuple = (stat_date,accountName,cityName,provinceName,groupId,campaignId,groupName,campaignName,clicks,views,totalCost,platform)
                resultList.append(tempTuple)
            db.insert_city_stat(resultList)
        logString = 'Update city info status:ok'
        log.info(logString)
        return True
    except Exception,ex:
        logString = 'Update city info status:fail ' + str(ex)
        log.debug(logString)
        print ex
        return False

