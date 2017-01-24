# -*- coding:utf-8 -*-
import time
import random

#保存每一步的cookie，rCookies为新增cookie的字典，cookies为返回新的字典
def add_cookie(cookies, rCookies):
    for cookie in rCookies:
        key = ((str(cookie).split('=')[0]).split(' '))[1]
        value = ((str(cookie).split('=')[1]).split(' '))[0]
        cookies[key] = value
    return cookies

def random_server_token():
    token = str(random.randint(100000,999999)) + str(int(time.time()))
    return token
