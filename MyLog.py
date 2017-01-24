# -*- coding:utf-8 -*-
__author__ = 'fremcode@gmail.com'

import logging

import datetime


class MyLog():
    def __init__(self, path='', name='MyLog', type='default', level='DEBUG'):
        """
        打印日志(支持滚动)
        :param path: 生成日志文件绝对路径
        :param name: 日志文件名xxx,生成日志文件名格式xxx_2015-01-09.log
        :param type: 日志记录类型名称
        :param level: 日志级别(从高到低,低于该级别将被忽略)CRITICAL > ERROR > WARNING > INFO > DEBUG > NOTSET:
        """
        self.path = path
        self.name = name
        logLevel = getattr(logging, level)
        self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s') #设置格式，例如生成的日志：  2017-01-10 13:58:26,500 - MySqlAll - INFO - tar bi_mysql_all_2017-01-09.tar.gz-status:ok
        self.logger = logging.getLogger(type)  #创建一个logger, 例如：type='MySqlAll'
        self.logger.setLevel(logLevel)         #设置logger 的级别

    def debug(self, msg):
        now_date = datetime.date.today()
        log_file = self.path + self.name + '_' + str(now_date) + '.log'
        fp = logging.FileHandler(log_file)
        fp.setLevel(logging.DEBUG)
        fp.setFormatter(self.formatter)
        self.logger.addHandler(fp)
        self.logger.debug(msg)                #注意这里的debug与下面的模块不一样
        self.logger.removeHandler(fp)

    def info(self, msg):
        now_date = datetime.date.today()
        log_file = self.path + self.name + '_' + str(now_date) + '.log'   #设置日志文件名字
        fp = logging.FileHandler(log_file)                                #创建日志文件
        fp.setLevel(logging.DEBUG)                                        #设置 fp 的级别，小于DEBUG的级别都不能输出，大于等于DEBUG的级别都能输出
        fp.setFormatter(self.formatter)                                   #设置日志输出格式
        self.logger.addHandler(fp)                                        #把 self.logger 添加至 fp ,   self.logger = logging.getLogger(type)。注意：此时logger的级别是DEBUG，fp的级别是DEBUG。当logger的级别>=fp的级别,日志才会输出
        self.logger.info(msg)
        self.logger.removeHandler(fp)

    def warning(self, msg):
        now_date = datetime.date.today()
        log_file = self.path + self.name + '_' + str(now_date) + '.log'
        fp = logging.FileHandler(log_file)
        fp.setLevel(logging.DEBUG)
        fp.setFormatter(self.formatter)
        self.logger.addHandler(fp)
        self.logger.warning(msg)
        self.logger.removeHandler(fp)

    def error(self, msg):
        now_date = datetime.date.today()
        log_file = self.path + self.name + '_' + str(now_date) + '.log'
        fp = logging.FileHandler(log_file)
        fp.setLevel(logging.DEBUG)
        fp.setFormatter(self.formatter)
        self.logger.addHandler(fp)
        self.logger.error(msg)
        self.logger.removeHandler(fp)

    def critical(self, msg):
        now_date = datetime.date.today()
        log_file = self.path + self.name + '_' + str(now_date) + '.log'
        fp = logging.FileHandler(log_file)
        fp.setLevel(logging.DEBUG)
        fp.setFormatter(self.formatter)
        self.logger.addHandler(fp)
        self.logger.critical(msg)
        self.logger.removeHandler(fp)


#测试方法
def test_except():
    log = MyLog('360')
    try:
        num = 2
        string = 'test' + num
    except Exception, e:
        log.info(e)


if __name__ == '__main__':
    test_except()
