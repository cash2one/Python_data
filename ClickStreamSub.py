#!/usr/bin/env python
# -*- coding:utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import os
import json
import codecs
import hashlib
import urllib
import time
import chardet
from cube import MyTool
from cube import MyTime
from cube import MyLog
from cube import MyHiveBin
from cube import MyAlarm
from multiprocessing import Process, Array, RLock

PROJECT_PATH = '/data1/bi/usertest/user/lincoln/data/jun/'
TAR_PATH = '/data1/bi/usertest/user/lincoln/data/jun/tar/'  #'/data1/bi/platform/tar/'
WORKERS = 23
BLOCK_SIZE = 0
FILE_TAR = None
FILE_TAR_RESULT = None
FILE_JSON_PATH = None
FILE_JSON_NAME = None
FILE_JSON_SIZE = 0
FILE_HIVE_PATH = None
FILE_HIVE_LIST = list()
HIVE_DB = 'to8to_tmp' #'to8to_rawdata'
HIVE_TABLE = 'clickstream_d' #'clickstream'
log = MyLog.MyLog(path=PROJECT_PATH + 'log' + os.sep, name='ClickStream', type='to8to', level='DEBUG')    #记录日志


def url_format(url):
    if url is False:
        return 'false'
    if url is True:
        return 'true'

    if '%' in url:
        url = urllib.unquote(url)          #注意，解码操作

    url = utf8_format(url)
        
    try:
        url = url.replace('\r\n', '')
        url = url.replace('\n', '')
        if url[0] == '/':                  #有一次，着陆页出现 http:///,就是在这做了处理
            url = url[1:]
    except Exception, ex:
        pass

    return url


def utf8_format(string):
    if string[:3] == codecs.BOM_UTF8:      #UTF-8不需要BOM来表明字节顺序，但可以用BOM来表明编码方式。字符”ZERO WIDTH NO-BREAK SPACE”的UTF-8编码是EF BB BF。所以如果接收者收到以EF BB BF开头的字节流，就知道这是UTF-8编码了。Windows就是使用BOM来标记文本文件的编码方式的。
        string = string[3:]

    encode_type = chardet.detect(str(string))   #chardet.detect 判断编码方式，返回一个字典，如：{'confidence': 1.0, 'encoding': 'ascii'}
    encode_type = encode_type['encoding']

    if encode_type == 'ascii':
        result = string.decode('ascii').encode('utf-8')  #decode的作用是将其他编码的字符串转换成unicode编码，encode的作用是将unicode编码转换成其他编码的字符串
    elif encode_type == 'gbk':
        result = string.decode('gbk').encode('utf-8')
    elif encode_type == 'gb2312' or encode_type == 'GB2312':
        try:
            result = string.decode('gbk').encode('utf-8')
        except UnicodeDecodeError:
            result = str(string)
    elif encode_type == 'ISO-8859-2':
        try:
            result = string.decode('gbk').encode('utf-8')
        except UnicodeDecodeError:
            result = string.decode('ISO-8859-2').encode('utf-8')
    elif encode_type == 'windows-1252':
        try:
            result = string.decode('gbk').encode('utf-8')
        except UnicodeDecodeError:
            result = string.decode('windows-1252').encode('utf-8')
    elif encode_type == 'IBM855':
        try:
            result = string.decode('gbk').encode('utf-8')
        except UnicodeDecodeError:
            result = string.decode('IBM855').encode('utf-8')
    elif encode_type == 'utf-8' or encode_type == 'ascii':
        result = str(string)
    else:
        try:
            result = string.decode(encode_type).encode('utf-8')
        except UnicodeDecodeError:
            result = str(string)
        except TypeError:
            result = str(string)
        except LookupError:
            result = str(string)

    return result





def data_parse(line):
    data_list = list()

    try:
        line_dict = json.loads(line)
    except Exception, ex:
        print str(ex)
        log.error('Json loads fail-' + str(ex) + str(line))
        return data_list

    user_id = ''
    #uid
    try:
        user_id = str(line_dict['uid'])
    except KeyError:
        pass

    cookie_id = ''
    #cid
    try:
        cookie_id = str(line_dict['cid'])
    except KeyError:
        pass

    session_id = ''
    #sid
    try:
        session_id = str(line_dict['sid'])
    except KeyError:
        pass

    user_location = ''
    #ul
    try:
        user_location = str(line_dict['ul'])
    except KeyError:
        pass

    ip_address = ''
    #ip
    try:
        ip_address = str(line_dict['ip'])
    except KeyError:
        pass

    os_version = ''
    #osv
    try:
        os_version = str(line_dict['osv'])
    except KeyError:
        pass

    os_type = ''
    #ost
    try:
        os_type = str(line_dict['ost'])
    except KeyError:
        pass

    product_name = ''
    #pn
    try:
        product_name = str(line_dict['pn'])
    except KeyError:
        pass

    product_version = ''
    #pv
    try:
        product_version = str(line_dict['pv'])
    except KeyError:
        pass

    user_agent = ''
    #ua
    try:
        user_agent = str(line_dict['ua'])
    except KeyError:
        pass

    explorer_version = ''
    #ev
    try:
        explorer_version = str(line_dict['ev'])
    except KeyError:
        pass

    sp_type = ''
    #st
    try:
        sp_type = str(line_dict['st'])
    except KeyError:
        pass

    network_type = ''
    #nt
    try:
        network_type = str(line_dict['nt'])
    except KeyError:
        pass

    leave_time = ''
    #lt
    try:
        leave_time = str(line_dict['lt'])
    except KeyError:
        pass

    visit_from = ''
    #vf
    try:
        visit_from = 'http://' + url_format(line_dict['vf'])
    except KeyError:
        pass

    device_type = ''
    #dt
    try:
        device_type = str(line_dict['dt'])
    except KeyError:
        pass

    device_id = ''
    #di
    try:
        device_id = str(line_dict['di'])
    except KeyError:
        pass

    display_solution = ''
    #ds
    try:
        display_solution = str(line_dict['ds'])
    except KeyError:
        pass

    for e in line_dict['e']:
        cv_id = ''
        #cid
        try:
            cv_id = str(e['ci'])
        except KeyError:
            pass

        visit_time = ''
        #vt
        try:
            visit_time = str(e['vt'])
        except KeyError:
            pass

        event_type = ''
        #et
        try:
            event_type = str(e['et'])
        except KeyError:
            pass

        event_name = ''
        #en
        try:
            event_name = str(e['en'])
        except KeyError:
            pass

        visit_resource = ''
        #vr
        try:
            visit_resource = 'http://' + url_format(e['vr'])
        except KeyError:
            pass

        parent_id = ''
        if event_type == '1':
            parent_id = str(hashlib.md5(cv_id + '-' + cookie_id).hexdigest())
        else:
            pass

        if event_type == '1':
            current_id = parent_id
        else:
            now_timestamp = str("%f " % time.time())[0:-4].replace('.', '')
            current_id = str(hashlib.md5(cv_id + '-' + cookie_id + '-' + session_id + now_timestamp).hexdigest())

        data_tuple = (
            user_id, cookie_id, session_id, user_location, ip_address, os_version, os_type, product_name,
            product_version, user_agent, explorer_version, sp_type, network_type, visit_time, leave_time, event_type,
            event_name, visit_from, visit_resource, device_type, device_id, display_solution, parent_id, current_id
        )
        data_list.append(data_tuple)

    return data_list






def set_file_path(date_arg):
    """
    设置文件路径
    :param date_arg: 传入日期参数
    :return: bool

    FILE_TAR       ：点击流原始文件：  /data1/bi/platform/rawdata/1009/UserEventLog2016-11-16.json.en.tar.gz
    FILE_JSON_PATH : 解压后文件路径    /data1/bi/platform/scripts/BI/ClickStream/decrypt/
    FILE_JSON_NAME : 解压后点击流名字  /data1/bi/platform/scripts/BI/ClickStream/decrypt/UserEventLog_to8to.2016-11-16.json
    FILE_HIVE_PATH ：                 /data1/bi/platform/tar/2016-11-16/ClickStream/
    """
    global FILE_TAR, FILE_TAR_RESULT, FILE_JSON_PATH, FILE_JSON_NAME, FILE_HIVE_PATH, FILE_HIVE_LIST
    FILE_TAR = '/data1/bi/usertest/user/lincoln/data/jun/src/UserEventLog' + date_arg + '.json.en.tar.gz'  #'/data1/bi/platform/rawdata/1009/UserEventLog' + date_arg + '.json.en.tar.gz'
    FILE_JSON_PATH = PROJECT_PATH + 'decrypt' + os.sep
    FILE_JSON_NAME = PROJECT_PATH + 'decrypt' + os.sep + 'UserEventLog_to8to.' + date_arg + '.json'   # '/data1/bi/platform/scripts/BI/ClickStream/'+ 'decrypt' + '/' + 'UserEventLog_to8to.' + '2016-11-16' + '.json'
    FILE_HIVE_PATH = TAR_PATH + date_arg + os.sep + 'ClickStream' + os.sep                            # '/data1/bi/platform/tar/' + '2016-11-16' +  '/' + 'ClickStream' + '/'
    os.system('mkdir -p ' + FILE_HIVE_PATH)                                                           # mkdir -p 需要创建上层目录，如果目录早已存在则不当作错误

    for pid in range(0, WORKERS):                                                                     # WORKERS = 23
        file_des = FILE_HIVE_PATH + 'ClickStream_to8to_' + str(pid) + '.txt'                          # 创建 一个包含 24个txt文件名的列表
        FILE_HIVE_LIST.append(file_des)                                                               #FILE_HIVE_LIST = list()




def del_file():
    global FILE_JSON_PATH
    os.system('rm -rf ' + FILE_JSON_NAME)                                                            #删除 /data1/bi/platform/scripts/BI/ClickStream/decrypt 下的文件  注意空格



def check_file():
    """
    检查json日志文件是否生成
    :return: bool;True,成功;False,失败
    """
    global FILE_TAR, FILE_JSON_PATH, FILE_JSON_NAME
    if MyTool.tar_file(FILE_TAR, FILE_JSON_PATH):         #特别注意。，这个应该是解压文件的 tar_file模块中  tar = tarfile.open('123.tar.gz')  tar.extractall()  从一个路径解压到另个路径解压文件
        #try:
        #    os.rename(FILE_TAR_RESULT, FILE_JSON_NAME)
        #except Exception, ex:
        #    log.error(str(ex))

        if os.path.isfile(FILE_JSON_NAME):
            return True
        else:
            log_str = FILE_JSON_NAME + " not exist!"
            log.error(log_str)
            return False
    else:
        log_str = "tar " + FILE_TAR + " status:fail!"
        log.error(log_str)
        return False


def get_file_size(file_src):
    """
    获取文件块大小
    :param file_src:
    :return:
    """
    global FILE_JSON_SIZE
    fp_src = open(file_src, 'r')         #file_src=/data1/bi/platform/scripts/BI/ClickStream/decrypt/UserEventLog_to8to.2016-11-16.json
    fp_src.seek(0, os.SEEK_END)          #用文件游标移动测量文件大小， os.SEEK_END=2 , file.seek(off,WHENCE=0) 文件中移动off个操作标记（文件指针），正往结束方向移动，负往开始方向移动。如果设定whence,就以whence设定的起始位为准，0代表从头开始，1代表当前位置，2代表文件最末尾位置。
    FILE_JSON_SIZE = fp_src.tell()
    fp_src.close()


def process_found(pid, array, read_lock):
    global FILE_JSON_SIZE, BLOCK_SIZE, FILE_JSON_NAME, FILE_HIVE_PATH, FILE_HIVE_LIST
    fp_src = open(FILE_JSON_NAME, 'rb')                                                    #FILE_JSON_NAME 读取 JSON 文件
    try:
        read_lock.acquire()           #将返回True
        begin = array[0]              #array是23个元素都为0的数组
        end = (begin + BLOCK_SIZE)    #BLOCK_SIZE每个文件的大小

        if begin >= FILE_JSON_SIZE:
            array[0] = begin
            raise Exception('end of file')

        if end < FILE_JSON_SIZE:
            fp_src.seek(end)          #又用到文件游标
            fp_src.readline()
            end = fp_src.tell()

        if end >= FILE_JSON_SIZE:
            end = FILE_JSON_SIZE

        array[0] = end
        print pid, '------------- ', begin, end
    except Exception, e:
        print e.__class__.__name__, str(e)
        return
    finally:
        read_lock.release()

    fp_src.seek(begin)
    pos = begin
    file_des = FILE_HIVE_PATH + 'ClickStream_to8to_' + str(pid) + '.txt'
    fp_des = open(file_des, 'wb')
    while pos < end:
        line = fp_src.readline()
        line_list = data_parse(line)                                                         #清洗开始
        if len(line_list) != 0:
            for line_tuple in line_list:
                try:
                    line_str = '\t'.join(line_tuple) + '\n'
                    fp_des.write(line_str)
                except Exception, ex:
                    print ex
                    log.error('join tuple fail-' + str(ex) + str(line))
        pos = fp_src.tell()

    fp_src.close()
    fp_des.close()
    FILE_HIVE_LIST.append(file_des)




def click_stream():
    global FILE_JSON_SIZE, BLOCK_SIZE, WORKERS, FILE_JSON_NAME
    get_file_size(FILE_JSON_NAME)                                            #获得文件大小
    BLOCK_SIZE = FILE_JSON_SIZE/WORKERS                                      #文件大小除以24，获得，每个 BLOCK_SIZE 的大小
    read_lock = RLock()                                                      # from multiprocessing import Process, Array, RLock
    array = Array('l', WORKERS)                                              #猜测，这里是要分为23个进程
    process = []
    for i in range(WORKERS):
        p = Process(target=process_found, args=[i, array, read_lock])       #lock:不使用锁时，不同进程的输出会混到一起
        process.append(p)                                                   #把文件分23个txt
    for i in range(WORKERS):
        process[i].start()
    for i in range(WORKERS):
        process[i].join()



def check_hive_file():
    global FILE_HIVE_LIST
    for file in FILE_HIVE_LIST:
        if os.path.isfile(file):
            continue
        else:
            return False
    return True



def load_file(date_arg): #导文件模块，主要考虑：要导的文件存在不存在？需要有删除计算当天的分区步骤，文件没导成功怎么办
    global WORKERS, FILE_HIVE_LIST, HIVE_DB, HIVE_TABLE                                 # 5.1 设置全局变量
    hive_status = check_hive_file()                                                     # 5.2 hive_status=True、False 检查是否存在文件
    if not hive_status:
        log_str = 'click stream file parse fail: file count not equal WORKERS'
        print MyTime.get_local_time(), '-------------- ' + log_str
        log.error(log_str)
        #MyAlarm.send_mail_sms(log_str)                                                  # 5.3 假如文件不存在，生成告警日志，并发送文件
    else:
        hive = MyHiveBin.HiveBin()      # 调用hive模块 模块在 com/hive/bin
        dt = date_arg.replace('-', '')  # "2016-11-30" 改变为 "20161130"
        hive.drop_partition(HIVE_DB, HIVE_TABLE, 'dt', dt)                              # 5.4 假如文件存在，删掉计算当天的分区       HIVE_DB= 'to8to_rawdata'   HIVE_TABLE='clickstream'

        partition_dict = {
            'dt': dt
        }

        for hive_file in FILE_HIVE_LIST:
            log_str = 'load file ' + hive_file + ' into hive begin'
            print MyTime.get_local_time(), '-------------- ' + log_str
            log.info(log_str)
            status = hive.load_file(HIVE_DB, HIVE_TABLE, hive_file, partition_dict)    # status 注意，导数据成功后将 状态 True、False 赋值给 status
            if status is False:
                log_str = 'Load file ' + hive_file + ' into hive status:fail; Click stream parse exit'
                log.error(log_str)
                #MyAlarm.send_mail_sms(log_str)                                         # 5.5 假如文件存在  但没导成功，将发送告警
                return False



def main(date_arg):
    global FILE_JSON_NAME, FILE_HIVE_PATH                                           ## 3.1、首先设置全局变量，FILE_JSON_NAME = None  FILE_HIVE_PATH = None
    set_file_path(date_arg)                                                         ## 3.2、设置文件路径 ,生成文件名列表 FILE_HIVE_LIST
    try:
        os.mkdir(FILE_HIVE_PATH)                                                    ## 3.3 创建目录  FILE_HIVE_PATH ：/data1/bi/platform/tar/2016-11-16/ClickStream/
    except Exception, ex:
        print str(ex)
        pass
    print MyTime.get_local_time(), '-------------- tar click stream file begin'     # from cube import MyTime
    log.info('tar click stream file begin')                                         # 写入日志  log = MyLog.MyLog(path='/data1/bi/platform/scripts/BI/ClickStream/log/', name='ClickStream', type='to8to', level='DEBUG')
    if check_file():                                                                ## 3.4 检查json日志文件是否生成
        print MyTime.get_local_time(), '-------------- tar click stream file success, then process work'
        log.info('tar click stream file success')
        log.info('click stream to8to process work')
        get_file_size(FILE_JSON_NAME)                                               ## 3.5 获得文件大小，这个有点多余
        click_stream()                                                              ## 3.6 开始清洗
        print MyTime.get_local_time(), '-------------- process success'
        log.info('click stream to8to process work success')



if __name__ == '__main__':
    date = MyTime.get_date(1)                                                       #1 、直接获取计算日期的当天时间 ：'2016-11-29',代表获取前一天的数据
    if len(sys.argv) == 2:
        date = sys.argv[1]                                                          #2 、假如脚本后面带 时间参数，就用参数（猜测这个是用来便于回滚脚本设置的）
    main(date)                                                                      #3 、清洗数据
    print MyTime.get_local_time(), '-------------- load all file into hive begin'   #4、 MyTime.get_local_time() 用来获取当前时间：'2016-11-30 19:41:16'
    #load_file(date)                                                                 #5、 导数据
    #clickstream_sjb_shell = 'python /data1/bi/platform/scripts/BI/ClickStream/ClickStreamSJB.py ' + date
    #os.system(clickstream_sjb_shell)
    #del_file()  #解析后可以删除源文件
