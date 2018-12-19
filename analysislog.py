import
multiprocessing

import logging

import
time,queue,random

from
multiprocessing.managers import BaseManager

from datetime
import datetime

import configparser

import os

import re

import cx_Oracle

import signal

from t_logdb import oracledbproces

from subprocess
import Popen, PIPE



conf = configparser.ConfigParser()

conf.read("conf.ini")

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.DEBUG)

dbhost = conf.get("db",
"dbhost")

port = int(conf.get("db", "port"))

dbname = conf.get("db",
"dbname")

username = conf.get("db",
"user")

password = conf.get("db",
"password")

bak_dir = conf.get("archive", "bak_dir")



def datetime2timestamp(dt, convert_to_utc=True):

''' Converts a datetime object
to UNIX timestamp in milliseconds. '''

start = datetime.datetime(1970, 1, 1)

if isinstance(dt, datetime.datetime):

if convert_to_utc:

dt = dt + datetime.timedelta(hours=-8)

timestamp = int((dt -
start).total_seconds() * 1000)

return timestamp

return



class QueueManager(BaseManager): pass



class analysis2db(multiprocessing.Process):

def conn(self):

while True:

try:

conf.read("conf.ini")

server = conf.get("queue", "queueserver")

port = int(conf.get("queue", "port"))

#anaqueue = conf.get("queue",
"anaqueue")

#ready2ana_queue =
conf.get("queue", "ready2ana_queue")

#QueueManager.register(anaqueue)

QueueManager.register('ready2ana_queue')

QueueManager.register('result_queue')

self.m =
QueueManager(address=(server,
port), authkey=b'abc')

self.m.connect()

#self.ready2ana_queue =
eval_r('self.m.'+ready2ana_queue+'()')

#self.result_queue =
eval_r('self.m.'+result_queue+'()')

self.ready2ana_queue=self.m.ready2ana_queue()

self.result_queue=self.m.result_queue()

break

except Exception as e:

logger.error(e)

def run(self):

self.conn()

self.db =
oracledbproces(dbhost, port, dbname, username, password)

logger.info("dbthread
start")

while True:

try:

logger.info("before:ready2ana_queue size is
{0}".format(self.ready2ana_queue.qsize()))

sfile = self.ready2ana_queue.get()

sfile_a = os.path.basename(sfile)

logger.info("receive
{0} ready2ana_queue size is {1}and
basename is {2}".format(sfile,self.ready2ana_queue.qsize(),sfile_a))

self.filename =
os.path.split(sfile)[1]

#if
os.path.isfile(sfile)==False:

#
self.result_queue.put({"name":sfile,"result":"ana"})

# logger.warn("{0}
notfind".format(sfile))

# continue

#if row[0]==1:

#
self.result_queue.put({"name":sfile,"result":"ana"})

# logger.info("{0} already
insert".format(sfile))

# continue

aa = ctilog(sfile)

aa.requestlog_list()

if
aa.inserdb()==self.filename:

logger.info("{0} already
insert".format(self.filename))

#self.result_queue.put({"name":sfile,"result":"ana"})#等待扫描result队列，删除ignorlist中的值

#logger.info("result_queue size
is {0}".format(self.result_queue.qsize()))

cmd = ("mv {0} {1}".format(sfile, bak_dir))#移动到bak文件夹

print(cmd)

a = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)

stdout, stderr = a.communicate()

except Exception as e:

logger.error(e)

time.sleep(5)

self.conn()



class ctilog():

def __init__(self, file):

self.file = file

self.request_list =
[]

self.__message =
''

self.__ctilogbegin =
time.time()

self.filename =
os.path.split(self.file)[1]

def requestlog_list(self):

with open(self.file,'rb') as f:

while True:

line = f.readline()

__message = str(line)

if line == b'':

break

if len(self.request_list) >1000:

break

a = re.search(r'(\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}).*(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}).*(POST
/api/v1/video/makeACDCall).*',
__message)

if a:

line_ip = a.group(1)

line_time = datetime.strptime(a.group(2), "%d/%b/%Y:%H:%M:%S").strftime('%Y-%m-%d %H:%M:%S')#日志中日期字符串转换为标准日期字符串

#line_time_str.strftime('%Y-%m-%d
%H:%M:%S')

self.request_list.append(["makeACDCall", line_ip, line_time])

#print(len(self.request_list))

def inserdb(self):

try:

dsn = cx_Oracle.makedsn(dbhost, port, dbname)

dbpool = cx_Oracle.SessionPool(user=username, password=password, dsn=dsn,min=1, max=50, increment=1)

conn = dbpool.acquire()

cur = conn.cursor()

if len(self.request_list) > 0:

makecallsql = 'insert into
request_log_t(request_type,IP,time) values(:1,:2,:3)'

cur.prepare(makecallsql)

cur.executemany(None,
self.request_list)

cur.execute(

'update tbl_files_tmp_t set
syncflag=1 where file_name = :1', (self.filename,))#日志分析结果已导入数据库，置syncflag标记位为1

conn.commit()

cur.close()

logger.info('{1} ok
duration {0}'.format(time.time()
- self.__ctilogbegin,
self.filename))

return self.filename

except Exception as e:

logger.error('db insert
{1}:{0}'.format(self.filename, e))

conn.rollback()

cur = conn.cursor()

conn.commit()

def term(sig_num, addtion):

print ("current pid is %s, group id
is %s" % (os.getpid(), os.getpgrp()))

os.killpg(os.getpgid(os.getpid()), signal.SIGKILL)

if __name__ == "__main__":

#aa =
ctilog(r'/home/vidmon/py_ENV/log/log/2018_08_01.request.log')

#aa.requestlog_list()

#aa.inserdb()

signal.signal(signal.SIGTERM, term)

analysis2db().start()
