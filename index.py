import multiprocessing

import logging

import time,queue,random

from multiprocessing.managers import BaseManager

import configparser

import os

import re

import cx_Oracle

import signal

from subprocess import Popen,PIPE

from t_logdb import oracledbproces



conf = configparser.ConfigParser()

conf.read("conf.ini")

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.DEBUG)

dbhost = conf.get("db", "dbhost")

port = int(conf.get("db", "port"))

dbname = conf.get("db", "dbname")

username = conf.get("db", "user")

password = conf.get("db", "password")



db_q = queue.Queue()

db_q_result = queue.Queue()

db_q_result1 = queue.Queue()

db_q_result2 = queue.Queue()

ignq= multiprocessing.Queue()

comq=multiprocessing.Queue()

n = 1



class QueueManager(BaseManager):

    pass

QueueManager.register('db_queue_task', callable=lambda: db_q)

QueueManager.register('ready2ana_queue', callable=lambda: db_q_result)

QueueManager.register('result_queue', callable=lambda: db_q_result1)

QueueManager.register('ready2zip_queue', callable=lambda: db_q_result2)

manager = QueueManager(address=('127.0.0.1', 5000), authkey=b'abc')

manager.start()

db_queue = manager.db_queue_task()

ready2ana_queue = manager.ready2ana_queue()

result_queue = manager.result_queue()

ready2zip_queue = manager.ready2zip_queue()

def validatefilename(fn):

'''按照文件名筛选文件'''

    if re.search('^\d{4}_\d{2}_\d{2}\.request\.log$', fn):

#logger.debug('validatefilename{0} {1}'.format(fn, 'true'))

        return True

    else:

#logger.debug('validatefilename{0} {1}'.format(fn, 'false'))

        return False

def isfile(file):

return
os.path.isfile(file)

def is_primary(file):

# logger.info('is_primary
{0}'.format(file))

if
validatefilename(os.path.split(file)[1]) == False: #
判断文件名是否为筛选的目标文件

#print("false")

return False

flag = False

if os.access(file, os.R_OK):

with open(file, 'rb') as f:

lines = f.readlines()

for line in lines:

if b'makeACDCall' in line:

#logger.debug('makeacdcall is in
line')

logtype = 1

flag = True

break

return flag

def getlog(sfile):

while True:

statinfo1 = os.stat(sfile)

time.sleep(0.5)

statinfo2 = os.stat(sfile)

if statinfo1.st_size ==
statinfo2.st_size:

if
is_primary(sfile):

#logger.debug('is_primary {0}
{1}'.format(sfile, 'true'))

db_queue.put(sfile) #
判断文件为目标文件后放入db_queue队列中，等待run函数获取后插入数据库中

logger.debug('put
db_queue {0}
and size is{1}'.format(sfile,db_queue.qsize()))

ignq.put(sfile)

#logger.debug('put ignq
{0}'.format(sfile))

else:

pass

break





#class
pubthread(multiprocessing.Process):

# def run(self):

# self.conn()

# logger.info("pubthread
starting")

# while True:

# data=pubq.get()

# try:

# logger.info('zipqueue lens
{0}'.format(self.ready2ana_queue.qsize()))

# logger.info('anaqueue lens
{0}'.format(self.ready2zip_queue.qsize()))

#
self.ready2ana_queue.put(data)

#
self.ready2zip_queue.put(data)

# logger.info("send
{0}".format(data))

# except Exception as
e:

# logger.error(e)

# time.sleep(5)

# self.conn()

# def conn(self):

# server=config.get("queue",
"queueserver")

# port=int(config.get("queue",
"port"))

#
QueueManager.register(ready2ana_queue)

#
QueueManager.register(ready2zip_queue)

# self.m =
QueueManager(address=(server, port),
authkey='abracadabra')

# self.m.connect()

#
self.ready2zip_queue=self.m.ready2zip_queue()

#
self.result_queue=self.m.result_queue()

class dbthread(multiprocessing.Process):

def run(self):

self.db =
oracledbproces(dbhost, port, dbname, username, password)

ignlist=[]

logger.info("dbthread
start")

while True:

logger.info("before:data get
from dbq,and size is {0}".format(db_queue.qsize()))

data=db_queue.get()

logger.info("after:data get from
dbq,and size is {0}".format(db_queue.qsize()))

#if
isfile(data)==False:

# continue

self.sfile=data

self.fname=
os.path.split(self.sfile)[1]

if self.insertdb():

if self.sfile not in ignlist:

ignlist.append(self.sfile)

ready2ana_queue.put(self.sfile)

#ready2zip_queue.put(self.sfile)

logger.debug('put
{0} and ready2ana_queue size is {1}'.format(self.sfile,ready2ana_queue.qsize()))

#self.db.dbexec('insert into
tbl_files_tmp_t(file_name,priflag) values (:1,0)',
(self.fname,))

#logger.info('insert {0} success
'.format(self.fname))



def insertdb(self):

row = self.db.dbexec('select count(1) from tbl_files_tmp_t where
file_name = :1', (self.fname,))

row_p = self.db.dbexec('select count(1) from tbl_files_tmp_t where
file_name = :1 and priflag = 1', (self.fname,))

logger.info('select
return {0}'.format(row_p))

if isinstance(row,list) :

if row[0][0]==0:

print(row_p)

logger.info('the row_p count is
0, now is ready to insert..')

self.db.dbexec('insert into tbl_files_tmp_t(file_name,priflag)
values (:1,1)', (self.fname,))

return True

if row_p[0][0]==1:

return True

else:

return False



class scanbakthread(multiprocessing.Process):

def run(self):

self.ignlistbak=[]

while True:

logger.info('start scan
bakdir')

logger.info("ready2ana_queue
size is {0}".format(ready2ana_queue.qsize()))

bak_dir = conf.get("archive", "bak_dir")

try:

#while
ignq.qsize()>0:

# data=ignq.get()

for root, dirs, files
in os.walk(bak_dir):

#print(root, dirs,
files)

for fn in files:

if fn not in self.ignlistbak:

self.ignlistbak.append(fn)

logger.debug('ignlistbak
add {0}'.format(fn))

sfile = os.path.join(root, fn) #
sfile文件绝对路径

self.getlog_bak(sfile)

except Exception as e:

logger.error(e)

finally:

time.sleep(1)

def getlog_bak(self,sfile):

while True:

statinfo1 = os.stat(sfile)

time.sleep(0.5)

statinfo2 = os.stat(sfile)

if statinfo1.st_size ==
statinfo2.st_size:

if
is_primary(sfile):

#logger.debug('is_primary {0}
{1}'.format(sfile, 'true'))

ready2zip_queue.put(sfile) #
判断文件为目标文件后放入ready2zip_queue队列中，等待scanbakthread获取后进行归档任务

logger.debug('put
ready2zip_queue {0}
and size is{1}'.format(sfile,ready2zip_queue.qsize()))

ignq.put(sfile)

#logger.debug('put ignq
{0}'.format(sfile))

break

else:

pass



class checkthread(multiprocessing.Process):

def run(self):

self.conn()

self.db =
oracledbproces(dbhost, port, dbname, username, password)

self.checkdic={}

logger.info("checkthread
starting")

while True:

logger.info("result_queue size
is {0}".format(result_queue.qsize()))

time.sleep(1)

try:

while self.result_queue.qsize()>0:

data = self.result_queue.get()

logger.info("receive
{0}".format(data))

self.checkmain(data)

except Exception as e:

logger.error(e)

time.sleep(5)

self.conn()

for key,value
in self.checkdic.items():

if value60*60*3:

self.checkdic[key]=time.time()

pubq.put(key)

logger.debug('put pubq
{0}'.format(key))

def conn(self):

server=conf.get("queue",
"queueserver")

port=int(conf.get("queue", "port"))

QueueManager.register('result_queue')

self.m =
QueueManager(address=(server,
port), authkey=b'abc')

self.m.connect()

self.result_queue=self.m.result_queue()

def checkmain(self,data):

try:

if isinstance(data,dict):

self.file=data["name"]

self.fname=os.path.split(self.file)[1]

self.result=data["result"]

if self.file not in self.checkdic:

self.checkdic[self.file]=time.time()

self.check()

except Exception as e:

logger.error('{0}{1}'.format(self.file,e))

def check(self):

try:

if isfile(self.file)==False:

self.db.dbexec('delete from tbl_files_tmp_t where file_name=:1
',(self.fname,))

del self.checkdic[self.file]

#comq.put(self.file)

logger.warn('file not find
cleared {0}'.format(self.file))

return

if self.result =="zip":

row=self.db.dbexec('select count(1) from tbl_files_tmp_t where
syncflag=1 and file_name=:1 ',(self.fname,))

logger.info('{0}{1}'.format(self.fname,row))

if row[0][0]==1
:

self.clearlog()

else:

self.db.dbexec('update tbl_files_tmp_t set zipflag=1 where
file_name=:1 and zipflag<>1',(self.fname,))

#elif self.result
=="ana":

# row=self.db.dbexec('select
count(1) from tbl_files_tmp_t where zipflag=1 and file_name=:1
',(self.fname,))

#
logger.info('{0}{1}'.format(self.fname,row))

# if row[0][0]==1 :

# self.clearlog()

except Exception as e:

logger.error('{0}{1}'.format(self.file,e))

def clearlog(self):

try:

while True:

cmd =Popen('rm -f
{0}' .format(self.file),shell=True,stdout=PIPE,stderr=PIPE)

stdout,stderr = cmd.communicate()

if
os.path.isfile(self.file)==False:

break

self.db.dbexec('delete from tbl_files_tmp_t where file_name=:1
and syncflag!=2',(self.fname,))

logger.debug('checkdic is
{0}'.format(self.checkdic))

del self.checkdic[self.file]

#comq.put(self.file)

logger.info('cleared
{0}'.format(self.file))

except Exception as e:

logger.error('clearlog
error {0}{1}'.format(self.file,e))

def start():

dbthread().start()

checkthread().start()

scanbakthread().start()

ignorelist = []

while True:

logger.info('start
loop')

logger.info("ready2ana_queue
size is {0}".format(ready2ana_queue.qsize()))

source_dir = conf.get("source", "dir")

try:

#while
ignq.qsize()>0:

# data=ignq.get()

for root, dirs, files
in os.walk(source_dir):

#print(root, dirs,
files)

for fn in files:

if fn not in ignorelist:

ignorelist.append(fn)

logger.debug('ignorelist
add {0}'.format(fn))

sfile = os.path.join(root, fn) #
sfile文件绝对路径

getlog(sfile)

except Exception as e:

logger.error(e)

finally:

time.sleep(1)

def term(sig_num, addtion):

print ("current pid is %s, group id
is %s" % (os.getpid(), os.getpgrp()))

os.killpg(os.getpgid(os.getpid()), signal.SIGKILL)

if __name__=='__main__':

signal.signal(signal.SIGTERM, term)

start()
