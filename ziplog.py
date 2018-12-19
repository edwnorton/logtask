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

from subprocess
import Popen,PIPE

from t_logdb import oracledbproces



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



class QueueManager(BaseManager): pass



class work(multiprocessing.Process):

def conn(self):

while True:

try:

conf.read("conf.ini")

server = conf.get("queue", "queueserver")

port = int(conf.get("queue", "port"))

QueueManager.register('ready2zip_queue')

QueueManager.register('result_queue')

self.m =
QueueManager(address=(server,
port), authkey=b'abc')

self.m.connect()

self.ready2zip_queue=self.m.ready2zip_queue()

self.result_queue=self.m.result_queue()

break

except Exception as e:

logger.error(e)

def cc(self,sfile):

while True:

statinfo1 = os.stat(sfile)

logger.debug('files is
{0}'.format(sfile))

time.sleep(0.5)

statinfo2 = os.stat(sfile)

if statinfo1.st_size ==
statinfo2.st_size:

self.checkfile(sfile)

break

else:

pass



def run(self):

self.conn()

logger.info("work process
start")

while True:

try:

logger.debug('ready2zip_queue
size is {0}'.format(self.ready2zip_queue.qsize()))

time.sleep(1)

data = self.ready2zip_queue.get()

self.filename =
os.path.basename(data)

bak_dir = conf.get("archive", "bak_dir")

sfile_bak = os.path.join(bak_dir, self.filename)

if
os.path.exists(sfile_bak):

logger.info("receive
{0}".format(sfile_bak))

self.cc(sfile_bak)

#else:

#
#self.result_queue.put({"name":sfile_bak,"result":"zip"})

# logger.warn("{0}
notfind".format(sfile_bak))

except Exception as e:

logger.error(e)

time.sleep(5)

self.conn()

def checkfile(self,sfile):

self.filename =
os.path.basename(sfile)

target_dir = conf.get("archive", "zip_dir")

if re.search('.*\.log$',self.filename ):

#fhs =
self.filename.split('.')

#fh1 = fhs[0]

#fh2 =
fhs[1].split('_')[0]

#dist_path =
os.path.join(target_dir, fh1 + '/' + fh2)

#dist_path =
os.path.join(target_dir, fh1)

#logger.info(dist_path)

#if os.path.isdir(dist_path) ==
False:

#
os.makedirs(dist_path)

# logger.info("dirs have been
installed {0}".format(dist_path))

# os.popen('chmod 777 -R
{0}'.format(dist_path))

tfile = os.path.join(target_dir, self.filename + '.tar.gz')

#print ("----------------tfile
is {0}".format(tfile))

if
os.path.isfile(tfile):

self.result_queue.put({"name":sfile,"result":"zip"})

else:

if
os.path.isfile(sfile):

self.zip(sfile,tfile)

else:

self.result_queue.put({"name":sfile,"result":"zip"})

def zip(self,sfile,tfile):

try:

logger.info('ziping
{0}'.format(self.filename))

cmd = Popen('tar -zcf
{0} {1}'
.format(tfile, sfile),shell=True,stdout=PIPE,stderr=PIPE)

stdout,stderr = cmd.communicate()

if cmd.returncode ==
0 :

self.inserdb()

if stdout=="":

self.result_queue.put({"name":sfile,"result":"zip"})

logger.info('zip success
{0}'.format(self.filename))

else:

self.result_queue.put({"name":sfile,"result":"zip"})

logger.warn('{0} zip
warn:{1}'.format(self.filename, stdout))

else:

logger.warn('{0} zip
warn:{1}'.format(self.filename, stderr))

except Exception as e:

logger.error(e)

def inserdb(self):

try:

dsn = cx_Oracle.makedsn(dbhost, port, dbname)

dbpool = cx_Oracle.SessionPool(user=username, password=password, dsn=dsn,min=1, max=50, increment=1)

conn = dbpool.acquire()

cur = conn.cursor()

cur.execute(

'update tbl_files_tmp_t set
zipflag=1 where file_name = :1', (self.filename,))#文件已归档，置zipflag标记位为1

conn.commit()

cur.close()

logger.info('{0} zipflag
has set 1'.format(self.filename))

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

signal.signal(signal.SIGTERM, term)

work().start()
