#!/usr/bin/python

from __future__ import division
import sys, os
pwd = os.path.dirname(os.path.realpath(__file__))
import sqlite3
import requests
import datetime
import subprocess
import os
conn = sqlite3.connect('test.db')
cursor = conn.cursor()
from StringIO import StringIO    
import pycurl


def get_fetchers(log_file_url):


	curl_log_cmd="curl -s  '%s' --stderr - | grep 'org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput' | awk '{print $1,$2,$12}' "%log_file_url
	print curl_log_cmd
        curl_log=subprocess.Popen(curl_log_cmd,stdout=subprocess.PIPE,shell=True,stderr=subprocess.PIPE)
        fetch_mappers,err = curl_log.communicate()
	print fetch_mappers

def get_records_written(log_file_url):

	output=[]
	curl_log_cmd="curl -s  '%s' --stderr - | grep 'org.apache.hadoop.hive.ql.exec.FileSinkOperator'| grep 'records written'  "%log_file_url
	storage = StringIO()
	c = pycurl.Curl()
	c.setopt(c.URL, log_file_url)
	c.setopt(c.WRITEFUNCTION, storage.write)
	c.perform()
	c.close()
	content = storage.getvalue().split('\n')
#	print content
	for i in content:
		if "records written" in i:
			#print i 
			record_size = i.split('records written - ',1)[1]	
	if record_size:
#		print record_size
		return  record_size
#	print curl_log_cmd
        '''curl_log=subprocess.Popen(curl_log_cmd,stdout=subprocess.PIPE,shell=True,stderr=subprocess.PIPE)
        records_written,err = curl_log.communicate()

	for i in records_written:
		output.append(i)
#	print records_written[0]
	print output'''




def log_file(log_url):

	curl_size_cmd="curl %s| sed 's/<\/*[^>]*>//g'  | grep syslog  | grep  -Eo '[0-9]+'"%log_url
	curl_log_size=subprocess.Popen(curl_size_cmd,stdout=subprocess.PIPE,shell=True,stderr=subprocess.PIPE)
	log_size,err = curl_log_size.communicate()
	if log_size:
	        log_size  = int(log_size.split()[0])
 #       	print log_size

	        log_size_kb = log_size/1024
       # 	print log_size_kb
	        log_size_mb = log_size_kb/1024
        	#print log_size_mb

	        if log_size_mb > 2 :
        #	        print "Log size is more than 2 MB"
                	start_time  = log_size - 2097152
	                end_time = log_size
 #       	        print start_time,end_time
			log_file_url = "%s/syslog/?start=%d&end=%s"%(log_url,start_time,end_time)
#			print log_file_url
			record_size = get_records_written(log_file_url)
			return record_size 
	        else:
        	        start_time = 0
                	end_time = log_size
#	                print start_time,end_time
			log_file_url = "%s/syslog/?start=0"%log_url
			#print log_file_url
			record_size = get_records_written(log_file_url)
			return record_size

def log_url(taskid):
	
	sql_q1="SELECT * from task_container where task_name='%s'"%taskid
	#print sql_q1
        cursor.execute(sql_q1)
        container_data = cursor.fetchall()
	for i in  container_data:
		job_no=i[0]
		task_id=i[1]
		container=i[2]
		host=i[3]
	#print job_no, task_id, container, host
	sql_q2="SELECT user from job_counter where job_no=%d"%job_no
	cursor.execute(sql_q2)
	user_data =  cursor.fetchall()
	for i in user_data:
		user_name=i[0]
#	print user_name
	log_url="http://%s:8042/node/containerlogs/%s/%s"%(host, container, user_name)	
#	print log_url
	record_size = log_file(log_url)
	return record_size
#log_url(sys.argv[1])

