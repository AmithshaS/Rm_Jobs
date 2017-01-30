#!/usr/bin/python

"""
	This is the entry module through job diagnosis is triggered.
	1. It gets app_data from get_app_data.start_jobs()
	
	if job_details available in DB :
		if job_details last updated in 5 mins :
			export_job directly
		else :
			do insert job_details
	else :
		do insert job_details
"""

from __future__ import division
from types import ModuleType
import os, sys
import requests, datetime, time
from datetime import timedelta  
import json, ast, yaml, re, subprocess
from flask import Flask
import sqlite3
import numpy as np
from datetime import datetime
import logging
import logging.config
import threading

pwd = os.path.dirname(os.path.realpath(__file__))
modules=pwd+"/fk_modules/"
check_modules=pwd+"/fk_known_cases/"
sys.path.append(modules)
sys.path.append(check_modules)

import get_app_data
import get_task_data
from get_task_data import check_task_average
import table_module
import check_host_health
import get_tasks_attempts
import get_failed_attempts
import check_completed_tasks

logging.config.fileConfig('configs/logging.conf')
logger = logging.getLogger('live_job_monitor')

own_pid=os.getpid()
script_proc="ps aux | grep live_job_mon.py | grep python | grep -v grep  | grep -v %d | awk '{print $2}'"%own_pid
check_proc=subprocess.Popen(script_proc,shell=True,stdout=subprocess.PIPE)

conn = sqlite3.connect('test.db')
a = sys.argv[1]
a = str(a)

try:
	logger.info('Getting Application details')
	app_data,app_meta =  get_app_data.start_jobs(a)
except:
	logger.info('Error In Getting Job Details')
	print "Check Job Details"
	sys.exit(0)

appid=app_meta[0]
jobid=app_meta[1]

#print appid

def export_job():
	logger.info('Getting Job Details in HTML')
	
	cmd_run_dig="./job_diag_html.py  %s"%jobid
	#print cmd_run_dig
	exe_cmd=subprocess.Popen(cmd_run_dig,shell=True)
	exe_cmd.communicate()

def insert_app_data():

	if app_data:

		sql_cmd='select job_no from job_details where job_name="%s"'%(jobid)
		logger.info('Inserting App Data to DB')
		cur_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	
		sql_in1='''INSERT INTO job_details (job_name, application_name,time)VALUES ("%s","%s","%s")''' % (app_meta[1],app_meta[0],cur_time)
 		conn.execute(sql_in1)
		conn.commit()

		cur = conn.execute(sql_cmd)
	 	for i in cur:
 			job_no=i[0]
 		app_data["job_no"]=job_no
		in_values =  ast.literal_eval(json.dumps(app_data))
		placeholders = ', '.join('?' * len(in_values))
		columns = ', '.join(in_values.keys())
		##should this be tuple or list?
		values = tuple(in_values.values())

		sql_col=[]
	 	for i in app_data.keys():
			if i == "queue" or i == "user" :
				inp = (i+" vachar(100)")
			else :
	 			inp = (i+" int(100)")
 			sql_col.append(inp)
 
	 	sql_col1= re.sub(r'\[|\]|"','',json.dumps(sql_col))
	 	## TODO why this create statement required as tables are created up front.
 		sql_cmd2 = "create table if not exists job_counter (%s);"%sql_col1
		conn.execute(sql_cmd2)

		sql_in2=  " INSERT INTO job_counter ('reducesCompleted', 'queue', 'mapsPending', 'FILE_BYTES_WRITTEN', 'NUM_FAILED_MAPS', 'reducesRunning', 'HDFS_BYTES_READ', 'elapsedTime', 'mapsRunning', 'FILE_BYTES_READ', 'TOTAL_LAUNCHED_MAPS', 'mapsCompleted', 'TOTAL_LAUNCHED_REDUCES', 'job_no', 'NUM_FAILED_REDUCES', 'HDFS_BYTES_WRITTEN', 'reducesPending', 'user') values (%s, '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '%s')"%(app_data['reducesCompleted'], app_data['queue'], app_data['mapsPending'], app_data['FILE_BYTES_WRITTEN'], app_data['NUM_FAILED_MAPS'], app_data['reducesRunning'], app_data['HDFS_BYTES_READ'], app_data['elapsedTime'], app_data['mapsRunning'], app_data['FILE_BYTES_READ'],     app_data['TOTAL_LAUNCHED_MAPS'], app_data['mapsCompleted'], app_data['TOTAL_LAUNCHED_REDUCES'], app_data['job_no'], app_data['NUM_FAILED_REDUCES'], app_data['HDFS_BYTES_WRITTEN'], app_data['reducesPending'], app_data['user'])
		conn.execute(sql_in2)
		conn.commit()



def insert_tasks(job_no,tasks_running,task_types):
	#print tasks_running,task_types

	## get tasks in running state 
        if tasks_running:
                logger.info('Inserting Running Tasks Details into DB')

                map_hosts_tasks={}


	## insert all running tasks into db 
                for i in tasks_running:
                        sql_in3='insert into running_tasks values(%d,"%s",%f,"%s","%s",%d,%d)'%(job_no,i,tasks_running[i]['progress'],tasks_running[i]['type'],tasks_running[i]['status'],tasks_running[i]['startTime'],tasks_running[i]['elapsedTime'])
                        conn.execute(sql_in3)
                        conn.commit()

        ## get reducers in fetching phase
	## Check Hosts where still reducers where fetching 

                        if tasks_running[i]['type'] == "REDUCE":
                                if tasks_running[i]['progress'] <= 34:
                                        map_attempt_id=re.findall(r'attempt.*_.*_m_.*_\d',tasks_running[i]['status'])
                                        if map_attempt_id:
                                                map_task_id=map_attempt_id[0].split('_')
                                                map_task_id="task_"+map_task_id[1]+"_"+map_task_id[2]+"_"+map_task_id[3]+"_"+map_task_id[4]
                                                map_host=get_task_data.get_map_host(appid,jobid,map_task_id)
                                                if map_host:
                                                        sql_in10='insert into reduce_map_host values (%s,"%s","%s","%s")'%(job_no,i,map_task_id,map_host)
                                                        conn.execute(sql_in10)
                                                        conn.commit()
                                                        host,ro_disk,disk,cpu=check_host_health.check(map_host)

                                                        sql_cmd17="select start_time from running_tasks where task_id='%s';"%i
                                                        get_startTime=conn.execute(sql_cmd17)

                                                        for t in get_startTime:
                                                                start_time=t[0]

                                                        sql_in16='insert into map_host_status values("%d","%s","%s","%s","%s","%s","%s","%d")'%(job_no, host, i,map_task_id, ro_disk, disk, cpu, start_time)
                                                        conn.execute(sql_in16)
                                                        conn.commit()




#                               print tasks_running[i]['progress']      
#                               if tasks_running[i]['progress'] > 34:
#                                       pass

def insert_jobcounters(job_no,tasks_running):

	if tasks_running:
		logger.info('Inserting Task Counters to DB')
		counters = []
		for task in tasks_running:
			#print "\n"
			name_dict = get_task_data.get_task_counters(appid,jobid,task)
	#               print name_dict
			sql_in4='insert into task_counters values(%d,"%s",%s,%s,%s,%s,%s)'%(job_no,task,name_dict['FILE_BYTES_WRITTEN'],name_dict['FILE_BYTES_READ'],name_dict['HDFS_BYTES_WRITTEN'],name_dict['REDUCE_SHUFFLE_BYTES'],name_dict['HDFS_BYTES_READ'])
                        #print  sql_in4
			conn.execute(sql_in4)
			conn.commit()
			counters.append(name_dict)


def insert_avg_result(job_no,tasks_running):


	if tasks_running:


		red_time_total=[]
		red_suffle_total=[]
		map_time_total=[]
#		print "\n"
	
		logger.info('Getting the Avg & Max of Time and Shuffle input')
		print appid,jobid
		red_time,red_suffle,map_time = check_task_average(appid,jobid)
#		print red_time,red_suffle,map_time
		if red_time:
			for i in red_time:
				red_time_total.append(i)
			for i in red_suffle:
				red_suffle_total.append(i)
		if map_time:
			for i in map_time:
				map_time_total.append(i)	
	

		if red_time_total:
			avg_red_time=np.mean(red_time_total)
			max_red_time=max(red_time_total)
		if red_suffle_total:
			avg_red_shfl=np.mean(red_suffle_total)
			max_red_shfl=max(red_suffle_total)
		if map_time_total:
			avg_map_time=np.mean(map_time_total)
			max_map_time=max(map_time_total)

		logger.info('Inserting the Avg & Max of Time and Shuffle input')

		if red_time_total and map_time_total:
			if not red_suffle_total:
				avg_red_shfl=0
				max_red_shfl=0
				sql_in7="insert into task_avg_time_bytes values(%d,%f,%d,%f,%f,%f,%f)"%(job_no,avg_red_time, max_red_time, avg_red_shfl, max_red_shfl, avg_map_time, max_map_time)
			else:
				sql_in7="insert into task_avg_time_bytes values(%d,%f,%d,%f,%f,%f,%f)"%(job_no,avg_red_time, max_red_time, avg_red_shfl, max_red_shfl, avg_map_time, max_map_time) 
			conn.execute(sql_in7)
        	        conn.commit()


		elif red_time_total and len(map_time_total)==0:
	
			avg_map_time=0
			max_map_time=0
			if not red_suffle_total:
				avg_red_shfl=0
				max_red_shfl=0
				sql_in7="insert into task_avg_time_bytes values(%d,%f,%d,%f,%f,%f,%f)"%(job_no,avg_red_time, max_red_time, avg_red_shfl, max_red_shfl, avg_map_time, max_map_time)
			else:
				sql_in7="insert into task_avg_time_bytes values(%d,%f,%d,%f,%f,%f,%f)"%(job_no,avg_red_time, max_red_time, avg_red_shfl, max_red_shfl, avg_map_time, max_map_time)

		
			conn.execute(sql_in7)
	                conn.commit()

		elif map_time_total and len(red_time_total)==0:
			avg_red_shfl=0
			max_red_shfl=0
			avg_red_time=0
			max_red_time=0
			sql_in7="insert into task_avg_time_bytes values(%d,%f,%d,%f,%f,%f,%f)"%(job_no,avg_red_time, max_red_time, avg_red_shfl, max_red_shfl, avg_map_time, max_map_time)
			conn.execute(sql_in7)
			conn.commit()

		elif len(red_time_total)==0 and len(map_time_total)==0:
			avg_red_shfl=0
	                max_red_shfl=0
			avg_red_time=0
			max_red_time=0
			avg_map_time=0
                	max_map_time=0
			sql_in7="insert into task_avg_time_bytes values(%d,%f,%d,%f,%f,%d,%d)"%(job_no,avg_red_time, max_red_time, avg_red_shfl, max_red_shfl, avg_map_time, max_map_time)
			#print "No Completed Jobs Found "
			conn.execute(sql_in7)
			conn.commit()




def insert_host_status(job_no,jobid,tasks_running):

	conn = sqlite3.connect('test.db')
	##get hosts running containers
	host_with_task={}
	hosts_dict={}
	host_container_list=[]
#	print "\n"
	for task in tasks_running:
		task = ast.literal_eval(json.dumps(task))
		sql_cmd9="select start_time from running_tasks where task_id='%s';"%task
#		print sql_cmd9
		get_startTime=conn.execute(sql_cmd9)

		for i in get_startTime:
			start_time=i[0]

		host,host_container=check_host_health.get_host(appid,jobid,task)
#		print host,host_container
		host_container_list.append(host_container)
		if host:
			host = host[0]
	#	print host

		### create dict for host where task is running with start time 
			if not host in hosts_dict:
				hosts_dict[host]=start_time

			if not host in host_with_task:
				host_with_task[host]={}
		
			if not task in host_with_task[host]:
					host_with_task[host][task]=start_time

	#print host_with_task
	#print hosts_dict
	#print host_container_list
	for hosts in host_container_list:
		for host in hosts:
			#print  hosts[host]
			for container in hosts[host]:
				task =  hosts[host][container]['taskid']
				sql_in14='insert into task_container values("%d","%s","%s","%s")'%(job_no, task, container, host)
#                               print sql_in14
                                conn.execute(sql_in14)
                                conn.commit()

	if hosts_dict:
		logger.info('Getting Host Health ')
#		print host_with_task[i][min(host_with_task[i], key=host_with_task[i].get)]
		for host in host_with_task:
			task_time =  host_with_task[host][min(host_with_task[host], key=host_with_task[host].get)]
			
			tasks = host_with_task[host].keys()
                	host,disk_ro,disk,cpu=check_host_health.check(host)
			#print host,disk_rodisk,cpu
			for tas in tasks:
				sql_in13='insert into host_status values("%d","%s","%s","%s","%s","%s","%d")'%(job_no,host,tas,disk_ro,disk,cpu,task_time)
#				print sql_in13
				conn.execute(sql_in13)
		        	conn.commit()



def insert_task_attempts(job_no,tasks_running):
	conn = sqlite3.connect('test.db')	
	for task in tasks_running:
	#	print task
		logger.info('Getting Task Attempts Details')
	        attempt_dict=get_tasks_attempts.attempt_check(appid,jobid,task)
		#print attempt_dict

		attempt_count = len(attempt_dict.keys())
		if attempt_count:
			sql_in8="insert into attempt_count values(%d,'%s','%s',%d)"%(job_no, task, tasks_running[task]['type'], attempt_count)
			#print sql_in8
			conn.execute(sql_in8)
			conn.commit()
			logger.info('Getting Running Tasks Failed Attemtpts')
			for i in  attempt_dict:
			#	print "\n",i
				task_type = attempt_dict[i]['type']
				startTime = attempt_dict[i]['startTime']
				elapsedTime = attempt_dict[i]['elapsedTime']
				status = attempt_dict[i]['status']
				nodeHttpAddress = attempt_dict[i]['nodeHttpAddress']
				progress = attempt_dict[i]['progress']
				diagnostics = attempt_dict[i]['diagnostics']
				#print task
				sql_in9="insert into attempt_counters values(%d,'%s','%s','%d','%d','%s','%s','%s','%s','%s')"%(job_no, task, i,startTime, elapsedTime,  task_type, status, nodeHttpAddress, progress, diagnostics)
				#print sql_in9
				conn.execute(sql_in9)
        		        conn.commit()


def insert_completed_attempts(job_no, appid, jobid):

		logger.info('Getting Running Tasks Failed Attemtpts ')
		com_attempt = get_failed_attempts.attempt_check(appid,jobid)
		if com_attempt:
			for task in com_attempt:
		#		print task
				for attempt in com_attempt[task]:
					#print attempt
					attempt_id = attempt
					attempt_type = com_attempt[task][attempt]['type']
					startTime = com_attempt[task][attempt]['startTime']
					elapsedTime = com_attempt[task][attempt]['elapsedTime']
					status = com_attempt[task][attempt]['status']
					progress = com_attempt[task][attempt]['progress']
					#print status
					nodeHttpAddress = com_attempt[task][attempt]['nodeHttpAddress']
					progress = com_attempt[task][attempt]['progress']
					diagnostics = com_attempt[task][attempt]['diagnostics']
					#print task
					sql_in11="insert into attempt_counters values(%d,'%s','%s','%d','%d','%s','%s','%s','%s','%s')"%(job_no, task, attempt_id, startTime, elapsedTime,attempt_type, status, nodeHttpAddress, progress, diagnostics)
					#print sql_in11
       	        	         	conn.execute(sql_in11)
	
		                        conn.commit()
				

		logger.info('Completed Inserting Running Tasks Failed Attemtpts ')

		


def insert_failed_maps(job_no,appid,jobid):
		conn = sqlite3.connect('test.db')
		#taskid, attemptid, host, disk, issue, time = check_completed_tasks.maps_completed(appid,jobid)
		out_dict = check_completed_tasks.maps_completed(appid,jobid)
		#print out_dict
		if out_dict :
			for task in out_dict:
				#task_id = task
				for attempt in out_dict[task]:
					attempt_id = out_dict[task][attempt]['id']
					host = out_dict[task][attempt]['host']
					diagnostics=out_dict[task][attempt]['diagnostics']
					attempt_type=out_dict[task][attempt]['attempt_type']
					progress=out_dict[task][attempt]['progress']
					#diagnosis=out_dict[task][attempt]['disk']+","+out_dict[task][attempt]['issue']
	
					elapsedTime = out_dict[task][attempt]['elapsedTime']
					startTime = out_dict[task][attempt]['startTime']
					#sql_in10="insert into map_failed_attempts values (%d, '%s', '%s', '%s', '%s', %f)"%(job_no, task_id , attempt_id , host,  diagnosis ,  elapsed_time )
					sql_in10="insert into map_failed_attempts values(%d,'%s','%s','%d','%f','%s','%s','%s','%s')"%(job_no, task, attempt_id, startTime, elapsedTime,attempt_type, host, progress, diagnostics)


					#print sql_in10
					conn.execute(sql_in10)
					conn.commit()




def start_insert():
#	print "Starting"
	table_module.clear_tables(jobid)
	table_module.create_tables()

	##insert application data into database
	insert_app_data()

	## get new job_no
 	sql_cmd='select job_no from job_details where job_name="%s"'%(jobid)
	#print sql_cmd
	cur = conn.execute(sql_cmd)
	for i in cur:
		job_no=i[0]

#	print appid,jobid

	## insert  tasks & its status 
	tasks_running,task_types = get_task_data.get_tasks(appid,jobid)
	#print len(tasks_running.keys())

	## insert running tasks to DB
	insert_tasks(job_no,tasks_running,task_types)

	## insert job counters
	insert_jobcounters(job_no,tasks_running)

	## insert avg results
	#insert_avg_result(job_no,tasks_running)

	## insert host status
	insert_host_status(job_no,jobid,tasks_running)

	## insert tasks attempts
	insert_task_attempts(job_no,tasks_running)

	## Get completed Attempts
	#insert_completed_attempts(job_no,appid,jobid)

	## insert completed tasks datas
	insert_failed_maps(job_no,appid,jobid)


	export_job()

def check_job_status():
	table_module.create_tables()
	logger.info('Getting job details from DB')
	
	sql_cmd0='select job_no,time from job_details where job_name="%s"'%(jobid)
	#print sql_cmd0
#	try:
	cur = conn.execute(sql_cmd0)
	if not cur.fetchall():
		cur=[]
#	except: 
#		cur=[]
	#if cur:


	if not cur:
		logger.info('Job Details Not found in DB')
		out = check_proc.communicate()[0]
	        if out:
        	        logger.info('Already process is running')
			time.sleep(5)
			export_job()
	        if not out:
        	        logger.info('Process is not running')
			start_insert()
	else:
		out = check_proc.communicate()[0]
		logger.info('Already job available in DB Checking for last Update time')
		cur = conn.execute(sql_cmd0)
		#job_db_out = cur.fetchall()
		#print job_db_out
		for i in cur:
                	job_no=i[0]
			job_time=i[1]
			FMT = '%Y-%m-%d %H:%M:%S'	
			current_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			tdelta = datetime.strptime(current_time, FMT) - datetime.strptime(job_time, FMT)
#			print job_no
		if tdelta.seconds > 3000:
			logger.info('Job Updated More than 5min')
			if out:
	                        logger.info('Already process is running')
       		                time.sleep(5)	
				export_job()	
			if not out:
                		logger.info('Process is not running')
				logger.info('Job Updated More than 5min')
				start_insert()
		else:
			logger.info('Job Updated Less than  5min')
			#start_insert()
			export_job()


if __name__ == "__main__":

	check_job_status()


