#!/usr/bin/python

import os, sys
pwd = os.path.dirname(os.path.realpath(__file__))
modules=pwd+"/fk_modules/"
#sys.path.append('/home/amithsha.s/python')
sys.path.append(modules)
import sqlite3
import logging
import logging.config



#logger = logging.getLogger('live_job_monitor')
def clear_tables(jobid):
	conn = sqlite3.connect('test.db')
	sql_cmd='select job_no from job_details where job_name="%s"'%(jobid)
	cur = conn.execute(sql_cmd)
	#print  sql_cmd
	try:
		
		#logger.info('Deleting the content From DB')
                for i in cur:
                        job_no=i[0]
                sql_del1="delete from job_details where job_no=%d "%(job_no)
                conn.execute(sql_del1)
		conn.commit()
                sql_del2="delete from job_counter where job_no=%d "%(job_no)
                conn.execute(sql_del2)
		conn.commit()
                sql_del3="delete from running_tasks where job_no=%d "%(job_no)
                conn.execute(sql_del3)
		conn.commit()
                sql_del4="delete from task_counters where job_no=%d "%(job_no)
                conn.execute(sql_del4)
		conn.commit()
                sql_del5="delete from task_avg_time_bytes where job_no=%d "%(job_no)
                conn.execute(sql_del5)
		conn.commit()
                sql_del6="delete from host_status where job_no=%d "%(job_no)
                conn.execute(sql_del6)
		conn.commit()
                sql_del7="delete from attempt_count where job_no=%d "%(job_no)
                conn.execute(sql_del7)
		conn.commit()
                sql_del8="delete from attempt_counters where job_no=%d "%(job_no)
                conn.execute(sql_del8)
		conn.commit()
		
		sql_del8="delete from map_failed_attempts where job_no=%d "%(job_no)
		conn.execute(sql_del8)
		conn.commit()

		sql_del9="delete from task_container where job_no=%d "%(job_no)
		conn.execute(sql_del9)
		conn.commit()

        except  Exception as e:
		#print e
                pass


def create_tables():

	conn = sqlite3.connect('test.db')

	sql_cmd1 = "CREATE TABLE if not exists job_details( job_no INTEGER PRIMARY KEY   AUTOINCREMENT, job_name varchar(200),application_name varchar(200),time text)"
	conn.execute(sql_cmd1)
	conn.commit()

	sql_cmd2='create table if not exists running_tasks (job_no int(200),task_id varchar(200),progress float(100),type varchar(200),status varchar(200),start_time int    ,elapsed_time int)'
	conn.execute(sql_cmd2)
	conn.commit()

	sql_cmd3='create table if not exists reduce_map_host (job_no int(200),task_id varchar(200),map_id varchar,host varchar)'
	conn.execute(sql_cmd3)
	conn.commit()

	sql_cmd4="create table if not exists map_host_status(job_no int(200),host varchar(200), red_task varchar(200), map_task varchar(200), ro_disk varchar(200), disk_issue varchar(200) ,cpu_issue varchar(200),task_time int)"
	conn.execute(sql_cmd4)
	conn.commit()



	sql_cmd5='create table if not exists task_counters (job_no int(100),task_name varchar(200),file_bytes_written float(100),file_bytes_read float(100),hdfs_bytes_written float(100),reduce_shuffle_bytes float(100),hdfs_bytes_read float(100))'
	conn.execute(sql_cmd5)
	conn.commit()

	sql_cmd6="create table if not exists task_avg_time_bytes (job_no int,avg_reduce_time int,max_reduce_time int,avg_shuffle_input float(200),max_shuffle_input float    (200),avg_map_time int(200)    ,max_map_time int(200))"                
	conn.execute(sql_cmd6)
	conn.commit()
	
	sql_cmd7="create table if not exists host_status(job_no int(200),host varchar(200), tasks_running varchar, ro_disk varchar(200), disk_issue varchar(200) ,cpu_issue varchar(200),task_time int)"
	conn.execute(sql_cmd7)
	conn.commit()

	sql_cmd8="create table if not exists attempt_counters(job_no int(200),task_name varchar, attempt_name varchar, startTime int, elapsedTime int,  job_type varchar, status varchar ,nodeHttpAddress varchar,progress varchar, diagnostics varchar)"
	conn.execute(sql_cmd8)
	conn.commit()

	sql_cmd9="create table if not exists attempt_count(job_no int(200), task_name varchar, task_type varchar, attempt_count int)"
	conn.execute(sql_cmd9)
	conn.commit()
	
	sql_cmd10="create table if not exists map_failed_attempts (job_no int(200),task_name varchar, attempt_name varchar, startTime int, elapsedTime int,  job_type varchar, nodeHttpAddress varchar,progress varchar, diagnostics varchar)"
	conn.execute(sql_cmd10)
        conn.commit()


	sql_cmd11="create table if not exists task_container (job_no int(200),task_name varchar, container_name varchar, host varchar)"
	conn.execute(sql_cmd11)
        conn.commit()


