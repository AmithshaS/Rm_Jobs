#!/usr/bin/python

import sqlite3, sys, re, time, json

conn = sqlite3.connect('test.db')


no=sys.argv[1]
org_job_no=sys.argv[1]
sql_q1='SELECT job_no ,job_name,application_name,time from job_details where job_name="%s"'%no
#print sql_q1
job_details = conn.execute(sql_q1)


for row in job_details:
	job_no=row[0]
	no=row[0]
	#print "no",no
	job_id=row[1]
	app_id=row[2]
	app_time=row[3]


app_detail={}
def job_data():
	sql_q2="SELECT job_no,reducesCompleted, mapsPending, FILE_BYTES_WRITTEN, reducesRunning, HDFS_BYTES_READ, elapsedTime, mapsRunning, FILE_BYTES_READ, TOTAL_LAUNCHED_MAPS, TOTAL_LAUNCHED_REDUCES,HDFS_BYTES_WRITTEN,reducesPending, mapsCompleted, queue, user from job_counter where job_no=%s"%no

	job_counter = conn.execute(sql_q2)
	
	for row in job_counter:
		#print row
		job_no=row[0]
		reducesCompleted=row[1]
		mapsPending=row[2]
		FILE_BYTES_WRITTEN=row[3]
		reducesRunning=row[4]
		HDFS_BYTES_READ=row[5]
		elapsedTime=row[6]
		mapsRunning=row[7]
		FILE_BYTES_READ=row[8]
		TOTAL_LAUNCHED_MAPS=row[9]
		TOTAL_LAUNCHED_REDUCES=row[10]
		HDFS_BYTES_WRITTEN=row[11]
		reducesPending =row[12]
		mapsCompleted =row[13]
		queue =row[14]
		user =row[15]


		#print "No of Maps Running", mapsRunning
		#print "No of Maps Pending", mapsPending
		#print "No of Reduces Running", reducesRunning
		#print "No of Reduces Pending", reducesPending
		#print "<p style=\"font-family:courier;\">Applicaiton Running for", elapsedTime,"hrs</p>"
	
		app_detail['type']="app"
		app_detail['name']=org_job_no
		app_detail['elapsed_Time']=elapsedTime
		app_detail['HDFS_BYTES_READ']=HDFS_BYTES_READ
		app_detail['HDFS_BYTES_WRITTEN']=HDFS_BYTES_WRITTEN
		app_detail['mapsRunning']=mapsRunning
		app_detail['mapsPending']=mapsPending
		app_detail['reducesRunning']=reducesRunning
		app_detail['reducesPending']=reducesPending
		app_detail['mapsCompleted']=mapsCompleted
		app_detail['reducesCompleted']=reducesCompleted
		app_detail['queue']=queue
		app_detail['user']=user
		
#	print app_detail
	sql_q3="SELECT type, count(*) from running_tasks where job_no=%s group by  type"%no
	get_task_types=conn.execute(sql_q3)
	app_detail['children']=[]
#	print app_detail
	for i in get_task_types:
#		print i
		task_type =  i[0]
		total_job= i[1]
		a={}
		a['name']=task_type
		a['type']="job"
		a['total_running']=total_job
		app_detail['children'].append(a)


		
		sql_q4="SELECT job_no, task_id, progress, type, status, start_time, elapsed_time from running_tasks where job_no=%s and type=\"%s\""%(no,task_type)
		#print sql_q4
		running_tasks = conn.execute(sql_q4)

		#print "RUNNING TASKS DETAILS"
	#print app_detail
		if running_tasks:
			b={}
		#	print 'TaskId Task Type Progress startTime elapsedTime Logs'
			a['children']=[]
			for row in running_tasks:
		#		print row
				job_no=row[0]
				task_id=row[1]
				progress=row[2]
				task_type=row[3]
				status=row[4]
				start_time=row[5]
				str_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time/1000))
				elapsed_time=row[6]

#				print " %s %s  %s %s %s "%(task_id,task_type,progress,str_time,elapsed_time)
	#			print task_id
				b['name']=task_id
				b['type']="task"
				b['progress']=progress
#				print b
				sql_q5="SELECT job_no, host, disk_issue, cpu_issue, task_time,tasks_running from host_status where job_no=%s and tasks_running=\"%s\""%(no,task_id)
#				print sql_q5
				host_get=conn.execute(sql_q5)
				host_dict={}
#				print host_dict
				if host_get:
					for det in host_get:
						host_get =  det[1] 
						host_disk =  det[2] 
						host_cpu =  det[3] 
						host_dict['name']=host_get
						host_dict['type']="host"
						host_dict['disk']=host_disk
						host_dict['cpu']=host_disk
						
						b['children']=[]
						b['children'].append(host_dict)
		



				if task_type == "REDUCE":
					host_dict['children']=[]
					fetch={}
					if progress <= 34:
						sql_q6="select map_id, host from  reduce_map_host  where task_id='%s' and job_no='%s'"%(task_id,no)
					#	print sql_q6
						map_host=conn.execute(sql_q6)
						
						for x in map_host:
							map_id=x[0]
							host_get=x[1]
							fetch['state']="Fetching"
							fetch['type']="red_fetch"
							fetch['status']=status
							fetch['name']=map_id
							fetch['children']=[]
							map_hosts={}
							map_hosts['name']=host_get

							sql_q7="SELECT job_no, host, disk_issue, cpu_issue, task_time from map_host_status where red_task=\"%s\" and map_task=\"%s\""%(task_id,map_id)
						        host_get=conn.execute(sql_q7)
						        if host_get:
								map_hosts['type']="host"
								map_hosts['disk']=host_disk
		                                                map_hosts['cpu']=host_disk

							fetch['children'].append(map_hosts)
							host_dict['children'].append(fetch)

				
				a['children'].append(b)	
				b={}
				host_dict={}
						
#	print app_detail
	with open('result.json', 'w') as fp:
		json.dump(app_detail, fp)



						#print host





job_data()
