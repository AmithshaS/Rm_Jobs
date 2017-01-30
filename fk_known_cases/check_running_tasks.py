#!/usr/bin/python
from __future__ import division
import requests, json, sys, os
import ast, re
import numpy as np
import check_host_disk
import check_failures
import ConfigParser
pwd = os.path.dirname(os.path.realpath(__file__))
script_dir = os.path.split(pwd)[0]
configs_path=script_dir+"/configs/hosts.conf"

configParser = ConfigParser.RawConfigParser()
configParser.read(configs_path)
resource_manager_add = configParser.get('resource_manager', 'rm01')



failed_task={}
def maps_running(appid,jobid):
        tasks_running={}
        task_type=[]
	try:
	        tasks=requests.get('http://%s/proxy/%s/ws/v1/mapreduce/jobs/%s/tasks'%(resource_manager_add,appid,jobid) , headers = {'ACCEPT':'application/json'}).json().get('tasks')
	except:
		print 'Task Information not availale in RM '
		sys.exit(1)

        tasks=tasks['task']
        for task in tasks:
		#print task
		taskid = task['id']
		map_task = re.findall('_m_',taskid)
		if map_task:
                	if task['state'] == 'RUNNING':
				try :
					attempt_details = requests.get('http://%s/proxy/%s/ws/v1/mapreduce/jobs/%s/tasks/%s/attempts'%(resource_manager_add,appid,jobid,taskid), headers = {'ACCEPT':'application/json'}).json().get('taskAttempts')
				except Exception as e:
					print "Not Found "
					print e
				taskAttempts=attempt_details['taskAttempt']
				for i in taskAttempts:
					if i['state'] != "RUNNING":
					#	print i['id']
						check_failures.fetch_check(i)



				#print task
				#attempt_id =  task['successfulAttempt']
				#print attempt_id
				#attempt_count =  int(attempt_id.split('_')[5])
				#print attempt_count
				#if  attempt_count > 0 :
						

		#			#print "Total Failed Attempt is %d "%attempt_count
					taskAttempts=attempt_details['taskAttempt']
			#		for i in taskAttempts:
			#			if i['state'] != "FAILED":

			#				#check_failures.fetch_check(i)
			#				print attempt_id


	
#appid=sys.argv[1]
#jobid=sys.argv[2]
#maps_running(appid,jobid)
