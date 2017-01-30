#!/usr/bin/python

from __future__ import division
import requests
import sys, os
import json, ast, re 
import ConfigParser

pwd = os.path.dirname(os.path.realpath(__file__))
script_dir = os.path.split(pwd)[0]
configs_path=script_dir+"/configs/hosts.conf"
configParser = ConfigParser.RawConfigParser()
configParser.read(configs_path)
resource_manager_add = configParser.get('resource_manager', 'rm01')

def attempt_check(appid,jobid):
	tasks_running={}
	task_type=[]
	attempt_dict={}
	try:
		tasks=requests.get('http://%s/proxy/%s/ws/v1/mapreduce/jobs/%s/tasks'%(resource_manager_add,appid,jobid) , headers = {'ACCEPT':'application/json'}).json().get('tasks')
	except Exception as e:
		print e
		print 'Task Information not availale in RM '
		sys.exit(1)
	tasks=tasks['task']
	#print tasks
        for task in tasks:
                #print task
                taskid = task['id']
                map_task = re.findall('_m_',taskid)
                if map_task:
                        if task['state'] == 'SUCCEEDED':
                                attempt_id =  task['successfulAttempt']
                                #print attempt_id
                                attempt_count =  int(attempt_id.split('_')[5])
          #                      print attempt_count
                                if  attempt_count > 0 :
					try :
						attempt_details = requests.get('http://%s/proxy/%s/ws/v1/mapreduce/jobs/%s/tasks/%s/attempts'%(resource_manager_add,appid,jobid,taskid), headers = {'ACCEPT':'application/json'}).json().get('taskAttempts')
					except Exception as e :
						#print e
						print "Not Found "
	#					pass
					taskAttempts=attempt_details['taskAttempt']
			#		print taskAttempts
                                        for i in taskAttempts:
                                                if i['state'] == "FAILED":
					#		print attempt_id
							#print i
					#		print taskid
							if not taskid in attempt_dict:
								attempt_dict[taskid]={}
							if not attempt_id in attempt_dict[taskid]:
								attempt_dict[taskid][attempt_id]={}
							attempt_dict[taskid][attempt_id]['type']=i['type']
							attempt_dict[taskid][attempt_id]['status']=i['status']
							attempt_dict[taskid][attempt_id]['progress']=i['progress']
							attempt_dict[taskid][attempt_id]['diagnostics']=i['diagnostics'].replace('\n', ' ')
							attempt_dict[taskid][attempt_id]['nodeHttpAddress']=i['nodeHttpAddress']
							attempt_dict[taskid][attempt_id]['startTime']=i['startTime']
							attempt_dict[taskid][attempt_id]['elapsedTime']=i['elapsedTime']
							


	#print  attempt_dict
	return  attempt_dict
	
#appid=sys.argv[1]
#jobid=sys.argv[2]
#attempt_check(appid,jobid)	
