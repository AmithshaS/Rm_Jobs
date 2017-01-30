#!/usr/bin/python
from __future__ import division
import requests, json, sys, os
import ast, re
import numpy as np
import ConfigParser

pwd = os.path.dirname(os.path.realpath(__file__))
script_dir = os.path.split(pwd)[0]
modules_path=script_dir+"/fk_modules"
configs_path=script_dir+"/configs/hosts.conf"
#print modules_path
sys.path.append(modules_path)
sys.path.append(configs_path)

configParser = ConfigParser.RawConfigParser()
configParser.read(configs_path)
resource_manager_add = configParser.get('resource_manager', 'rm01')


import check_failures

failed_task={}
def maps_completed(appid,jobid):
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
                	if task['state'] == 'SUCCEEDED':
				attempt_id =  task['successfulAttempt']
				#print attempt_id
				attempt_count =  int(attempt_id.split('_')[5])
				#print attempt_count
				if  attempt_count > 0 :

					#print "Total Failed Attempt is %d "%attempt_count
					try :
						attempt_details = requests.get('http://%s/proxy/%s/ws/v1/mapreduce/jobs/%s/tasks/%s/attempts'%(resource_manager_add,appid,jobid,taskid), headers = {'ACCEPT':'application/json'}).json().get('taskAttempts')
					except :
						print "Not Found "
					taskAttempts=attempt_details['taskAttempt']
					for i in taskAttempts:
						if i['state'] != "SUCCEEDED":

						#	check_failures.fetch_check(i)
							#print attempt_id
							if "container launch failure" in i['diagnostics']:
							#	print "Exit code is 143"
								
								if i['state'] == "FAILED":
							#		print i['diagnostics']
									startTime= i['startTime']
									nodehttp =  i['nodeHttpAddress']
									time = i['elapsedTime']
									type = i['type']
									progress = i['progress']
									state = i['state']
									#print time
									minut=(time/(1000*60))
                					                minut=float(minut)
					#				print "Elapsed Time", minut ,"min"
		 
									node = nodehttp.split(':')[0]
							#		print node
									try:
										#print "Checking For Host Disks"
										#print "***********************************"
										#print taskid
										#print "Failed Task Attempt ID : ",i['id']
										host,disk,issue = check_host_disk.ro_check(node)
										if not taskid in failed_task:
											failed_task[taskid]={}
										if not attempt_id in failed_task[taskid]:
											failed_task[taskid][attempt_id]={}
										diag = disk+","+issue
										failed_task[taskid][attempt_id]['id']=i['id']
										failed_task[taskid][attempt_id]['diagnostics']	= diag
										failed_task[taskid][attempt_id]['startTime']	= startTime
										failed_task[taskid][attempt_id]['attempt_type']	= type
										failed_task[taskid][attempt_id]['elapsedTime']	=minut
										failed_task[taskid][attempt_id]['host']	=host
										failed_task[taskid][attempt_id]['progress']	= progress
										failed_task[taskid][attempt_id]['state']	= state
										failed_task[taskid][attempt_id]['error']	="Contaiener Launch Failure"
#										failed_task[taskid][attempt_id]['error']	=issue
										#failed_task[taskid][attempt_id]['time']	=minut
									except Exception as e:
										#print "Host is Good"
										#print e
										pass
							if "Too Many fetch failures" in i['diagnostics']:
                                                                if i['state'] == "FAILED":
                                                        #               print i['diagnostics']
                                                                        startTime= i['startTime']
                                                                        nodehttp =  i['nodeHttpAddress']
                                                                        time = i['elapsedTime']
                                                                        type = i['type']
                                                                        progress = i['progress']
                                                                        state = i['state']
                                                                        #print time
                                                                        minut=(time/(1000*60))
                                                                        minut=float(minut)
                                        #                               print "Elapsed Time", minut ,"min"
                 
                                                                        node = nodehttp.split(':')[0]
									if not taskid in failed_task:
										failed_task[taskid]={}
									if not attempt_id in failed_task[taskid]:
										failed_task[taskid][attempt_id]={}
									diag = i['diagnostics']
									failed_task[taskid][attempt_id]['id']=i['id']
									failed_task[taskid][attempt_id]['diagnostics']  = diag
									failed_task[taskid][attempt_id]['startTime']    = startTime
									failed_task[taskid][attempt_id]['attempt_type'] = type
									failed_task[taskid][attempt_id]['elapsedTime']  =minut
									failed_task[taskid][attempt_id]['host'] =nodehttp
									failed_task[taskid][attempt_id]['progress']     = progress
									failed_task[taskid][attempt_id]['state']        = state
									failed_task[taskid][attempt_id]['error']        ="Too Many Fetch Failures"

	if failed_task:
	#	print "Returning Failed Task"
		#print   failed_task
		return  failed_task
						
						


	
#appid=sys.argv[1]
#jobid=sys.argv[2]
#maps_completed(appid,jobid)
