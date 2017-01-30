#!/usr/bin/python

from __future__ import division
import requests, json, sys, os
import ast
import numpy as np
import threading
import ConfigParser

pwd = os.path.dirname(os.path.realpath(__file__))
script_dir = os.path.split(pwd)[0]
configs_path=script_dir+"/configs/hosts.conf"

configParser = ConfigParser.RawConfigParser()
configParser.read(configs_path)
resource_manager_add = configParser.get('resource_manager', 'rm01')

counter_data=["FILE_BYTES_READ","FILE_BYTES_WRITTEN","HDFS_BYTES_READ","HDFS_BYTES_WRITTEN","REDUCE_SHUFFLE_BYTES"]

def get_tasks(appid,jobid):

        tasks_running={}
        task_type=[]
	try:
                cmd = 'http://%s/proxy/%s/ws/v1/mapreduce/jobs/%s/tasks'%(resource_manager_add,appid,jobid)
	        tasks=requests.get(cmd , headers = {'ACCEPT':'application/json'}).json().get('tasks')
	except:
		print '<p>Task Information not availale in RM </p>'

        tasks=tasks['task']
        for task in tasks:
                if task['state'] == 'RUNNING':
                        taskid = task['id']
                        tasks_running[taskid]={}
                        tasks_running[taskid]["type"]=task['type']
                        tasks_running[taskid]["status"]=task['status']
                        tasks_running[taskid]["progress"]=task['progress']
                        tasks_running[taskid]["startTime"]=task['startTime']
			if task['elapsedTime']:
				millisec = task['elapsedTime']
				minut=(millisec/(1000*60))
				minut=float(minut)
				#print minut
	                        tasks_running[taskid]["elapsedTime"]=minut

#       #print tasks_running
        for i in tasks_running:
                task_url="http://%s/proxy/%s/ws/v1/mapreduce/jobs/%s/tasks/%s"%(resource_manager_add,appid,jobid,i)

                if tasks_running[i]["type"] not in task_type:
                        task_type.append(tasks_running[i]["type"])
#	#print tasks_running
	return tasks_running,task_type


def get_task_counters(appid,jobid,taskid):

        name_dict={}
        #print "Getting Counters for Task :",taskid
        try:
                task_counter=requests.get('http://%s/proxy/%s/ws/v1/mapreduce/jobs/%s/tasks/%s/counters'%(resource_manager_add,appid,jobid,taskid) , headers = {'ACCEPT':'application/json'}).json().get('jobTaskCounters')

        except KeyError:
                print "Tasks Not Found in Resource manager check in History server / Wait for some time and re-run the script"

        task_counter = ast.literal_eval(json.dumps(task_counter))
        for i in  task_counter['taskCounterGroup'] :
                if i['counterGroupName'] == "org.apache.hadoop.mapreduce.FileSystemCounter":
                        for x in  i['counter']:
                                if x['name'] == "FILE_BYTES_READ":
                                        values_bytes=x['value']
                                        values_gb=format(values_bytes/1024/1024/1024 , '.10f')
                                        ##print x['name'],values_gb
                                        name_dict[x['name']]={}
                                        name_dict[x['name']]=values_gb
                                if x['name'] == "FILE_BYTES_WRITTEN":
                                        values_bytes=x['value']
                                        #values_gb=str(values_bytes/1024/1024/1024)     
                                        values_gb=format(values_bytes/1024/1024/1024, '.20f')
                                        ##print x['name'],values_gb
                                        name_dict[x['name']]={}
                                        name_dict[x['name']]=values_gb
                                if x['name'] == "HDFS_BYTES_READ":
                                        values_bytes=x['value']
                                        values_gb=float(values_bytes/1024/1024/1024)
                                        ##print x['name'],values_gb
                                        name_dict[x['name']]={}
                                        name_dict[x['name']]=values_gb
                                if x['name'] == "HDFS_BYTES_WRITTEN":
                                        values_bytes=x['value']
                                        values_gb=float(values_bytes/1024/1024/1024)
#                                       #print x['name'],values_gb       
                                        name_dict[x['name']]={}
                                        name_dict[x['name']]=values_gb
		if i['counterGroupName'] == "org.apache.hadoop.mapreduce.TaskCounter":
                        for x in  i['counter']:
                                if x['name'] == "REDUCE_SHUFFLE_BYTES":
                                        values_bytes=x['value']
                                        values_gb=float(values_bytes/1024/1024/1024)
                                        name_dict[x['name']]={}
                                        name_dict[x['name']]=values_gb

	for val in counter_data:
		if val not in json.dumps(name_dict.keys()):
			name_dict[val]="NULL"
#	return name_dict
	#print name_dict
	return name_dict

red_time=[]
red_suffle=[]
map_time=[]
def cal_avg(appid,jobid,tasks):

	for task in tasks:
#		print task
		if task['state'] == 'SUCCEEDED':
			task_id = task['id']
			#if task_type_name == '"REDUCE"':
			if task['type'] == "REDUCE":
				millis =task['elapsedTime']
				#sec = millis/1000
				mins=(millis/(1000*60))
				mins = float(mins)
				red_time.append(mins)
				taskid = task['id']

                                task_counter=requests.get('http://%s/proxy/%s/ws/v1/mapreduce/jobs/%s/tasks/%s/counters'%(resource_manager_add,appid,jobid,taskid) , headers = {'ACCEPT':'application/json'}).json().get('jobTaskCounters')
				for i in  task_counter['taskCounterGroup'] :
					#print i
					if i['counterGroupName'] == "org.apache.hadoop.mapreduce.TaskCounter":
						for x in  i['counter']:
							if x['name'] == "REDUCE_SHUFFLE_BYTES":
							#       #print x['value']
								values_bytes=x['value']
								##print values_bytes
								values_gb=float(values_bytes/1024/1024/1024)
								##print values_gb
								red_suffle.append(values_gb)
			if task['type'] == "MAP":
				millis =task['elapsedTime']
				#sec = millis/1000
				mins=(millis/(1000*60))
				mins = float(mins)
				map_time.append(mins)

#	return red_time,red_suffle,map_time

def check_task_average(appid,jobid):
        tasks=requests.get('http://%s/proxy/%s/ws/v1/mapreduce/jobs/%s/tasks'%(resource_manager_add,appid,jobid) , headers = {'ACCEPT':'application/json'}).json().get('tasks')
        tasks=tasks['task']
        tasks=ast.literal_eval(json.dumps(tasks))
#	print tasks
	total_tasks =  len(tasks)
	
## if more than 200 tasks it will splitted into chunks and sends to RM 
	if total_tasks > 200:
#		print "tasks is more than 200"
		chunks = [tasks[x:x+200] for x in xrange(0, len(tasks), 200)]
	
		for i, item in enumerate(chunks):
#			t = "t%d"%i
			t = threading.Thread(target=cal_avg,args=(appid,jobid,item,))
		        t.start()
		t.join()
		return red_time,red_suffle,map_time
	else:
		t1 = threading.Thread(target=cal_avg,args=(appid,jobid,tasks,))
		t1.start()
		t1.join()
		return red_time,red_suffle,map_time


def get_map_host(appid,jobid,map_task_id):

	map_task=requests.get('http://%s/proxy/%s/ws/v1/mapreduce/jobs/%s/tasks/%s/attempts'%(resource_manager_add,appid,jobid,map_task_id) , headers = {'ACCEPT':'application/json'}).json().get('taskAttempts')

	taskAttempts=map_task['taskAttempt']
	taskAttempts = ast.literal_eval(json.dumps(taskAttempts))
	for taskAttempt in taskAttempts:
		if taskAttempt['state']=="SUCCEEDED":
			
			#print "Copying Map Data from:",taskAttempt['nodeHttpAddress']
			host = taskAttempt['nodeHttpAddress']
			if host:
				host = host.split(':')
				return host[0]


			return taskAttempt['nodeHttpAddress']

#get_tasks(sys.argv[1],sys.argv[2])
#check_task_average(sys.argv[1],sys.argv[2])
#get_task_counters(sys.argv[1],sys.argv[2],sys.argv[3])
