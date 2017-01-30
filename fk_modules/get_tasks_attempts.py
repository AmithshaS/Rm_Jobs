#!/usr/bin/python

from __future__ import division
import requests
import sys, os
import json, ast
import ConfigParser

pwd = os.path.dirname(os.path.realpath(__file__))
script_dir = os.path.split(pwd)[0]
configs_path=script_dir+"/configs/hosts.conf"
configParser = ConfigParser.RawConfigParser()
configParser.read(configs_path)
resource_manager_add = configParser.get('resource_manager', 'rm01')

def attempt_check(app_id,job_id,task_id):
#	#print app_id,job_id,task_id	
	link="http://%s/proxy/%s/ws/v1/mapreduce/jobs/%s/tasks/%s/attempts"%(resource_manager_add,app_id,job_id,task_id)
	tasks=requests.get(link, headers = {'ACCEPT':'application/json'}).json().get('taskAttempts')

	taskAttempts=tasks['taskAttempt']
	taskAttempts=ast.literal_eval(json.dumps(taskAttempts))
#	#print taskAttempts
	attempt_dict={}
	for taskAttempt in taskAttempts:
#		#print taskAttempt

		if taskAttempt['state']=="FAILED" or taskAttempt['state']=="KILLED":
		
	#	#print taskAttempt
#		#print taskAttempt['id']
			attempt_dict[taskAttempt['id']]={}
			attempt_dict[taskAttempt['id']]['type']=taskAttempt['type']
#			attempt_dict[taskAttempt['id']]['state']=taskAttempt['state']
			attempt_dict[taskAttempt['id']]['startTime']=taskAttempt['startTime']
			attempt_dict[taskAttempt['id']]['elapsedTime']=taskAttempt['elapsedTime']
			attempt_dict[taskAttempt['id']]['status']=taskAttempt['status']
			attempt_dict[taskAttempt['id']]['nodeHttpAddress']=taskAttempt['nodeHttpAddress']
			attempt_dict[taskAttempt['id']]['progress']=taskAttempt['progress']
			if taskAttempt['diagnostics']:
				attempt_dict[taskAttempt['id']]['diagnostics']=taskAttempt['diagnostics']
			if not taskAttempt['diagnostics']:
				attempt_dict[taskAttempt['id']]['diagnostics']="NULL"
	return attempt_dict
	
	
