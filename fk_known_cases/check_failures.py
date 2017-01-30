#!/usr/bin/python
from __future__ import division
import requests, json, sys, os
import ast, re, time
import numpy as np
import ConfigParser
import sqlite3


pwd = os.path.dirname(os.path.realpath(__file__))
#print pwd

#modules=pwd+"/fk_modules/"
#print modules
#sys.path.append(modules)

script_dir = os.path.split(pwd)[0]
configs_path=script_dir+"/configs/hosts.conf"
modules_path=script_dir+"/fk_modules/"
#print modules_path
sys.path.append(modules_path)

configParser = ConfigParser.RawConfigParser()
configParser.read(configs_path)
history_server_add = configParser.get('history_server', 'hs01')


import check_host_health
#import check_host_disk

failed_task={}


def launch_failure(task_name, attempt_name, job_data, startTime, elapsedTime, nodeHttpAddress):
	if job_data:
#		print job_data
		error_type="Launch Failure"
		if "container launch failure" in job_data:
			#id = job_data['id']
			print "This %s Failed Due to Container Launch Failure"%attempt_name
			minut=(elapsedTime/(1000*60))
			minut=float(minut)
			host,disk,issue = check_host_disk.ro_check(nodeHttpAddress)
			error = disk,issue

			return task_name, attempt_name, error_type, error, minut, nodeHttpAddress


def fetch_check(task_name, attempt_name, job_data, startTime, elapsedTime, nodeHttpAddress):
		if job_data:
			#print job_data
			error_type="Fetch Failure"
			if "Too Many fetch failures" in job_data:
				#id = job_data['id']
				#print "This %s Failed Due to Too Many fetch failures"%attempt_name
				minut=(elapsedTime/(1000*60))
				minut=float(minut)
				error = "Too Many fetch failures"	
				#print "Elapsed Time", minut ,"min"
				return task_name, attempt_name, error_type, error, minut, nodeHttpAddress



def disk_space_issue(task_name, attempt_name, job_data, startTime, elapsedTime, nodeHttpAddress):

	
	if job_data:
#		print job_data
	#	print "/n/n/n"
		#print attempt_name
		if "ShuffleError: error in shuffle in OnDiskMerger" in job_data:
		#	print attempt_name
		#	print "Yes Shuffle Error"
			error = check_host_health.space_check(nodeHttpAddress)
		#	time.sleep(1)
			#print out 
			minut=(elapsedTime/(1000*60))
			minut=float(minut)
			error_type = "Disk Space Issue"
			return task_name, attempt_name, error_type, error, minut, nodeHttpAddress
		elif "FSError" in job_data:
		#	print attempt_name
		#	print "Yes FS Error with Disk Space"
			error = check_host_health.space_check(nodeHttpAddress)
                 #       time.sleep(1)
                        #print out
                        minut=(elapsedTime/(1000*60))
                        minut=float(minut)
                        error_type = "Disk Space Issue"
                        return task_name, attempt_name, error_type, error, minut, nodeHttpAddress
		
	
						
					
