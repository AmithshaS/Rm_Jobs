#!/usr/bin/python

"""
This module is used to retrieve application or job high level details.

methods :
        start_jobs(appId) :
                - It hits RM url to fetch application/job level details and returns two result : 
                  app_data - a dict which contains values for "reducesCompleted", "mapsRunning", "FILE_BYTES_WRITTEN", "reducesRunning", "HDFS_BYTES_READ", "elapsedTime","mapsPending", "FILE_BYTES_READ", "TOTAL_LAUNCHED_MAPS", "TOTAL_LAUNCHED_REDUCES", "HDFS_BYTES_WRITTEN", "reducesPending", "mapsCompleted","NUM_FAILED_MAPS","NUM_FAILED_REDUCES","queue","user"
                  app_meta - a list containing jobId and appId (which may not be required)
"""

from __future__ import division
import sys, os
import requests
import datetime
import time
import os
from datetime import timedelta
import json,ast
import yaml
import re
import numpy as np
import subprocess
import ConfigParser

pwd = os.path.dirname(os.path.realpath(__file__))
#print pwd
script_dir = os.path.split(pwd)[0]
configs_path=script_dir+"/configs/hosts.conf"
#print configs_path

configParser = ConfigParser.RawConfigParser()   
configParser.read(configs_path)
resource_manager_add = configParser.get('resource_manager', 'rm01')

def start_jobs(appId):
        app_data={}
        cmd = 'http://%s/ws/v1/cluster/apps/%s'%(resource_manager_add,appId)
	app_meta=[]
	app_meta.append(appId)
        job_data=["reducesCompleted", "mapsRunning", "FILE_BYTES_WRITTEN", "reducesRunning", "HDFS_BYTES_READ", "elapsedTime","mapsPending", "FILE_BYTES_READ", "TOTAL_LAUNCHED_MAPS", "TOTAL_LAUNCHED_REDUCES", "HDFS_BYTES_WRITTEN", "reducesPending", "mapsCompleted","NUM_FAILED_MAPS","NUM_FAILED_REDUCES","queue","user"]
	
        try:
	        app_details=requests.get(cmd, headers = {'ACCEPT':'application/json'}).json().get('app')
                queue_name = app_details["queue"]
                user_name = app_details["user"]
	except Exception as e :
		print "Job Not Found Either not in Running state / Succeeded / Killed"
		print e
		sys.exit(1)
	
        ##This call might not be required as its used only to retrieve jobId which can be derived from appId itself.
        try:
                cmd = 'http://%s/proxy/%s/ws/v1/mapreduce/jobs'%(resource_manager_add,appId)
		jobs=requests.get(cmd , headers = {'ACCEPT':'application/json'}).json().get('jobs')
	except:
		print "Job Not Found Either not in Running state / Succeeded / Killed"
                sys.exit(1)
        
        jobs=jobs['job']
        
        for job in jobs:
                job_id = job['id']

                #print "Job Id","\t:",job_id
		app_meta.append(ast.literal_eval(json.dumps(job_id)))
                try:
                        cmd = 'http://%s/proxy/%s/ws/v1/mapreduce/jobs/%s/counters'%(resource_manager_add,appId,job_id)
                        job_counter=requests.get(cmd , headers = {'ACCEPT':'application/json'}).json().get('jobCounters')
                        cmd = 'http://%s/proxy/%s/ws/v1/mapreduce/jobs/%s'%(resource_manager_add,appId,job_id)
                        pending_cal=requests.get(cmd , headers = {'ACCEPT':'application/json'}).json().get('job')
                except:
                        print "Job Not Found in Resource Manager Either Moved to History Server"
			break
                for counter in  job_counter['counterGroup']:
                        for x in counter['counter']:
                                #print x
                                name =  x['name']
                                #TODO: To be re-factored
                                if x["name"] == "FILE_BYTES_READ":
                                        x = json.dumps(x)
                                        x = yaml.load(x)
                                        values_bytes=x['totalCounterValue']
                                        values_gb=values_bytes/1024/1024/1024
                                        app_data[x["name"]]={}
					app_data[x["name"]]=values_gb

                                if x["name"] == "FILE_BYTES_WRITTEN":
                                        x = json.dumps(x)
                                        x = yaml.load(x)
                                        values_bytes=x['totalCounterValue']
                                        values_gb=values_bytes/1024/1024/1024
                                        app_data[x["name"]]={}
                                        app_data[x["name"]]=values_gb

                                if x["name"] == "HDFS_BYTES_READ":
                                        x = json.dumps(x)
                                        x = yaml.load(x)
                                        values_bytes=x['totalCounterValue']
                                        values_gb=values_bytes/1024/1024/1024
                                        app_data[x["name"]]={}
                                        app_data[x["name"]]=values_gb

                                if x["name"] == "HDFS_BYTES_WRITTEN":
                                        x = json.dumps(x)
                                        x = yaml.load(x)
                                        values_bytes=x['totalCounterValue']
                                        values_gb=values_bytes/1024/1024/1024
                                        app_data[x["name"]]={}
                                        app_data[x["name"]]=values_gb

                                if x["name"] == "NUM_FAILED_MAPS":
                                        x = json.dumps(x)
                                        x = yaml.load(x)
                                        app_data[x["name"]]={}
                                        app_data[x["name"]]=x['totalCounterValue']

                                if x["name"] == "NUM_FAILED_REDUCES":
                                        x = json.dumps(x)
                                        x = yaml.load(x)
                                        app_data[x["name"]]={}
                                        app_data[x["name"]]=x['totalCounterValue']


				if x["name"] == "TOTAL_LAUNCHED_MAPS":
                                        x = json.dumps(x)
                                        x = yaml.load(x)
                                        app_data[x["name"]]={}
                                        app_data[x["name"]]=x['totalCounterValue']

                                if x["name"] == "TOTAL_LAUNCHED_REDUCES":
                                        x = json.dumps(x)
                                        x = yaml.load(x)
                                        app_data[x["name"]]={}
                                        app_data[x["name"]]=x['totalCounterValue']


                app_data["mapsCompleted"]=pending_cal["mapsCompleted"]
                app_data["reducesCompleted"]=pending_cal["reducesCompleted"]
                app_data["mapsPending"]=pending_cal["mapsPending"]
                app_data["reducesPending"]=pending_cal["reducesPending"]
                app_data["mapsRunning"]=pending_cal["mapsRunning"]
                app_data["reducesRunning"]=pending_cal["reducesRunning"]
                app_data["elapsedTime"]=float((pending_cal["elapsedTime"]/(1000*60))/60)
                
	if app_data:
		app_data["queue"] = ast.literal_eval(json.dumps(queue_name))
		app_data["user"] = ast.literal_eval(json.dumps(user_name)) 
                for val in job_data:
                        if val not in json.dumps(app_data.keys()):
                                app_data[val]="NULL"
		#print app_data
                #print app_meta
                return app_data,app_meta

#start_jobs(sys.argv[1])
