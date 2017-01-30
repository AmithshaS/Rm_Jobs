#!/usr/bin/python

from __future__ import division
import sys, os
pwd = os.path.dirname(os.path.realpath(__file__))
#print pwd
script_dir = os.path.split(pwd)[0]
configs_path=script_dir+"/configs/hosts.conf"
#print configs_path

import requests, ast
import os,sys
import json,ast
import yaml
import re
import ConfigParser
configParser = ConfigParser.RawConfigParser()   
configParser.read(configs_path)
resource_manager_add = configParser.get('resource_manager', 'rm01')



def get_queues():
        cmd = 'http://%s/ws/v1/cluster/scheduler'%(resource_manager_add)
	queue_stats=requests.get(cmd, headers = {'ACCEPT':'application/json'}).json().get('scheduler')
#	queue_stats = ast.literal_eval(json.dumps(queue_stats))



## Queues are divided in to 3 levels 
## Parent 
## Parent Leaf 
## Child Leaf
## Returns Json from API Call It contains Many Headers
	for i in queue_stats:
		for datas in queue_stats[i]:
#			print datas
			if datas == "queues":
				for queue_data in queue_stats[i][datas]:

## Here it will start showing the main parent level queuws 

					for data in queue_stats[i][datas][queue_data]:
#						print data['queueName']
#							if data['queueName'] == "main":
							'''for data_stats	in data:
#								print data_stats
								if not data_stats == "queues": 
									if not data_stats == "resourcesUsed":
										print "%s \t: \t"%data_stats, data[data_stats]
								if data_stats == "resourcesUsed":
									for i in data[data_stats]:
										print "%s \t\t: \t"%i,data[data_stats][i]'''

## Here it will start showing the main level Leafs, Here we can get the datas by mentioned the key name 
 	
							for main_leafs in  data['queues']['queue']:
								#print data['queues']['queue']["queueName"]
								#print json.dumps(main_leafs)
								'''print "\n"
								print "\tLeaf Name \t\t: \t", main_leafs['queueName']
								print "\tabsoluteUsedCapacity \t: \t", main_leafs['absoluteUsedCapacity']
								print "\tusedCapacity \t\t: \t", main_leafs['usedCapacity']
								print "\n"'''
							
## Here it will start showing the child leafs

								for child_queues in main_leafs['queues']['queue']:
									if child_queues['queueName'] == app_queue_name :
										print "Queue Main \t\t: \t", data['queueName']
										print "\tLeaf Name \t\t: \t", main_leafs['queueName']
										print "\tabsoluteUsedCapacity \t: \t", main_leafs['absoluteUsedCapacity']
										print "\tusedCapacity \t\t: \t", main_leafs['usedCapacity']
										print "\n"
#									print child_queues
										for stats in child_queues:
#											print stats
											if not stats == "users" or stats == "userAMResourceLimit" or stats == "nodeLabels" or stats == "resourcesUsed" or stats == "AMResourceLimit" or stats == "usedAMResource":
#											if not stats == "users" or "userAMResourceLimit" :
												print "\t\t %s \t\t\t:\t"%stats, child_queues[stats]
												##break
									#	print "\n"
									'''print "\t\tChild Name \t\t: \t",child_queues['queueName']
									print "\t\tabsoluteUsedCapacity \t: \t", child_queues['absoluteUsedCapacity']
									print "\t\tusedCapacity \t\t: \t", child_queues['usedCapacity']
									print "\t\tnumPendingApplications \t: \t", child_queues['numPendingApplications']
									print "\n"									
									
								print "\n"'''





app_queue_name = sys.argv[1]
get_queues()

