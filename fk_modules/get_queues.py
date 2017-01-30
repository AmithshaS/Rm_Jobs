#!/usr/bin/python

from __future__ import division
import sys
sys.path.append('/home/amithsha.s/python')
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

def get_queue_parent():
        cmd = 'http://prod-fdphadoop-bheema-rm-0001:8088/jmx'
        app_details=requests.get(cmd, headers = {'ACCEPT':'application/json'}).json()
	print app_details.keys()
	for i in app_details:
		for x in app_details[i]:
#			if x
			for y in x:
				#print y
				if y == "name":
#					print x[y]
					if "QueueMetrics" in x[y]:
						#print "****************************"
	#					print x[y]
	#					print x["modelerType"]
						print x["tag.Queue"]
						#print "****************************"




get_queue_parent()

