#!/usr/bin/python

from __future__ import division
from types import ModuleType
import os, sys
pwd = os.path.dirname(os.path.realpath(__file__))
script_dir = os.path.split(pwd)[0]
configs_path=script_dir+"/configs/hosts.conf"

import requests
import datetime
import time
import os 
from datetime import timedelta  
import re
import numpy as np
import subprocess
import ast, json
import ConfigParser
configParser = ConfigParser.RawConfigParser()
configParser.read(configs_path)
resource_manager_add = configParser.get('resource_manager', 'rm01')

		


##check host space 
def space_check(host):
	data=[]
	host_name =  host.split(':')[0]
	#print "It came "
	cmd = "ssh -o StrictHostKeyChecking=no %s  'df -h | grep /dev/vd'"%host_name
        check_space=subprocess.Popen(cmd,stdout=subprocess.PIPE,shell=True,stderr=subprocess.PIPE)
	#print cmd
        #cmd_output,cmd_error = check_space.communicate()
	cmd_output= filter(lambda x:len(x)>0,(line.strip() for line in check_space.stdout))
        #print cmd_output
        check_type = "SPACE"
	#print cmd_output
	if cmd_output:
		title="<tr><th>Disk</th><th>Used %</th><th>Available Size</th></tr>"
		#data.append(title)
		data.append("<table>")
		data.append(title)
		for i in  cmd_output:
			disk_data = ' '.join(i.split()).split(' ')
		#	print disk_data
		#	print disk_data[5], disk_data[4], disk_data[3]
			disk_d = "<tr><td>%s</td><td> %s</td><td> %s</td></tr>"%(disk_data[5],disk_data[4],disk_data[3])
			data.append(disk_d)
			#data.append(disk_data[5], disk_data[4], disk_data[3])
		data.append("</table>")
	if data:
		#print ", ".join(data)
		return ", ".join(data).replace(',',' ')
	#	print "</table>"


##check the host for RO Disk 

def ro_check(host):
	cmd = "ssh -o StrictHostKeyChecking=no %s mount | grep ro "%host
        check_dmesg=subprocess.Popen(cmd,stdout=subprocess.PIPE,shell=True,stderr=subprocess.PIPE)
        #output,cmd_error = check_dmesg.communicate()
	#print output
	check_type = "RO"
	ro_disk=[]
	for i in  check_dmesg.stdout:
		if "/dev/v" in i:
			i =  i.split(' ')
			disk = i[0]
			issue = "RO"	
			ro_disk.append(disk)
	if not ro_disk:
		msg = "No Error"
		return  msg
	else :
		return  ro_disk


##check the host health by cpu nmi and dmesg 
def check(host):
	disk_error=[]
	cpu_error=[]
	uptime=[]
	check_type = "DMESG"
#	print "<p>Checking %s for dmesg and cpu nmi</p>"%host
	#print host
	cmd = "ssh -o StrictHostKeyChecking=no %s dmesg -T"%host
	check_dmesg=subprocess.Popen(cmd,stdout=subprocess.PIPE,shell=True,stderr=subprocess.PIPE)
        output,cmd_error = check_dmesg.communicate()
#	print error	
#	print cmd_output

### Check disk RO 
	ro_disks_checks = ro_check(host)


	if output:
        	output = output[0]
	        for i in output.split('\n'):
	                out = i
	                i_o = re.findall(r'I/O error',i)
	                cpu_nmi = re.findall(r'sending NMI to all CPUs',i)
        	        if i_o :
                	        i = re.match(r'^.*\[.*\]',i)
                        	if i :
                	                i =  i.group(0)
                    	      	      	i = re.sub(r'\[','',i)
	                      	        i = re.sub(r'\]','',i)
	                       	        FMT='%a %b %d %H:%M:%S %Y'
	                                log_time  =  datetime.strptime(i, FMT)
        	                        now = datetime.now()
                	                if log_time.year == now.year:
                        	                FMT1="%Y-%m-%d %H:%M:%S"
                                	        diff =  now - log_time
                                 	        diff_in_sec =  diff.total_seconds()
                                    		if diff_in_sec<21600:
	                                                #print 'Error : dmesg disk error is Less than 6hrs'
							disk_dmesg_error="Error : dmesg disk error is Less than 6hrs"
        	                                        disk_error.append("yes")
                	                                #break

			if cpu_nmi :
        	                i = re.match(r'^.*\[.*\]',i)
                	        if i :
                        	        i =  i.group(0)
                      		        i = re.sub(r'\[','',i)
                 	                i = re.sub(r'\]','',i)
	                                FMT='%a %b %d %H:%M:%S %Y'
        	                        log_time  =  datetime.strptime(i, FMT)
                	                now = datetime.now()
                        	        FMT1="%Y-%m-%d %H:%M:%S"
                         	        if log_time.year == now.year:
	                                        diff =  now - log_time
        	                                diff_in_sec =  diff.total_seconds()
                	                        if diff_in_sec < 21600:
							#print 'cpu nmiError : dmesg CPU NMI error is Less than 6hrs'
                        	                        #print 'Error : dmesg CPU NMI error is Less than 6hrs'
							cpu_dmesg_error="Error : dmesg CPU NMI error is Less than 6hrs"
                                	                cpu_error.append("yes")
                                        	        #break
		
		#print disk_error,cpu_error
		if  len(disk_error) == 0 and len(cpu_error) == 0:
	#		print "Host RO Erro CPU Error Disk Error"
			#print '%s %s No Error No Error'%(host,ro_disks_checks)
			return host, ro_disks_checks, "No Error", "No Error"

		if len(disk_error) == 0 and len(cpu_error) != 0 :
                        return host, ro_disks_checks, "No Error", disk_dmesg_error
		if len(disk_error) != 0 and len(cpu_error) == 0 :
			return host, ro_disks_checks, cpu_dmesg_error, "No Error"


	else:
#		print "Command Failed"
		return host, ro_disks_checks, "Command Failed", "Command Failed"
	
def get_host(appid,jobid,taskid):
	hosts=[]
	host_container={}
        attempts=requests.get('http://%s/proxy/%s/ws/v1/mapreduce/jobs/%s/tasks/%s/attempts'%(resource_manager_add,appid,jobid,taskid) , headers = {'ACCEPT':'application/json'}).json().get('taskAttempts')
        attempts=ast.literal_eval(json.dumps(attempts))

#       print "<p>Reducer running in </p>"
        for attempt in attempts:
                for i in attempts[attempt]:
			if i['state']=="RUNNING" :
	                        host = i['nodeHttpAddress']
        	                host = host.split(':')[0]
                	        if not host in host_container:
          	                      host_container[host]={}
				if not i['assignedContainerId'] in host_container[host]:
					host_container[host][i['assignedContainerId']]={}
	
				host_container[host][i['assignedContainerId']]['taskid']=taskid
				host_container[host][i['assignedContainerId']]['startTime']=i['startTime']

				#print 'Name Value'
				#print "nodeHttpAddress","%s"%i['nodeHttpAddress']
				#print "assignedContainerId","%s"%i['assignedContainerId']
				if not host in hosts:
					hosts.append(host)
			
	#print hosts
	return hosts,host_container
#check(sys.argv[1])
#space_check(sys.argv[1])
