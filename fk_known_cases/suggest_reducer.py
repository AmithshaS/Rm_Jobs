#!/usr/bin/python

import sqlite3, sys, re, time, os, requests
import decimal
import threading 
from threading import Thread
pwd = os.path.dirname(os.path.realpath(__file__))
script_dir = os.path.split(pwd)[0]
sys.path.append(script_dir)

db_file =  script_dir+"/test.db"
conn = sqlite3.connect(db_file)
cursor = conn.cursor()


current_time=int(time.time() * 1000)




disk_out={}
high_util_disk=[]
high_await_disk=[]
disk=["vda","vdb","vdc","vdd","vde","vdf","vdg","vdh","vdi","vdj","vdk","vdl"]


def reducer_data_size(task):
	#print task

	sql_q1 = "select task_name, file_bytes_written, file_bytes_read, hdfs_bytes_written, hdfs_bytes_written, reduce_shuffle_bytes from task_counters where  task_name = '%s'"%task
	get_task_data=conn.execute(sql_q1)
	for data in get_task_data:
		task_name = data[0]
		hdfs_read = data[3]
		hdfs_write = data[4]
		reduce_shuffle = data[5]
		if reduce_shuffle :
			sql_q2 = "select task_id ,progress ,status ,elapsed_time from running_tasks where  task_id = '%s'"%task
			get_task_details=conn.execute(sql_q2)
			for task_data in get_task_details:
			#	task_name = task_data[0]
				progress = task_data[1]
				status = task_data[2]
				elapsed_time = task_data[3]
				#print elapsed_time
				if elapsed_time > 60:
					elapsed_time = elapsed_time/60
	#		reduce_shuffle = round(reduce_shuffle)		
			return (task_name, reduce_shuffle, hdfs_write,  elapsed_time)			
			#print "%s with %f GB of Reduce Shuffle Input and %f GB of HDFS Write Running for %s Hrs"%(task_name, reduce_shuffle, hdfs_write,  elapsed_time)


			

		



def disks_util_await(disk_data):

	#for row in disk_data:
                #print row
                job_no=disk_data[0]
                host=disk_data[1]
                #print host
                disks_error=disk_data[2]
                disk_issue=disk_data[3]
                cpu_issue=disk_data[4]
                task_time=disk_data[5]
                tasks_running=disk_data[6]


                if not ( disks_error == "NO RO DISKS" and "No Error" in disk_issue) :
                #if not disks_error == "NO RO DISKS" : 
                        for i in disk:
                                #print "Disk =", i 
                                await_time_url="http://10.47.0.183/api/query?start=2h-ago&end=5m-ago&m=avg:1m-avg:prod-fdphadoop.iostat.await.time{host=%s,dev=%s}&fomrat=ascii"%(host, i)
                                util_per_url="http://10.47.0.183/api/query?start=2h-ago&end=5m-ago&m=avg:1m-avg:prod-fdphadoop.iostat.util.per{host=%s,dev=%s}&fomrat=ascii"%(host, i)
                                #print await_time_url
                                #print util_per_url

                                disk_util_per = requests.get(await_time_url, headers={'ACCEPT' : 'application/json'}).json()
                                disk_util_per  =  disk_util_per [0]
                                util_per      = disk_util_per ['dps']
                                #print util_per
                                #print await_time.values()
                                total_util     = len(util_per.values())
                                count_high_util= sum(n > 95 for n in util_per.values())
                                low_util = total_util - count_high_util



                                if not host in disk_out:
                                        disk_out[host]={}
                                if not "util" in disk_out[host]:
                                        disk_out[host]["util"]=[]
                                if not "await" in disk_out[host]:
                                        disk_out[host]["await"]=[]

                                if count_high_util > low_util  :
                                        #print "%s Disk is Highly Utilised  "%i
                                        disk_out[host]["util"].append(i)

				disk_await_time = requests.get(await_time_url, headers={'ACCEPT' : 'application/json'}).json()
                                disk_await_time =  disk_await_time[0]
                                await_time      = disk_await_time['dps']

                                #print await_time.values()
                                total_await     = len(await_time.values())
                                count_high_await= sum(n > 40 for n in await_time.values())
                                low_await_time = total_await - count_high_await
				#print count_high_await, low_await_time
                                if count_high_await > low_await_time  :
                                        #print "%s Disk got high Await Time "%i
                                        disk_out[host]["await"].append(i)




def get_io_link(task_time,host):
        link="http://10.47.4.24/dashboard/db/bheema-disk-utilisation?panelId=12&fullscreen&edit&from=%s&to=%s&var-host=%s"%(task_time,current_time,host)
        return link


def disk_util_await(no):
	sql_q2="SELECT job_no, host, ro_disk, disk_issue, cpu_issue, task_time,tasks_running from host_status where job_no=%s"%no
	#print sql_q2
	get_host_data=conn.execute(sql_q2)
	#disks_util_await(get_host_data)
#	t = "thread"

	for data in get_host_data:
		#print data
		#disks_util_await(data)
		t = threading.Thread(target=disks_util_await,args=(data,))
		t.start()
	t.join()

	#print   disk_out
	return  disk_out



#disk_util_await(sys.argv[1])
#reducer_data_size(sys.argv[1])
