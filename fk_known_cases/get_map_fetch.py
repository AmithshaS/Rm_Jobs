#!/usr/bin/python

import sqlite3, sys, re, time
from threading import Thread



conn = sqlite3.connect('test.db')
cursor = conn.cursor()


no=sys.argv[1]
sql_q1='SELECT job_no ,job_name,application_name,time from job_details where job_name="%s"'%no
#print sql_q1
job_details = conn.execute(sql_q1)



for row in job_details:
	job_no=row[0]
	no=row[0]
	#print "no",no
	job_id=row[1]
	app_id=row[2]

maps_dict={}
def check_mapper_group():
	sql_q2 = "select count(*),map_id from reduce_map_host where job_no=%s group by map_id"%no
	cursor.execute(sql_q2)
	mapper_group =  cursor.fetchall()
	for row in mapper_group:
		task_count=row[0]
		map_id=row[1]
#		print "%d No of Reducers Fetching From %s"%(task_count,map_id)
		if not map_id in maps_dict:
			maps_dict[map_id]=task_count
	#print maps_dict
	#for i in maps_dict:
	#	print i
	max_value =  max(maps_dict.values())

	max_map_fetcher =  (key for key,value in maps_dict.items() if value==max(maps_dict.values())).next()
	print "This %s Has Been Fetched by %d Reducers "%(max_map_fetcher,max_value)
	

	print set(maps_dict.values())
				











check_mapper_group()
