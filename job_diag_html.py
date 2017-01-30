#!/usr/bin/python

import sqlite3, sys, re, time, os
from threading import Thread
pwd = os.path.dirname(os.path.realpath(__file__))
check_modules=pwd+"/fk_known_cases/"
html_modules=pwd+"/fk_html_modules/"
sys.path.append(html_modules)
sys.path.append(check_modules)
import check_failures
import suggest_reducer
import job_data_html
import check_failed_task_html
import host_check_html
import get_disk_details_html
import get_task_logs
import task_data_html
import cal_avg_html
import task_attempts_html
import check_reducer_stage_html
import reducer_comments_html

 
conn = sqlite3.connect('test.db')
cursor = conn.cursor()


no=sys.argv[1]
sql_q1='SELECT job_no ,job_name,application_name,time from job_details where job_name="%s"'%no
job_details = conn.execute(sql_q1)


for row in job_details:
	job_no=row[0]
	no=row[0]
	#print "no",no
	job_id=row[1]
	app_id=row[2]
	app_time=row[3]
	#print no

print "<!DOCTYPE html>"
print "<html>"
print "<p style=\"color:red;\" align=\"right\"><font size=\"3\">**JOB DIGNOSIS REPORT</p>"
print "<p align=\"right\">DB Last Updated Time %s</p>"%app_time

app_url="http://prod-fdphadoop-bheema-rm-0001:8088/proxy/%s"%app_id
print '<h1>APPLICATION ID: <a href="%s" target="_blank">'%app_url,app_id,"</a></h1>"

print '</head>'
print " <p><font size=\"2\" color=\"red\">** note: data size's are in GB & Time in min **</font></p>"
print '<style>'
print 'p.serif {'
print 'font-family: "Times New Roman", Times, serif;'
print '}'

print 'p.sansserif {'
print 'font-family: Arial, Helvetica, sans-serif;'
print '}'
print '</style>'
print '<body><font face="verdana">'

job_data_html.job_data(no)
check_failed_task_html.check_failed_task(no)
task_attempts_html.map_attempt_data(no)
cal_avg_html.cal_avg(no)
task_data_html.task_data(no,app_id)
task_attempts_html.task_attempt(no)
check_reducer_stage_html.check_reduce_stage(no)
host_check_html.host_check(no)
get_disk_details_html.get_disk_details(no)
reducer_comments_html.get_reducer_comments(no)


conn.close()

print '</body></font>'
print '</html>'
