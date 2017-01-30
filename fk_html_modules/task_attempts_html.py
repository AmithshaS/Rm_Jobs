#!/usr/bin/python

import sqlite3, sys, re, time, os, json
from threading import Thread
pwd = os.path.dirname(os.path.realpath(__file__))
script_dir = os.path.split(pwd)[0]
check_modules=script_dir+"/fk_known_cases/"
sys.path.append(check_modules)



conn = sqlite3.connect('test.db')
cursor = conn.cursor()





attempt_get_dict={}
def attempt_counters(no):
        sql_q7="SELECT job_no, task_name, job_type, status, nodeHttpAddress, progress, diagnostics from attempt_counters where job_no=%d"%no
        attempt_get=conn.execute(sql_q7)
        #print "\n"

        for row in attempt_get:
                job_no=row[0]
                task_name=row[1]
                job_type=row[2]
                status=row[3]
                nodeHttpAddress=row[4]
                progress=row[5]
                diagnostics=row[6]
                task_name_get=task_name.split('_')
                task_own_name="task_"+task_name_get[1]+"_"+task_name_get[2]+"_"+task_name_get[3]+"_"+task_name_get[4]
                if not task_own_name in attempt_get_dict:
                        attempt_get_dict[task_own_name]={}
                if not task_name in attempt_get_dict[task_own_name] :
                        attempt_get_dict[task_own_name][task_name]={}

                attempt_get_dict[task_own_name][task_name]["job_type"]=job_type
                attempt_get_dict[task_own_name][task_name]["status"]=status
                attempt_get_dict[task_own_name][task_name]["nodeHttpAddress"]=nodeHttpAddress
                attempt_get_dict[task_own_name][task_name]["progress"]=progress
                attempt_get_dict[task_own_name][task_name]["diagnostics"]=diagnostics









def task_attempt(no):
        sql_q8="select job_no,task_name, task_type, attempt_count from attempt_count where job_no=%d"%no

        cursor.execute(sql_q8)
        count_get =  cursor.fetchall()
        count_get=conn.execute(sql_q8)

        for row in count_get:
                attempt_count = row[3]
                if attempt_count > 0:
                        print "<br>"
                        print "\nCHECKING FOR FAILED TASK ATTEMPTS"
                        print '<table style="width:100%" cellspacing="10">'
                        print '<col width="30"><col width="20"><col width="20"><col width="100">'
                        print '<tr><th align="left">TASK NAME</th><th align="left">TASK TYPE</th><th align="left">TOTAL COUNTS</th><th align="left">SYSTEM diagnostics</th></tr>'
                        pass

        attempt_counters(no)
        for row in count_get:
                task = row[1]
                task_type = row[2]
                attempt_count = row[3]
#               print task,job_type,attempt_count
                if attempt_count > 0:
                        #print "\nFound Failed Attemt for %s\n"%task
#                       
                        for x in  attempt_get_dict[task]:
                                if attempt_get_dict[task][x]['diagnostics']:
#                                       print "Found System diagnostics for %s - %s"%(task,x)
                                        print "<tr><td>%s</td><td>%s</td><td align='center'>%s</td><td style=\"font-size:10px\">%s</td></tr>"%(task,task_type,attempt_count,attempt_get_dict[task][x]['diagnostics'])
                #else:
                        #pass

        print "</table>"



def map_attempt_data(no):


        sql_q9="select  job_no, task_name, attempt_name, elapsedTime, nodeHttpAddress, progress, diagnostics from map_failed_attempts where job_no=%d"%no
        cursor.execute(sql_q9)
        attempt_get =  cursor.fetchall()
        if attempt_get:
                report = "Container Launch Failure"
                print '<br>'
                print '<p> Pattern 3 : Checking For Completed Mappers with Failed  Attempts </p>'
                print '<table style="width:100%" cellspacing="10">'
                print '<tr><th align="left">TASK NAME</th><th align="left">ATTEMPT NAME</th><th align="left">HOST</th><th align="left">REPORT </th><th align="left">ERROR</th><th align="left">ELAPSED TIME in min </th></tr>'
                for row in attempt_get:
                        task_id = row[1]
                        attempt_id = row[2]
                        #startTime = row[3]
                        elapsedTime = row[3]
                        host = row[4]
                        progress = row[5]
                        diagnosis  = row[6]
                        print "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%f</td></tr>"%(task_id, attempt_id, host, report, diagnosis, elapsedTime)


                print '</table>'
