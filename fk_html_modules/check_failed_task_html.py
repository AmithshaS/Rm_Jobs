#!/usr/bin/python

import sqlite3, sys, re, time, os
from threading import Thread
pwd = os.path.dirname(os.path.realpath(__file__))
script_dir = os.path.split(pwd)[0]
check_modules=script_dir+"/fk_known_cases/"
sys.path.append(check_modules)
import check_failures

def check_failed_task(no):

	conn = sqlite3.connect('test.db')
	cursor = conn.cursor()


        fetch_error=[]
        launch_error=[]
        space_error=[]

        sql_q10="SELECT job_no, task_name, attempt_name, startTime, elapsedTime,  job_type, status, nodeHttpAddress, progress, diagnostics  from attempt_counters  where job_no=%d"%no
        #print sql_q10
        cursor.execute(sql_q10)
        failed_details =  cursor.fetchall()

        if failed_details:
                for row in failed_details:
                        task_name =  row[1]
                        attempt_name =   row[2]
                        startTime=  row[3]
                        elapsedTime=  row[4]
                        job_type=  row[5]
                        status=  row[6]
                        nodeHttpAddress =  row[7]
                        progress =  row[8]
                        diagnostics =  row[9]
                        fetch_result = check_failures.fetch_check(task_name, attempt_name, diagnostics, startTime, elapsedTime, nodeHttpAddress)
                        fetch_error.append(fetch_result)
                        launch_result = check_failures.launch_failure(task_name, attempt_name, diagnostics, startTime, elapsedTime, nodeHttpAddress)
                        launch_error.append(launch_result)
                        disk_space = check_failures.disk_space_issue(task_name, attempt_name, diagnostics, startTime, elapsedTime, nodeHttpAddress)
                        if disk_space:
                                space_error.append(disk_space)

        if launch_error:
                if not None in launch_error :
                        print '<br>'
                        print '<p> Pattern 1 : Cheking For Container Launch Failures</p>'
                        print '<table style="width:100%" cellspacing="10">'
                        print '<tr><th align="left">TASK NAME</th><th align="left">ATTEMPT NAME</th><th align="left">HOST</th><th align="left">REPORT </th><th align="left">ERROR</th><th align="left">ELAPSED TIME in min </th></tr>'
                        for i in launch_error:
                                print "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%f</td></tr>"%(i[0], i[1], i[5], i[2], i[3], i[4])


                        print '</table>'


        if fetch_error :
        #       print fetch_error
                if not None in fetch_error :
                        print '<br>'
                        print '<p> Pattern 2 : Cheking For Fetch Failures </p>'
                        print '<table style="width:100%" cellspacing="10">'
                        print '<tr><th align="left">TASK NAME</th><th align="left">ATTEMPT NAME</th><th align="left">HOST</th><th align="left">REPORT </th><th align="left">ERROR</th><th align="left">ELAPSED TIME in min </th></tr>'
                        for i in fetch_error:
                                print "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%f</td></tr>"%(i[0], i[1], i[5], i[2], i[3], i[4])

                        print '</table>'


        if space_error:
                if not None in space_error:
                        print '<br>'
                        print '<p> Pattern 3 : Cheking For Disk Space Issue</p>'
                        print '<table style="width:100%" cellspacing="10">'
                        print '<tr><th align="left">TASK NAME</th><th align="left">ATTEMPT NAME</th><th align="left">HOST</th><th align="left">REPORT </th><th align="left">ERROR</th><th align="left">ELAPSED TIME in min </th></tr>'
                        #print  space_error
                        for i in space_error:
                                task_id= i[0]
                                task_out = task_counter(task_id)
                                task_id_count= "<a href=\"javascript:showhide('%s')\">%s</a><div id=\"%s\" style=\"display:none;\"><table>%s</table></div>"%(task_id,task_id,task_id,task_out)

                                print "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%f</td></tr>"%(task_id_count, i[1], i[5], i[2], i[3], i[4])


                        print '</table>'


	conn.close()










