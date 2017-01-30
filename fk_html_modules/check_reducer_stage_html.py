#!/usr/bin/python

import sqlite3, sys, re, time, os, json
from threading import Thread
pwd = os.path.dirname(os.path.realpath(__file__))
script_dir = os.path.split(pwd)[0]
check_modules=script_dir+"/fk_known_cases/"
sys.path.append(check_modules)



conn = sqlite3.connect('test.db')
cursor = conn.cursor()



def check_reduce_stage(no):


	sql_q3="SELECT job_no, task_id, progress, type, status, start_time, elapsed_time from running_tasks where job_no=%d"%no
        cursor.execute(sql_q3)
        running_tasks =  cursor.fetchall()
#       running_tasks = conn.execute(sql_q3)
#       print "\n"


        for row in running_tasks:
                progress=row[2]
                type=row[3]
                if progress <= 34 and type == "REDUCE":
                        print '<br>'
                        print "<p>CHECK REDUCER IN FETCHING PHASE</p>"
                        print '<table style="width:80%">'
                        print '<tr><th align="justify">REDUCER TASK ID</th><th align="justify">FETCHING FROM </th></tr>'
                        pass
	for row in running_tasks:
                no=row[0]
                task_id=row[1]
                progress=row[2]
                type=row[3]
                status=row[4]
                if type == "REDUCE":
                        if progress <= 34:
                                print "<br>"
#                               print "\nCHECKING REDUCER STAGE FOR %s\n"%task_id
#                               print "REDUCER IS IN FETCHING PHASE"
                                sql_q11="select host from  reduce_map_host  where task_id='%s' and job_no='%s'"%(task_id,no)
                                #print sql_q11
                                map_host=conn.execute(sql_q11)
                                for x in map_host:
                                        host=x[0]

                                        sql_q13="SELECT job_no, task_id, start_time from running_tasks where job_no='%d' and task_id='%s'"%(no,task_id)
#                                       print sql_q13
                                        running_tasks = conn.execute(sql_q13)
                                        #print "\nRUNNING TASKS DETAILS\n"
                                        for row in running_tasks:
                                                job_no=row[0]
                                                task_id=row[1]
                                                start_time=row[2]
                                        if host:

                                                #print "%s --> Fetching from Mapper Host --> %s"%(task_id,host)
#                                               print "GETTING HOST WHERE REDUCER IS READING"
#                                               print "Mapper host:",host
                                                print "<tr><td>%s</td><td>%s</td</tr>"%(task_id,host)
                                                #print host
                                                #get_io_link(start_time,host)

                        #else:
                        #       print "<tr><td>NULL</td><td>NULL</td</tr>"
        print '</table>'


