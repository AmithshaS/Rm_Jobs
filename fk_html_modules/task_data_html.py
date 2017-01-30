#!/usr/bin/python

import sqlite3, sys, re, time, os, json
from threading import Thread
pwd = os.path.dirname(os.path.realpath(__file__))
script_dir = os.path.split(pwd)[0]
check_modules=script_dir+"/fk_known_cases/"
sys.path.append(check_modules)



conn = sqlite3.connect('test.db')

def task_counter(no, task):
        print task

        sql_q4="SELECT job_no, task_name, file_bytes_written, file_bytes_read, hdfs_bytes_written, reduce_shuffle_bytes, hdfs_bytes_read from task_counters where job_no=%d and task_name='%s'"%(no,task)
        #print sql_q4
        task_counter = conn.execute(sql_q4)
        #print "\nTASK COUNTER \n"
        for row in task_counter:
                job_no=row[0]
                task_name=row[1]
                file_bytes_written=row[2]
                file_bytes_read=row[3]
                hdfs_bytes_written=row[4]
                reduce_shuffle_bytes=row[5]
                hdfs_bytes_read=row[6]

                return  "<tr><td>HDFS_BYTES_WRITTEN </td><td>%s</td></tr> <tr><td>HDFS_BYTES_READ </td><td>%s</td></tr><tr><td>reduce_shuffle_bytes</td><td>%s</td></tr>"%(hdfs_bytes_written,hdfs_bytes_read,reduce_shuffle_bytes)





def task_data(no, app_id):
        sql_q3="SELECT job_no, task_id, progress, type, status, start_time, elapsed_time from running_tasks where job_no=%d"%no

        #sql_q3="SELECT job_no, task_id, progress, type, status, start_time, elapsed_time from running_tasks where job_no=%d"%no

        running_tasks = conn.execute(sql_q3)
        print '<br>'
        print "RUNNING TASKS DETAILS"

        if running_tasks:
                print '<script type="text/javascript">'
                print "function showhide(id) {"
                print "var e = document.getElementById(id);"
                print "e.style.display = (e.style.display == 'block') ? 'none' : 'block';"
                print '}'
                print '</script>'
                print '<table style="width:70%">'
                print '<style>'
                print 'table, th, td {'
                print 'border: 1px solid black;'
                print 'border-collapse: collapse;'
                print '}'
                print 'th, td {'
                print 'padding: 5px;'
                print '}'
                print '</style>'
                print '<tr><th align="justify">TaskId</th><th align="justify">Task Type</th><th align="justify">Progress</th><th align="justify">startTime</th><th align="justify">elapsedTime</th><th align="justify">Logs</th></tr>'
                for row in running_tasks:
                #       print row
                        job_no=row[0]
                        task_id=row[1]
                        progress=row[2]
                        task_type=row[3]
                        status=row[4]
                        start_time=row[5]
                        str_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time/1000))
                        elapsed_time=row[6]

#                print "task_id", task_id,"\tstartTime",str_time,"\telapsedTime",elapsed_time
                        task_out = task_counter(no, task_id)
                        log_url="http://prod-fdphadoop-bheema-rm-0001:8088/proxy/%s/mapreduce/task/%s"%(app_id,task_id)
                        task_log_url='<a href="%s" target="_blank">Task logs</a>'%log_url

                        task_id_count= "<a href=\"javascript:showhide('%s')\">%s</a><div id=\"%s\" style=\"display:none;\"><table>%s</table></div>"%(task_id,task_id,task_id,task_out)

                        print "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"%(task_id_count,task_type,progress,str_time,elapsed_time,task_log_url)

                print "</table>"

