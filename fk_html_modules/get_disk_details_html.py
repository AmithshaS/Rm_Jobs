#!/usr/bin/python

import sqlite3, sys, re, time, os, json
from threading import Thread
pwd = os.path.dirname(os.path.realpath(__file__))
script_dir = os.path.split(pwd)[0]
check_modules=script_dir+"/fk_known_cases/"
sys.path.append(check_modules)
import suggest_reducer



def get_disk_details(no):


	current_time=int(time.time() * 1000)

	conn = sqlite3.connect('test.db')
	cursor = conn.cursor()

        data_datas = suggest_reducer.disk_util_await(no)
#       print data_datas
        disk_data_html=[]

        if data_datas:
                print '<br>'
                print '<p><b> Recommendations</b> </p>'
                print '<br>'
                print '<table style="width:100%" cellspacing="10">'
                print '<tr><th align="left">HOST</th><th align="left">TASKS</th><th align="left">Highly Utilised Disk</th><th align="left">Disk with High Await Time</th></tr>'

                for i in data_datas:
                        #print data_datas[i].values()
                        util = data_datas[i].values()[0]
                        await = data_datas[i].values()[1]
                        if util or await:
                                if not util :
                                        util = "Good"
                                if not await:
                                        await = "Good"
                                disk_data_html="yes"
        #                       print util, await      
				sql_q12 = "select tasks_running, task_time from host_status where host='%s' and job_no=%s"%(i,no)
                                cursor.execute(sql_q12)
                                tasks =  cursor.fetchall()
				tasks_data =  list(tasks)[0]
				task_start_time =  tasks_data[1]
				tasks = json.dumps(tasks_data[0])
				#print task_start_time
				util_link="http://10.47.4.24/dashboard/db/bheema-disk-utilisation?panelId=12&fullscreen&from=%s&to=%s&var-host=%s"%(task_start_time,current_time,i)
				await_link="http://10.47.4.24/dashboard/db/bheema-disk-utilisation?panelId=13&fullscreen&from=%s&to=%s&var-host=%s"%(task_start_time,current_time,i)

				#print util_link
				#print await_link
                                #print data_datas[i].values()
                                print '<tr><td>%s</td><td>%s</td><td>%s <a href=%s target="_blank"> util link</a></td><td>%s <a href=%s target="_blank"> await link</a></td></tr>'%(i, tasks, util,util_link, await, await_link)

                if not disk_data_html:
                        print "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"%("ALL RUNNING TASKS HOSTS", "ALL", "GOOD", "GOOD")

                print "</table>"


	conn.close()










