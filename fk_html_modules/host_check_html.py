#!/usr/bin/python

import sqlite3, sys, re, time, os
from threading import Thread
pwd = os.path.dirname(os.path.realpath(__file__))
script_dir = os.path.split(pwd)[0]
check_modules=script_dir+"/fk_known_cases/"
sys.path.append(check_modules)


current_time=int(time.time() * 1000)


def get_io_link(task_time,host):
        link="http://10.47.4.24/dashboard/db/bheema-disk-utilisation?panelId=12&fullscreen&edit&from=%s&to=%s&var-host=%s"%(task_time,current_time,host)
        return link


def host_check(no):

	conn = sqlite3.connect('test.db')
	cursor = conn.cursor()
	print "<p>HOST CHECK FOR  Disk & CPU Error in dmesg </p>"
        sql_q6="SELECT job_no, host, ro_disk, disk_issue, cpu_issue, task_time,tasks_running from host_status where job_no=%s"%no
        #print sql_q6
        host_get=conn.execute(sql_q6)

        if host_get:
                print '<table style="width:80%">'
                print '<tr><th align="justify">Host</th><th align="justify">DISK ISSUE</th><th align="justify">DISK DMESG</th><th align="justify">CPU DMESG</th><th align="justify">DISK I/O URL</th><th align="justify">TASK RUNNING</th></tr>'

                print '<script type="text/javascript">'
                print "function showhide(id) {"
                print "var e = document.getElementById(id);"
                print "e.style.display = (e.style.display == 'block') ? 'none' : 'block';"
                print '}'
                print '</script>'
                disk_html=[]
                for row in host_get:
                        #print "HERE",row
                        job_no=row[0]
                        host=row[1]
                        disks_error=row[2]
                        disk_issue=row[3]
                        cpu_issue=row[4]
                        task_time=row[5]
                        tasks_running=row[6]

                        if not ("No Error" in cpu_issue or "No Error" in disk_issue or disks_error == "No Error") :
        #                       print "\n\n\n\nNo Error in Disk"

                                if disks_error == "No Error" :
                                        disk_error_sign="&#9989;"

				elif disks_error == "Command Failed" :
                                        disk_error_sign="&#10071;"

                                else:
                                        disk_error_sign=disks_error

                                if disk_issue == "No Error" :
                                        disk_sign="&#9989;"

                                elif disk_issue == "Command Failed" :
                                        disk_sign="&#10071;"

                                else:
                                        disk_sign="&#10060;"

                                if cpu_issue == "No Error" :
                                        cpu_sign = "&#9989;"

                                elif cpu_issue == "Command Failed" :
                                        cpu_sign="&#10071;"
                                else:
                                        cpu_sign="&#10060;"

                                link = get_io_link(task_time,host)
                                #if link:
                                link_get = '<a href="%s" target="_blank">host disk utilisation</a>'%link
                                #print link_get
                                link_out= "%s"%(link_get)
                                disk_html="yes"
                                task_url="<a href=\"javascript:showhide('%s')\">Task show/hide.</a><div id=\"%s\" style=\"display:none;\"><p>%s</p></div>"%(host,host,tasks_running)
                                print "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"%(host, disk_error_sign, disk_sign, cpu_sign, link_out, task_url)

                if not disk_html:
                        print "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"%("ALL RUNNING TASKS HOST", "GOOD", "GOOD", "GOOD", "NULL", "NULL")
                print "</table>"


	conn.close()










