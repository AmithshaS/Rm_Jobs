#!/usr/bin/python

import sqlite3, sys, re, time, os
from threading import Thread
pwd = os.path.dirname(os.path.realpath(__file__))
script_dir = os.path.split(pwd)[0]
check_modules=script_dir+"/fk_known_cases/"
sys.path.append(check_modules)


def job_data(no):

	conn = sqlite3.connect('test.db')
	cursor = conn.cursor()
        sql_q2="SELECT job_no,reducesCompleted, mapsPending, FILE_BYTES_WRITTEN, reducesRunning, HDFS_BYTES_READ, elapsedTime, mapsRunning, FILE_BYTES_READ, TOTAL_LAUNCHED_MAPS, TOTAL_LAUNCHED_REDUCES,HDFS_BYTES_WRITTEN,reducesPending, mapsCompleted, queue, user from job_counter where job_no=%d"%no
        #job_counter = conn.execute(sql_q2)
        cursor.execute(sql_q2)
        job_counter = cursor.fetchall()
        print '<br>'
        print "\nJOB COUNTERS\n"
        print '<br>'
        if job_counter:

		#print "<a href=\"javascript:showhide('Job Data')\">job_data</a><div id='Job Data' style=\"display:none;\">"
		print '<a href="javascript:showhide(\'Job Data\')">View Job Data</a><div id=\'Job Data\' style="display:none;">'

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
	for row in job_counter:
                #print row
                job_no=row[0]
                reducesCompleted=row[1]
                mapsPending=row[2]
                FILE_BYTES_WRITTEN=row[3]
                reducesRunning=row[4]
                HDFS_BYTES_READ=row[5]
                elapsedTime=row[6]
                mapsRunning=row[7]
                FILE_BYTES_READ=row[8]
                TOTAL_LAUNCHED_MAPS=row[9]
                TOTAL_LAUNCHED_REDUCES=row[10]
                HDFS_BYTES_WRITTEN=row[11]
                reducesPending =row[12]
                mapsCompleted =row[13]
                queue =row[14]
                user =row[15]

                print "<p>Applicaiton Running for", elapsedTime,"hrs</p>"

                if HDFS_BYTES_READ:

                        if HDFS_BYTES_READ > 1024:
				HDFS_BYTES_READ = HDFS_BYTES_READ/1024
                                HDFS_BYTES_READ="<td bgcolor=\"#FF6347\">%s TB</td>"%HDFS_BYTES_READ
                        elif HDFS_BYTES_READ > 500:
                                HDFS_BYTES_READ="<td bgcolor=\"#FF8C00\">%s GB</td>"%HDFS_BYTES_READ
                        elif HDFS_BYTES_READ < 500:
                                HDFS_BYTES_READ="<td >%s GB</td>"%HDFS_BYTES_READ

                if HDFS_BYTES_WRITTEN:

                        if HDFS_BYTES_WRITTEN > 1024:
				HDFS_BYTES_WRITTEN = HDFS_BYTES_WRITTEN/1024
                                HDFS_BYTES_WRITTEN="<td bgcolor=\"#FF6347\">%s GB</td>"%HDFS_BYTES_WRITTEN
                        elif HDFS_BYTES_WRITTEN > 500:
                                HDFS_BYTES_WRITTEN="<td bgcolor=\"#FF8C00\">%s GB</td>"%HDFS_BYTES_WRITTEN
                        elif HDFS_BYTES_WRITTEN < 500:
                                HDFS_BYTES_WRITTEN="<td >%s GB</td>"%HDFS_BYTES_WRITTEN



		print "<tr><td>Total HDFS_BYTES_READ</td>%s</tr>"%(HDFS_BYTES_READ)
                print "<tr><td>Total HDFS_BYTES_WRITTEN</td>%s</tr>"%(HDFS_BYTES_WRITTEN)
                print "<tr><td>No of Maps Running</td><td>%s</td></tr>"%(mapsRunning)
                print "<tr><td>No of Maps Pending</td><td>%s</td></tr>"%(mapsPending)
                print "<tr><td>No of Reduces Running</td><td>%s</td></tr>"%(reducesRunning)
                print "<tr><td>No of Reduces Pending</td><td>%s</td></tr>"%(reducesPending)
                print "<tr><td>No of Maps Completed</td><td>%s</td></tr>"%(mapsCompleted)
                print "<tr><td>No of Reducers Completed</td><td>%s</td></tr>"%(reducesCompleted)
                print "<tr><td>Queue Name </td><td>%s</td></tr>"%(queue)
                print "<tr><td>User Name </td><td>%s</td></tr>"%(user)


        print '</table>'
        print '</div>'

	conn.close()










