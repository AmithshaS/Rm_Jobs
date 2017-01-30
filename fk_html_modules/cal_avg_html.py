#!/usr/bin/python

import sqlite3, sys, re, time, os
from threading import Thread
pwd = os.path.dirname(os.path.realpath(__file__))
script_dir = os.path.split(pwd)[0]
check_modules=script_dir+"/fk_known_cases/"
sys.path.append(check_modules)




conn = sqlite3.connect('test.db')
cursor = conn.cursor()


def cal_avg(no):
        sql_q5="SELECT job_no,avg_reduce_time, max_reduce_time, avg_shuffle_input, max_shuffle_input, avg_map_time, max_map_time from task_avg_time_bytes where job_no=%d"%no
        avg_cal = conn.execute(sql_q5)
        print '<br>'
        print "<p>AVERAGE & MAXIMUM VALUES</p>"
        print "<table>"
        for row in avg_cal:
                job_no=row[0]
                avg_reduce_time=row[1]
                max_reduce_time=row[2]
                avg_shuffle_input=row[3]
                max_shuffle_input=row[4]
                avg_map_time=row[5]
                max_map_time =row[6]

#               print job_no, avg_reduce_time, max_reduce_time, avg_shuffle_input, max_shuffle_input, avg_map_time, max_map_time


                print '<tr><td>AVG MAP TIME</td><td>%s</td></tr>'%(avg_map_time)
                print '<tr><td>MAX MAP TIME</td><td>%s</td></tr>'%(max_map_time)
                print '<tr><td>AVG REDUCE TIME</td><td>%s</td></tr>'%(avg_reduce_time)
                print '<tr><td>MAX REDUCE TIME</td><td>%s</td></tr>'%(max_reduce_time)
                print '<tr><td>AVG SHUFFLE INPUT</td><td>%s</td></tr>'%(avg_shuffle_input)
                print '<tr><td>MAX SHUFFLE INPUT</td><td>%s</td></tr>'%(max_shuffle_input)
                print '</table>'


