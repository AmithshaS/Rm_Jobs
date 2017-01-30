#!/usr/bin/python

import sqlite3, sys, re, time, os, json
from threading import Thread
pwd = os.path.dirname(os.path.realpath(__file__))
script_dir = os.path.split(pwd)[0]
check_modules=script_dir+"/fk_known_cases/"
sys.path.append(check_modules)
import suggest_reducer


conn = sqlite3.connect('test.db')
cursor = conn.cursor()


def get_reducer_comments(no):
        sql_q3="SELECT task_id from running_tasks where job_no=%d"%no
        running_tasks = conn.execute(sql_q3)
        for tasks in running_tasks:
                task = tasks[0]
                task_data = suggest_reducer.reducer_data_size(task)
                if task_data:
                        #print task_data
                        task_name = task_data[0]
                        reduce_shuffle = task_data[1]
                        hdfs_write = task_data[2]
                        elapsed_time  = task_data[3]
                        print "<p><b> %s with %f GB of Reduce Shuffle Input and %f GB of HDFS Write Running for %s Hrs </b></p>"%(task_name, reduce_shuffle, hdfs_write,  elapsed_time)

