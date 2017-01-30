#!/usr/bin/python

import os,subprocess 

from flask import Flask, render_template, request, url_for, redirect

app = Flask(__name__)

@app.route('/')
def form():
    return render_template('form_submit.html')



@app.route('/apps/', methods=['POST'])
def process_app():

	name=str(request.form['appname'])
	#app_id = name	
	return redirect(url_for('show_post', app_id=name))



@app.route('/apps/<app_id>')
def show_post(app_id):
	app_html= ("%s"+'.'+"html")%app_id
	cmd = './live_job_mon.py %s > templates/%s'%(app_id,app_html)
        p = subprocess.Popen([cmd], stdout=subprocess.PIPE,shell=True)
        out, err = p.communicate()
        return render_template(app_html)
#	return "yes"



if __name__ == '__main__':
  app.run( 
        host="0.0.0.0",port="5000")
