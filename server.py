#!/usr/bin/env python3

'''
Simple Jobs Distribution Framework
Copyright 2012-2016 Olivier Friard

Server

requirements:
* tornado
* sqlite3

usage:
python3 distrib_server.py PORT


sqlite DB schema:

CREATE TABLE jobs (
  id integer PRIMARY KEY,
  project text,
  job_id integer,
  project_status text,
  system text ,
  min_client_version integer,
  command text,
  program text,
  script text,
  results_file text,
  job_status text,
  init_time integer,
  end_time integer,
  remote_ip integer,
  data text  );

'''

DBFile = "projects.sqlite"


import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.options
from tornado.options import define, options

import platform
import os
import time
import sqlite3
import json
import sys

PORT = int(sys.argv[1])

define("port", default = PORT, help = "run on the given port", type = int)

connection = sqlite3.connect(DBFile)
cursor = connection.cursor()

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("<h1>Jobs distribution server</h1>")
        self.write("""<a href="projects_list">Available projects</a>""")

class upload(tornado.web.RequestHandler):

    def post(self):

        project = self.get_argument("project")
        job_id = self.get_argument("job_id")
        job_status = self.get_argument("job_status")

        print( "project: {}  job_id: {}".format(project, job_id))

        if "error" not in job_status:

            fileinfo = self.request.files['upload_file'][0]

            filePath = "results/" + fileinfo["filename"]

            # check if file already exists
            if os.path.isfile(filePath):
                filePath += "_" + str(job_id)

            try:
                with open(filePath, "wb") as fh:
                    fh.write(fileinfo['body'])
            except:
                print("error saving {} ".format(filePath))
                return

        cursor.execute("UPDATE jobs SET job_status = '%(job_status)s', end_time = strftime('%%s','now') WHERE job_id = %(job_id)s " % {'job_id': str(job_id), 'job_status': job_status})
        print("db updated")
        return




class get_job(tornado.web.RequestHandler):
    def get(self):

        remote_ip = self.request.remote_ip
        project = self.get_argument("project", "")
        system = self.get_argument("system", "")
        print(system)

        cursor.execute("SELECT job_id FROM jobs WHERE project = '{project}' AND job_status = 't' AND system LIKE '%{system}%' ORDER BY job_id LIMIT 1".format(project=project, system=system))
        rows = list(cursor.fetchall())
        print(rows)
        job_id = 0
        if rows:
           job_id = rows[0][0]

        if job_id:
            cursor.execute("UPDATE jobs SET job_status = 's', init_time = strftime('%%s','now'), remote_ip = '%(remote_ip)s' WHERE job_id = %(job_id)s" % {'job_id': str(job_id), 'remote_ip': remote_ip})
            connection.commit()

            cursor.execute("SELECT  project_status, command, program, script, results_file, data FROM jobs WHERE job_id=%(job_id)s LIMIT 1" % {'job_id': job_id},())
            rows = cursor.fetchall()
            if rows:
                project_status, command, program, script, results_file, data = rows[0]
                out_dict = {'job_id': job_id, 'project': project, 'project_status': project_status, 'command': command, 'program': program, 'data': data,'results_file': results_file }

                self.write(json.dumps(out_dict))
                print( json.dumps(out_dict) )
            else:
                print( 'error getting job for %s' % project  )
                self.write( 'error getting job for %s' % project )
        else:
            connection.commit()

            print( 'no more jobs for %s' % project )
            out_dict = {'msg': 'There are no more jobs or no jobs for your platform', 'project': project}

            self.write( json.dumps(out_dict) )

class project_stats(tornado.web.RequestHandler):

    def get(self):
        """
        show stats for a project
        """

        projectName = self.get_argument("project", "")
        if not projectName:
            return
        print(projectName)

        self.write("<h1>Jobs distribution server</h1>")
        self.write("<h2>{} stats</h2>".format(projectName))

        cursor.execute("SELECT count(*) FROM jobs WHERE project = ? and job_status = 'd'", (projectName,))
        jobsDone = cursor.fetchone()[0]
        cursor.execute("SELECT count(*) FROM jobs WHERE project = ? ", (projectName,))
        jobsTotal = cursor.fetchone()[0]

        self.write("{} jobs done<br>".format(jobsDone))
        self.write("{} % of project done<br>".format(round(jobsDone/jobsTotal*100),1))


        cursor.execute("SELECT job_id,job_status,init_time,remote_ip,end_time,results_file FROM jobs WHERE project = ?", (projectName,))
        rows = cursor.fetchall()
        if not rows:
            self.write("Project {} not found".format(projectName))
            return
            '''
 id integer PRIMARY KEY,
   project text,
     project_status text,
       job_id integer,
         job_status text,
           system text ,
             min_client_version integer,
               command text,
                 program text,
                   script text,
                     results_file text,

                       init_time integer,
                         end_time integer,
                           remote_ip integer,
                             data text
                            '''
        self.write("""<table width="100%" border="1">""")
        self.write("<tr><th>Job id</th><th>Status</th><th>Client IP</th><th>Duration</th><th>Results file</th></tr>")

        STATUS = {"s":"submitted", "t":"to do", "d": "done", "e":"error"}

        for row in rows:
            results_file = ""
            jobStatus = STATUS[row[1]] if row[1] in ["s","t","d"] else row[1]
            if jobStatus in ["submitted", "done"]:
                ts = time.localtime(int(row[2]))
                init_time = "{}-{}-{} {}:{}".format(ts.tm_year, ts.tm_mon, ts.tm_mday, ts.tm_hour, ts.tm_min)
            else:
                init_time = "-"
            if jobStatus in ["done"]:
                try:
                    t = int(row[4]) - int(row[2])
                    unit = "sec"
                    if t >=  60:
                        t = t/ 60
                        unit = "min"
                    if t > 60:
                        t = t / 60
                        unit = "h"
                    if t > 24:
                        t = t/ 24
                        unit = "days"
                    init_time = "{0:.2f} {1}".format(t, unit)
                except:
                    init_time = ""

                results_file = """<a href="results/{0}">{0}</a>""".format(row[5])

            self.write("<tr>"+("<td>{}</td>"*5).format(row[0], jobStatus, row[3], init_time, results_file.replace("###JOB_ID###", str(row[0]) ))+"</tr>")
        self.write("</table>")


class projects_list(tornado.web.RequestHandler):

    def get(self):
        """
        return list of available projects for client platform and version
        """
        cursor.execute("SELECT project FROM jobs GROUP BY project")
        projectsList = [project[0] for project in list(cursor.fetchall())]

        self.write("<h1>Jobs distribution server</h1>")
        self.write("<h2>Available projects</h2>")

        for project in projectsList:
            self.write( """<li><a href="project_status?project={0}">{0}</a></li>""".format(project))


class API_projects_list(tornado.web.RequestHandler):

    def get(self):
        """
        return list of available projects for client platform and version
        """
        clientVersion = self.get_argument("clientVersion", 0)
        system = self.get_argument("system", "")

        cursor.execute("SELECT project FROM jobs WHERE system LIKE ? AND job_status = 't' GROUP BY project", ('%' + system + '%',))
        projectsList = [project[0] for project in list(cursor.fetchall())]

        self.write(json.dumps(projectsList))



class project_status(tornado.web.RequestHandler):

    def get(self):
        projectName = self.get_argument("project", "")

        self.write("<h1>Jobs distribution server</h1>")
        if projectName:

            r = list(cursor.execute("SELECT count(job_id) FROM jobs WHERE project = ? ", ( projectName ,)))
            totJobsNb = r[0][0]
            if not totJobsNb:
                self.write("Project <b>{}</b> not found!".format(projectName))
                return

            r = list(cursor.execute("SELECT count(job_id) FROM jobs WHERE project = ? AND job_status = 'd'", ( projectName ,)))
            totDoneJobsNb = r[0][0]

            r = list(cursor.execute("SELECT count(job_id) FROM jobs WHERE project = ? AND job_status = 's'", ( projectName ,)))
            totSubmittedJobsNb = r[0][0]


            self.write("<h2><b>{}</b> project</h2>".format(projectName))
            self.write("Number of jobs: <b>{}</b><br>".format(totJobsNb))
            self.write("Number of done jobs: <b>{}</b><br>".format(totDoneJobsNb))
            self.write("Number of submitted jobs: <b>{}</b><br>".format(totSubmittedJobsNb))

            self.write('<h3>Activity</h2>')
            # jobs done last hour
            r = list(cursor.execute("""select count(*) from jobs where project = "{}" and job_status = "d" and init_time and end_time and end_time > strftime('%s','now')-3600""".format(projectName)))
            self.write("Jobs done last hour: <b>{}</b><br>".format(r[0][0]))

            # jobs done last day
            r = list(cursor.execute("""select count(*) from jobs where project = "{}" and job_status = "d" and init_time and end_time and end_time > strftime('%s','now')-86400""".format(projectName)))
            self.write("Jobs done last day: <b>{}</b><br>".format(r[0][0]))



            self.write('<h3>Jobs done by IP</h2>')
            # jobs done by IP
            r = list(cursor.execute("select remote_ip, count(*) from jobs where project = '{}' and job_status = 'd' GROUP BY remote_ip".format(projectName)))
            self.write("""<table border="1"><thead><tr><td>IP</td><td>jobs done</td></thead>""")
            for ip, nb in r:
                self.write("<tr><td>{}</td><td>{}</td></tr>".format(ip,nb))
            self.write("</table>")

            self.write("""<h3><a href="stats?project={}">Results</a></h2>""".format(projectName))

        else:

            self.write("<h2>List of projects</h2>")
            rows = list(cursor.execute("SELECT project, count(*) FROM jobs GROUP BY project"))
            for row in rows:
                self.write("""<b><a href="stats?project={0}">{0}</a></b><br>""".format(row[0]))
                self.write("Total number of jobs: {}".format(row[1]))
                self.write("<hr>")


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            ("/data/(.*)", tornado.web.StaticFileHandler, {'path': 'data'}),
            ("/results/(.*)", tornado.web.StaticFileHandler, {'path': 'results'}),
            ("/", MainHandler),
            ("/get_job", get_job),
            ("/upload", upload),
            ("/projects_list", projects_list),
            ("/projectsList", API_projects_list),
            ("/stats", project_stats),
            ("/project_status", project_status)

        ]
        settings = {
            "debug": True,
        }
        tornado.web.Application.__init__(self, handlers, **settings)


def main():

    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()


cursor.close(True)
