#!/usr/bin/env python3

"""
Simple Jobs Distribution Framework
Copyright 2012-2016 Olivier Friard

Client

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  Use this program on a trusted network with trusted host and guest

Usage:

Usage: ./distrib_client.py [options]

Options:
  -h, --help            show this help message and exit
  -s SERVER, --server=SERVER
                        Server address:port
  -p PROJECT, --project=PROJECT
                        Project name (all: choose between all projects,
                        random: randomly choose a project)
  -l, --list            List of projects
  -n NJ, --jobs-number=NJ
                        Number of jobs to compute
  -u, --unique          Launch if not already running
  -v, --version         Print client version


"""

__version__ = 10

import urllib.request
import os
import sys
import zipfile
import time
from optparse import OptionParser
import platform
import shutil
import json
import subprocess
import http.client, mimetypes
import tempfile

print("\nSimple Jobs Distribution Framework\nversion: {}, platform: {}".format(__version__, platform.system()))

# host must include the protocol (http://, https:// ...)
#host = 'http://penelope.unito.it:8800'
SERVER = "http://130.192.143.253:1111"

system = platform.system()

zipfiles_mem = []
jobs_count = 0

def wait():
    print("\nThe Simple Distributed Computing Platform client will exit in 15 seconds...\n")
    time.sleep(15)

class unzip:
    def __init__(self, verbose = False, percent = 10):
        self.verbose = verbose
        self.percent = percent

    def extract(self, file, dir):

        if not dir.endswith(':') and not os.path.exists(dir):
            os.mkdir(dir)

        zf = zipfile.ZipFile(file)

        # create directory structure to house files
        self._createstructure(file, dir)

        num_files = len(zf.namelist())
        percent = self.percent
        divisions = 100 / percent
        perc = int(num_files / divisions)

        # extract files to directory structure
        for i, name in enumerate(zf.namelist()):

            if self.verbose == True:
                print( "Extracting %s" % name)
            elif perc > 0 and (i % perc) == 0 and i > 0:
                complete = int (i / perc) * percent
                print( "%s%% complete" % complete)

            if not name.endswith('/'):
                outfile = open(os.path.join(dir, name), 'wb')

                print( 'File:',os.path.join(dir, name))

                outfile.write(zf.read(name))
                outfile.flush()
                outfile.close()
                # set permission to u+rwx (448)
                os.chmod(os.path.join(dir, name) , 448)

        return True

    def _createstructure(self, file, dir):
        self._makedirs(self._listdirs(file), dir)


    def _makedirs(self, directories, basedir):
        """ Create any directories that don't currently exist """
        for dir in directories:
            curdir = os.path.join(basedir, dir)
            if not os.path.exists(curdir):
                os.mkdir(curdir)

    def _listdirs(self, file):
        """ Grabs all the directories in the zip structure
        This is necessary to create the structure before trying
        to extract the file to it. """
        zf = zipfile.ZipFile(file)
        dirs = []
        for name in zf.namelist():
            if name.endswith('/'):
                dirs.append(name)
        dirs.sort()
        return dirs


def post_multipart(host, selector, fields, files):
    """
    Post fields and files to an http host as multipart/form-data.
    """
    content_type, body = encode_multipart_formdata(fields, files)

    # Choose between http and https connections
    if(host.find('https') == 0):
        h = http.client.HTTPSConnection(host)
    else:
        h = http.client.HTTPConnection(host)

    h.putrequest('POST', selector)
    h.putheader('content-type', content_type)
    h.putheader('content-length', str(len(body)))
    h.endheaders()
    h.send(body)
    response = h.getresponse()
    return response.read()

def encode_multipart_formdata(fields, files):
    """
    fields is a sequence of (name, value) elements for regular form fields.
    files is a sequence of (name, filename, value) elements for
    """
    BOUNDARY_STR = '----------ThIs_Is_tHe_bouNdaRY_$'
    CRLF = bytes("\r\n","ASCII")
    L = []
    for (key, value) in fields:
        L.append(bytes("--" + BOUNDARY_STR,"ASCII"))
        L.append(bytes('Content-Disposition: form-data; name="%s"' % key,"ASCII"))
        L.append(b'')
        L.append(bytes(value,"ASCII"))
    if files:
        for (key, filename, value) in files:
            L.append(bytes('--' + BOUNDARY_STR,"ASCII"))
            L.append(bytes('Content-Disposition: form-data; name="%s"; filename="%s"' % (key, filename),"ASCII"))
            L.append(bytes('Content-Type: %s' % get_content_type(filename),"ASCII"))
            L.append(b'')
            L.append(value)
    L.append(bytes('--' + BOUNDARY_STR + '--',"ASCII"))
    L.append(b'')
    body = CRLF.join(L)
    content_type = 'multipart/form-data; boundary=' + BOUNDARY_STR
    return content_type, body

def get_content_type(filename):
    return mimetypes.guess_type(filename)[0] or 'application/octet-stream'



def get_project_parameters():

    usage = "usage: ./%prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option("-s", "--server", dest = "server", help = "Server address:port")
    parser.add_option("-p", "--project", dest = "project", help = "Project name (all: choose between all projects, random: randomly choose a project)")
    parser.add_option("-l", "--list", action = "store_true", dest = "list", help = "List of projects")
    parser.add_option("-n", "--jobs-number", dest = "nj", help = "Number of jobs to compute")

    parser.add_option("-u", "--unique",  action = "store_true", dest = "unique", help = "Launch if not already running")
    parser.add_option("-v", "--version", action = "store_true", dest = "ver", help = "Print client version")

    parser.add_option("-a", "--after", dest = "t1", help = "No job computation after (i.e 600 for 6:00 AM)")
    parser.add_option("-b", "--before", dest = "t2", help = "No job computation before (i.e 2000 for 8:00 PM)")

    (options,args) = parser.parse_args()

    if options.ver:
        sys.exit(0)

    if options.unique:
        unique = True
    else:
        unique = False

    print('unique',unique)

    server = options.server if options.server else SERVER

    print("\nQuerying available projects for your platform...")
    try:
        response = urllib.request.urlopen("{server}/projectsList?clientVersion={clientVersion}&system={system}".format(server=server, clientVersion=__version__, system=platform.system()))
        response = response.read().decode('utf-8').strip()
    except:
         return True, "Error! Check the server URL"

    remoteProjectsList = json.loads(response)

    print("Available projects for your OS: {}".format(",".join(remoteProjectsList)))
    if options.list:
        return True, ""

    projectName = options.project

    if projectName:

        print("project name:", projectName)

        if projectName in remoteProjectsList:

            if options.nj is not None:
                nj = int(options.nj)
            else:
                if options.t1 and options.t2:
                    nj = 1e6
                else:
                    while True:
                        try:
                            nj = int(input("\nNumber of jobs: "))
                            break
                        except ValueError:
                            print("Not a valid number. Try again...")

            return False, (projectName, nj, server, unique)

        else:
            return True, 'project not found'

    else:
        return True, 'No project'



def execute(projectName, server):
    """
    download job to execute
    return return_code, msg

    return_code True if error
    """
    jobDir = ""
    job_id = -1

    print("Getting job from server...")

    try:

        response = urllib.request.urlopen("{server}/get_job?project={projectName}&system={system}".format(server=server, projectName=projectName, system=platform.system()))

        #print(response.read())

        #print ("Response:", response)

        # Get the URL. This gets the real URL.
        #print( "The URL is: ", response.geturl())

        # Getting the code
        #print( "This gets the code: ", response.code)

        if response.code != 200:
            return True, "Download error {}".format(response.code)

        #print( "The Headers are: ", response.info())

        # Get all data
        job = json.loads(response.read().decode('utf-8').strip())

        if "msg" in job:
            print(job["msg"])
            raise Exception(True, job["msg"])

        job_id = job["job_id"]
        print("Job id: {}".format(job_id))

        command = json.loads(job["command"])
        results_file = job["results_file"].replace("###JOB_ID###", str(job_id) )
        data = job["data"].replace("###JOB_ID###", str(job_id))
        program = json.loads(job["program"])

        if platform.system() in command:
            system_command = command[platform.system()].replace("###JOB_ID###", str(job_id))
        else:
            raise Exception(True, "No jobs for your OS ({})".format(platform.system()))

        # check if project dir exists
        jobDir = "{}_{}".format(projectName, job_id)

        if not os.path.isdir(jobDir):
            os.makedirs( jobDir )
            os.chdir(jobDir)

            if program:

                if platform.system() in program:
                    system_program = program[ platform.system() ]

                    if system_program:
                        for file_ in system_program:

                            response = urllib.request.urlopen("{server}/data/{file}".format(server=server, file=file_))
                            program_file_content = response.read()

                            with open( file_, "wb") as fh:
                                fh.write( program_file_content )

                            # check if fileis compressed
                            if ".zip" in file_:
                                unzipper = unzip()
                                unzipper.extract(file_, ".")

        # project dir exists cd
        else:
            raise Exception(True, "directory {} already exists".format(jobDir))

        if data:
            try:
                response = urllib.request.urlopen("{server}/data/{data}".format(server=server, data=data))
                data_file_content = response.read()
            except:
                raise Exception(True, "File not found: {}".format(data))

            if b"###JOB_ID###" in data_file_content:
                data_file_content = data_file_content.replace(b"###JOB_ID###", str(job_id) )

            with open( data, "wb") as outFile:
                outFile.write( data_file_content)

            # check if data is compressed file
            if ".zip" in data:
                unzipper = unzip()
                unzipper.extract(data, ".")

        p = subprocess.Popen(system_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )
        out, error = p.communicate()
        #print(out)
        #print(error)
        #out = out.decode('utf-8')
        #error = error.decode('utf-8')
        if error:
            # update DB with error in job_status field
            raise Exception(True, error.decode('utf-8'))
            #return True, "Error: {}".format(retcode)


        if results_file and os.path.isfile(results_file):
            try:
                fields = [('job_id', str(job_id)), ('project', projectName), ("job_status", "d")]
                files = [('upload_file', results_file, open(results_file, "rb").read())]
                post_multipart(server.replace('http://', ''), '/upload', fields, files)
            except:
                raise Exception(True, "Error uploading file")
        else:
            if not os.path.isfile(results_file):
                raise Exception(True, "Results file not found")

        # remove job directory
        os.chdir(sys.path[0])
        shutil.rmtree(jobDir)
        return False, "Job completed"

    except Exception as error:

        # send error to server if job
        print(error.args[1])

        print('job_id', job_id)
        if job_id != -1:
            fields = [('job_id', str(job_id)), ('project', projectName), ('job_status', 'error: {}'.format(error.args[1]))]
            post_multipart(server.replace('http://', ''), '/upload', fields, None)

        # delete job directory (if any)
        os.chdir(sys.path[0])
        if os.path.isdir(jobDir):
            print("deleting job directory")
            shutil.rmtree(jobDir)
            print("Job directory deleted")

        print("final except")
        return error.args


# read parameters
result, parameters = get_project_parameters()
if result:
    print(parameters)
    sys.exit()

projectName, jobsMaxNumber, server, unique = parameters

if unique and os.path.isfile(tempfile.gettempdir() + os.sep + "distrib_client_lock"):
    print("distrib client is already running. If not delete the {} file".format(tempfile.gettempdir() + os.sep + "distrib_client_lock"))
    sys.exit()

# write lock file
try:
    with open(tempfile.gettempdir() + os.sep + "distrib_client_lock", "w") as f_out:
        f_out.write(str(os.getpid()))
except:
    print("Writing lock file failed")



jobNumber = 0

while True:

    try:
        result, msg = execute(projectName, server)
    except:
        result = True
        msg = "Undefined error"

    # return to script directory
    os.chdir(os.path.dirname(os.path.realpath(__file__)))

    if result:
        print(msg)
        break
    else:
        jobNumber += 1

    if jobNumber >= jobsMaxNumber:
        break

print( "{} job(s) executed".format(jobNumber))


try:
    os.unlink(tempfile.gettempdir() + os.sep + "distrib_client_lock")
except:
    print("Deleting lock file failed")

