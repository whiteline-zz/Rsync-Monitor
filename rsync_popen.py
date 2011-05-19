import re
import sys
import MySQLdb
from MySQLdb import connect
import time
import fileinput
import subprocess

def match_iter(value,regexp,buf):
	result = None

	for result in re.finditer(regexp,buf):
		pass

	if(result == None or result.group(2) == ''):
		return value
	else:	
		return "'"+result.group(2)+"'"

retval = 'NULL'
byteval = 'NULL'
command = 'NULL'

timeexec = time.time()
sys.argv.pop(0)
rsync = subprocess.Popen(sys.argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

while(rsync.returncode == None):
	(r_stdout,r_stderr) = rsync.communicate()

	byteval = match_iter(byteval,'(sent )(\d*)( bytes)',r_stdout)
	command = match_iter(command,'(opening connection using\: )(.*)',r_stdout)

timeexec = time.time() - timeexec

retval = rsync.returncode

db = connect(host='spider.irf.se',user='rsync',passwd='abrakadabra',db='rsyncreport')
c = db.cursor()
c.execute("""INSERT INTO `rsyncreport`.`rsyncreport`(
		`id`,`v_datetime`,`v_name`,`v_bytes`,`v_return`,`v_timeexec`,`v_command`)
		VALUES(
		NULL,NOW(),USER(),"""+str(byteval)+""","""+str(retval)+""",
		"""+str(timeexec)+""","""+str(command)+""");""")

if(retval != 0):
	print "Error in rsync job"

c.close()
