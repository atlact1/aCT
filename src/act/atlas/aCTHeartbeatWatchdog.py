# Tool for updating heartbeats when the main process has failed.

from act.common.aCTLogger import aCTLogger
from act.arc.aCTDBArc import aCTDBArc
from aCTDBPanda import aCTDBPanda
from aCTPanda import aCTPanda
import sys
import time

if len(sys.argv) != 2:
    print "Usage: python aCTHeartbeatWatchdog.py timelimit"
    sys.exit(1)

timelimit = int(sys.argv[1])

# logger
logger = aCTLogger('aCTHeartbeatWatchdog')
log = logger()
# database
dbarc = aCTDBArc(log)
dbpanda = aCTDBPanda(log)

# Query for running jobs with theartbeat longer than timelimit seconds ago
select = "sendhb=1 and " \
         "pandastatus in ('sent', 'starting', 'running', 'transferring') and " \
         "theartbeat != 0 and " + dbpanda.timeStampLessThan("theartbeat", timelimit)
columns = ['pandaid', 'pandastatus', 'proxyid', 'sitename']
jobs = dbpanda.getJobs(select, columns)

if jobs:
    print 'Found %d jobs with outdated heartbeat (older than %d seconds):\n' % (len(jobs), timelimit)
    
    # Panda server for each proxy
    pandas = {}
    for job in jobs:
        print job['pandaid'], job['sitename']
        proxyid = job['proxyid']
        if proxyid not in pandas:
            panda = aCTPanda(log, dbarc.getProxyPath(proxyid))
            pandas[proxyid] = panda
                   
        pandas[proxyid].updateStatus(job['pandaid'], job['pandastatus'])
        # update heartbeat time in the DB
        dbpanda.updateJob(job['pandaid'], {'theartbeat': dbpanda.getTimeStamp(time.time()+1)})

