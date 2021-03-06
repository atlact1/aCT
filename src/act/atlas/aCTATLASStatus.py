# Handler for filling pandajobs information from arcjobs information. Also
# deals with post-processing of jobs and error handling.

import time
import datetime
import re
import os
import shutil
import arc

from act.common import aCTSignal
from act.common import aCTUtils

from aCTATLASProcess import aCTATLASProcess
from aCTPandaJob import aCTPandaJob
from aCTAGISParser import aCTAGISParser

class aCTATLASStatus(aCTATLASProcess):
    
    def __init__(self):
        aCTATLASProcess.__init__(self)
        self.agisparser = aCTAGISParser(self.log)
                 
    def setSites(self):
        self.sites = self.agisparser.getSites()

    def checkJobstoKill(self):
        """
        Get starting jobs for offline sites and kill them.
        Check for jobs with pandastatus tobekilled and cancel them in ARC:
        - pandastatus NULL: job was killed by panda so nothing to report
        - pandastatus something else: job was manually killed, so create pickle
          and report failed back to panda
        """

        sites = [s for s,a in self.sites.iteritems() if a['status'] == 'offline']
        
        if sites:
            
            sites = "'"+"','".join(sites)+"'"
            jobs = self.dbpanda.getJobs("(actpandastatus='starting' or actpandastatus='sent') and sitename in (" + sites + ")",
                                        ['pandaid', 'arcjobid', 'siteName', 'id'])

            for job in jobs:
                self.log.info("Cancelling starting job for %d for offline site %s", (job['pandaid'], job['siteName']))
                select = 'id=%s' % job['id']
                self.dbpanda.updateJobsLazy(select, {'actpandastatus': 'failed', 'pandastatus': 'failed'})
                if job['arcjobid']:
                    self.dbarc.updateArcJob(job['arcjobid'], {'arcstate': 'tocancel'})
            
            self.dbpanda.Commit()
        
        # Get jobs killed by panda
        jobs = self.dbpanda.getJobs("actpandastatus='tobekilled'", ['pandaid', 'arcjobid', 'pandastatus', 'id'])
        if not jobs:
            return
        
        for job in jobs:
            self.log.info("Cancelling arc job for %d", job['pandaid'])
            select = 'id=%s' % job['id']
            
            # Check if arcjobid is set before cancelling the job
            if not job['arcjobid']:
                self.dbpanda.updateJobsLazy(select, {'actpandastatus': 'cancelled'})
                continue
            
            # Put timings in the DB
            arcselect = "arcjobid='%s' and arcjobs.id=pandajobs.arcjobid" % job['arcjobid']
            arcjobs = self.dbarc.getArcJobsInfo(arcselect, tables='arcjobs,pandajobs')
            desc = {}
            if arcjobs:
                desc['endTime'] = arcjobs[0]['EndTime'] if arcjobs[0]['EndTime'] else datetime.datetime.utcnow()
                desc['startTime'] = self.getStartTime(desc['endTime'], arcjobs[0]['UsedTotalWallTime']) if arcjobs[0]['UsedTotalWallTime'] else datetime.datetime.utcnow()
            
            # Check if job was manually killed
            if job['pandastatus'] is not None:
                self.log.info('%s: Manually killed, will report failure to panda' % job['pandaid'])
                self.processFailed(arcjobs)
                # Skip validator since there is no metadata.xml
                desc['actpandastatus'] = 'failed'
                desc['pandastatus'] = 'failed'
            else:
                desc['actpandastatus'] = 'cancelled'
            self.dbpanda.updateJobsLazy(select, desc)

            # Finally cancel the arc job                
            self.dbarc.updateArcJob(job['arcjobid'], {'arcstate': 'tocancel'})
        
        self.dbpanda.Commit()

    def getStartTime(self, endtime, walltime):
        """
        Get starttime from endtime-walltime where endtime is datetime.datetime and walltime is in seconds
        If endtime is none then use current time
        """
        if not endtime:
            return datetime.datetime.utcnow() - datetime.timedelta(0, walltime)
        return endtime-datetime.timedelta(0, walltime)
           
           
    def updateStartingJobs(self):
        """
        Check for sent jobs that have been submitted to ARC and update
        actpandastatus to starting, and also for jobs that were requeued
        from running.
        """

        select = "arcjobs.id=pandajobs.arcjobid and (arcjobs.arcstate='submitted' or arcjobs.arcstate='holding')"
        select += " and (pandajobs.actpandastatus='sent' or pandajobs.actpandastatus='running')"
        select += " limit 100000"
        columns = ["arcjobs.id", "arcjobs.JobID"]
        jobstoupdate=self.dbarc.getArcJobsInfo(select, columns=columns, tables="arcjobs,pandajobs")

        if len(jobstoupdate) == 0:
            return
        else:
            self.log.debug("Found %d submitted jobs", len(jobstoupdate))

        for aj in jobstoupdate:
            select = "arcjobid='"+str(aj["id"])+"'"
            desc = {}
            desc["pandastatus"] = "starting"
            desc["actpandastatus"] = "starting"
            desc["computingElement"] = aj['JobID']
            self.dbpanda.updateJobsLazy(select, desc)
        self.dbpanda.Commit()

        
    def updateRunningJobs(self):
        """
        Check for new running jobs and update pandajobs with
        - pandastatus
        - node
        - computingElement
        - startTime
        """

        # do an inner join to pick up all jobs that should be set to running
        # todo: pandajobs.starttime will not be updated if a job is resubmitted 
        # internally by the ARC part.
        select = "arcjobs.id=pandajobs.arcjobid and arcjobs.arcstate='running' and pandajobs.actpandastatus in ('starting', 'sent')"
        select += " limit 100000"
        columns = ["arcjobs.id", "arcjobs.UsedTotalWalltime", "arcjobs.ExecutionNode",
                   "arcjobs.JobID", "arcjobs.RequestedSlots", "pandajobs.pandaid", "pandajobs.siteName"]
        jobstoupdate=self.dbarc.getArcJobsInfo(select, columns=columns, tables="arcjobs,pandajobs")

        if len(jobstoupdate) == 0:
            return
        else:
            self.log.debug("Found %d running jobs", len(jobstoupdate))

        for aj in jobstoupdate:
            select = "arcjobid='"+str(aj["id"])+"'"
            desc = {}
            desc["pandastatus"] = "running"
            desc["actpandastatus"] = "running"
            if len(aj["ExecutionNode"]) > 255:
                desc["node"] = aj["ExecutionNode"][:254]
                self.log.warning("%s: Truncating wn hostname from %s to %s" % (aj['pandaid'], aj['ExecutionNode'], desc['node']))
            else:
                desc["node"] = aj["ExecutionNode"]
            desc["computingElement"] = aj['JobID']
            desc["startTime"] = self.getStartTime(datetime.datetime.utcnow(), aj['UsedTotalWalltime'])
            desc["corecount"] = aj['RequestedSlots']
            # When true pilot job has started running, turn of aCT heartbeats
            if self.sites[aj['siteName']]['truepilot']:
                self.log.info("%s: Job is running so stop sending heartbeats", aj['pandaid'])
                desc['sendhb'] = 0
            self.dbpanda.updateJobsLazy(select, desc)
        self.dbpanda.Commit()

        
    def updateFinishedJobs(self):
        """
        Check for new finished jobs, update pandajobs with
        - pandastatus
        - startTime
        - endTime
        """

        # don't get jobs already having actpandastatus states treated by
        # validator to avoid race conditions
        select = "arcjobs.id=pandajobs.arcjobid and arcjobs.arcstate='done'"
        select += " and pandajobs.actpandastatus != 'tovalidate'"
        select += " and pandajobs.actpandastatus != 'toresubmit'"
        select += " and pandajobs.actpandastatus != 'toclean'"
        select += " and pandajobs.actpandastatus != 'finished'"
        select += " limit 100000"
        columns = ["arcjobs.id", "arcjobs.UsedTotalWallTime", "arcjobs.EndTime", "arcjobs.appjobid", "pandajobs.sendhb", "pandajobs.siteName"]
        jobstoupdate=self.dbarc.getArcJobsInfo(select, tables="arcjobs,pandajobs", columns=columns)
        
        if len(jobstoupdate) == 0:
            return
        else:
            self.log.debug("Found %d finished jobs", len(jobstoupdate))


        for aj in jobstoupdate:
            select = "arcjobid='"+str(aj["id"])+"'"
            desc = {}
            desc["pandastatus"] = "transferring"
            desc["actpandastatus"] = "tovalidate"
            desc["startTime"] = self.getStartTime(aj['EndTime'], aj['UsedTotalWallTime'])
            desc["endTime"] = aj["EndTime"]
            # True pilot job may have gone straight to finished, turn off aCT heartbeats if necessary
            if self.sites[aj['siteName']]['truepilot'] and aj["sendhb"] == 1:
                self.log.info("%s: Job finished so stop sending heartbeats", aj['appjobid'])
                desc['sendhb'] = 0
            self.dbpanda.updateJobsLazy(select, desc)
        self.dbpanda.Commit()


    def checkFailed(self, arcjobs):
        failedjobs = []
        resubmitting=False
        for aj in arcjobs:
            if self.sites[aj['siteName']]['truepilot']:
                self.log.info('%s: No resubmission for true pilot job', aj['appjobid'])
                failedjobs += [aj]
                continue
            resubmit=False
            # todo: errors part of aCTConfigARC should probably be moved to aCTConfigATLAS.
            for error in self.arcconf.getList(['errors','toresubmit','arcerrors','item']):
                if aj['Error'].find(error) != -1:
                    resubmit=True
            if resubmit:
                self.log.info("%s: Resubmitting %d %s %s" % (aj['appjobid'],aj['arcjobid'],aj['JobID'],aj['Error']))
                select = "arcjobid='"+str(aj["arcjobid"])+"'"
                jd={}
                # Validator processes this state before setting back to starting
                jd['pandastatus'] = 'starting'
                jd['actpandastatus'] = 'toresubmit'
                self.dbpanda.updateJobsLazy(select,jd)
                resubmitting=True
            else:
                failedjobs += [aj]
        if resubmitting:
            self.dbpanda.Commit()
            self.dbarc.Commit()
        return failedjobs

    def createPilotLog(self, outd, pandaid):
        '''
        Create the pilot log messages to appear on panda logger. Takes the gmlog
        'failed' file and errors from the pilot log if available. Creates a
        local copy under tmp/failedlogs.
        '''
        nlines=20
        log=""
        try:
            f=open(outd+"/gmlog/failed","r")
            log+="---------------------------------------------------------------\n"
            log+="GMLOG: failed\n"
            log+="---------------------------------------------------------------\n"
            log+=''.join(f.readlines())
            f.close()
        except:
            pass
        

        import glob
        lf=glob.glob(outd+"/log*")
        try:
            f=open(lf[0],"r")
            lines=f.readlines()
            log+="---------------------------------------------------------------\n"
            log+="LOGFILE: tail\n"
            log+="---------------------------------------------------------------\n"
            lns=[]
            for l in lines:
                if re.match('.*error',l,re.IGNORECASE):
                    lns.append(l)
                if re.match('.*warning',l,re.IGNORECASE):
                    lns.append(l)
                if re.match('.*failed',l,re.IGNORECASE):
                    lns.append(l)
            log+=''.join(lns[:nlines])
            # copy logfiles to failedlogs dir
            failedlogsd = self.arcconf.get(["tmp","dir"])+"/failedlogs"
            try:
                f=open(os.path.join(failedlogsd, str(pandaid)+".log"),"w")
                f.write(log)
                f.close()
            except:
                pass
        except:
            pass
        return log


    def processFailed(self, arcjobs):
        """
        process jobs failed for other reasons than athena (log_extracts was not created by pilot)
        """
        if not arcjobs:
            return

        self.log.info("processing %d failed jobs" % len(arcjobs))
        for aj in arcjobs:
            jobid=aj['JobID']
            if not jobid:
                # Job was not even submitted, there is no more information
                self.log.warning("%s: Job has not been submitted yet so no information to report", aj['appjobid'])
                continue
            
            cluster = arc.URL(str(jobid)).Host()
            sessionid=jobid[jobid.rfind('/')+1:]
            date = time.strftime('%Y%m%d')
            outd = os.path.join(self.conf.get(['joblog','dir']), date, cluster, sessionid)
            # Make sure the path up to outd exists
            try:
                os.makedirs(os.path.dirname(outd), 0755)
            except:
                pass
            try:
                shutil.rmtree(outd)
            except:
                pass
            # copy from tmp to outd. tmp dir will be cleaned in validator
            localdir = os.path.join(self.arcconf.get(['tmp','dir']), sessionid)
            try:
                shutil.copytree(localdir, outd)
            except (OSError, shutil.Error) as e:
                self.log.warning("%s: Failed to copy job output for %s: %s" % (aj['appjobid'], jobid, str(e)))
                # Sometimes fetcher fails to get output, so just make empty dir
                try:
                    os.makedirs(outd, 0755)
                except OSError, e:
                    self.log.warning("%s: Failed to create %s: %s. Job logs will be missing" % (aj['appjobid'], outd, str(e)))
                
            # set right permissions
            aCTUtils.setFilePermissionsRecursive(outd)

            # set update, pickle from pilot is not available
            # some values might not be properly set
            # TODO synchronize error codes with the rest of production
            pupdate = aCTPandaJob()
            pupdate.jobId = aj['appjobid']
            pupdate.state = 'failed'
            pupdate.siteName = aj['siteName']
            pupdate.computingElement = cluster
            pupdate.schedulerID = self.conf.get(['panda','schedulerid'])
            pupdate.pilotID = self.conf.get(["joblog","urlprefix"])+"/"+date+"/"+cluster+'/'+sessionid+"|Unknown|Unknown|Unknown|Unknown"
            if len(aj["ExecutionNode"]) > 255:
                pupdate.node = aj["ExecutionNode"][:254]
                self.log.warning("%s: Truncating wn hostname from %s to %s" % (aj['pandaid'], aj['ExecutionNode'], pupdate.node))
            else:
                pupdate.node = aj["ExecutionNode"]
            pupdate.node = aj['ExecutionNode']
            pupdate.pilotLog = self.createPilotLog(outd, aj['pandaid'])
            pupdate.cpuConsumptionTime = aj['UsedTotalCPUTime']
            pupdate.cpuConsumptionUnit = 'seconds'
            pupdate.cpuConversionFactor = 1
            pupdate.pilotTiming = "0|0|%s|0" % aj['UsedTotalWallTime']
            pupdate.exeErrorCode = aj['ExitCode']
            pupdate.exeErrorDiag = aj['Error']
            pupdate.pilotErrorCode = 1008
            codes = []
            codes.append("Job timeout")
            codes.append("qmaster enforced h_rt limit")
            codes.append("job killed: wall")
            codes.append("Job exceeded time limit")
            if [errcode for errcode in codes if re.search(errcode, aj['Error'])]:
                pupdate.pilotErrorCode = 1213
            codes=[]
            codes.append("Job probably exceeded memory limit")
            codes.append("job killed: vmem")
            codes.append("pvmem exceeded")
            if [errcode for errcode in codes if re.search(errcode, aj['Error'])]:
                pupdate.pilotErrorCode = 1212
            pupdate.pilotErrorDiag = aj['Error']
            # set start/endtime
            pupdate.startTime = self.getStartTime(aj['EndTime'], aj['UsedTotalWallTime'])
            pupdate.endTime = aj['EndTime']
            # save the pickle file to be used by aCTAutopilot panda update
            try:
                picklefile = os.path.join(self.arcconf.get(['tmp','dir']), "pickle", str(aj['pandaid'])+".pickle")
                pupdate.writeToFile(picklefile)
            except Exception as e:
                self.log.warning("%s: Failed to write file %s: %s" % (aj['appjobid'], picklefile, str(e)))

    
    def updateFailedJobs(self):
        """
        Query jobs in arcstate failed, set to tofetch
        Query jobs in arcstate donefailed, cancelled and lost and not finished in panda.
        If they should be resubmitted, set arcjobid to null in pandajobs and
        cleanupLeftovers() will take care of cleaning up the old jobs.
        If not do post-processing and fill status in pandajobs
        """
        # Get outputs to download for failed jobs
        select = "arcstate='failed'"
        columns = ['id']
        arcjobs = self.dbarc.getArcJobsInfo(select, columns)
        if arcjobs:
            for aj in arcjobs:
                select = "id='"+str(aj["id"])+"'"
                desc = {"arcstate":"tofetch", "tarcstate": self.dbarc.getTimeStamp()}
                self.dbarc.updateArcJobsLazy(desc, select)
            self.dbarc.Commit()
        
        # Look for failed final states in ARC which are still starting or running in panda
        select = "(arcstate='donefailed' or arcstate='cancelled' or arcstate='lost')"
        select += " and actpandastatus in ('starting', 'running')"
        select += " and pandajobs.arcjobid = arcjobs.id limit 100000"
        columns = ['arcstate', 'arcjobid', 'appjobid', 'JobID', 'Error', 'arcjobs.EndTime',
                   'siteName', 'ExecutionNode', 'pandaid', 'UsedTotalCPUTime',
                   'UsedTotalWallTime', 'ExitCode', 'sendhb']

        jobstoupdate=self.dbarc.getArcJobsInfo(select, columns=columns, tables='arcjobs,pandajobs')

        if len(jobstoupdate) == 0:
            return
        
        failedjobs = [job for job in jobstoupdate if job['arcstate']=='donefailed']
        if len(failedjobs) != 0:
            self.log.debug("Found %d failed jobs", len(failedjobs))
        lostjobs = [job for job in jobstoupdate if job['arcstate']=='lost']
        if len(lostjobs) != 0:
            self.log.debug("Found %d lost jobs", len(lostjobs))
        cancelledjobs = [job for job in jobstoupdate if job['arcstate']=='cancelled']
        if len(cancelledjobs) != 0:
            self.log.debug("Found %d cancelled jobs", len(cancelledjobs))
                
        failedjobs=self.checkFailed(failedjobs)
        # process all failed jobs that couldn't be resubmitted
        self.processFailed(failedjobs)

        for aj in failedjobs:
            select = "arcjobid='"+str(aj["arcjobid"])+"'"
            desc = {}
            desc["pandastatus"] = "transferring"
            desc["actpandastatus"] = "toclean" # to clean up any output
            desc["endTime"] = aj["EndTime"]
            desc["startTime"] = self.getStartTime(aj['EndTime'], aj['UsedTotalWallTime'])
            # True pilot job may have gone straight to failed, turn off aCT heartbeats if necessary
            if self.sites[aj['siteName']]['truepilot'] and aj["sendhb"] == 1:
                self.log.info("%s: Job finished so stop sending heartbeats", aj['appjobid'])
                desc['sendhb'] = 0
            self.dbpanda.updateJobsLazy(select, desc)

        for aj in lostjobs:
            # There is no cleaning to do for lost jobs so just resubmit them
            select = "arcjobid='"+str(aj["arcjobid"])+"'"
            desc={}

            # For truepilot, just set to clean and transferring to clean up arc job
            if self.sites[aj['siteName']]['truepilot']:
                self.log.info("%s: Job is lost, cleaning up arc job", aj['appjobid'])
                desc['sendhb'] = 0
                desc['pandastatus'] = 'transferring'
                desc['actpandastatus'] = 'toclean'
            else:
                self.log.info("%s: Resubmitting lost job %d %s %s" % (aj['appjobid'], aj['arcjobid'],aj['JobID'],aj['Error']))
                desc['pandastatus'] = 'starting'
                desc['actpandastatus'] = 'starting'
                desc['arcjobid'] = None
            self.dbpanda.updateJobsLazy(select,desc)

        for aj in cancelledjobs:
            # Jobs were unexpectedly killed in arc, resubmit and clean
            select = "arcjobid='"+str(aj["arcjobid"])+"'"
            desc = {}
            # For truepilot, just set to clean and transferring to clean up arc job
            if self.sites[aj['siteName']]['truepilot']:
                self.log.info("%s: Job was cancelled, cleaning up arc job", aj['appjobid'])
                desc['sendhb'] = 0
                desc['pandastatus'] = 'transferring'
                desc['actpandastatus'] = 'toclean'
            else:
                self.log.info("%s: Resubmitting cancelled job %d %s" % (aj['appjobid'], aj['arcjobid'],aj['JobID']))
                desc["pandastatus"] = "starting"
                desc["actpandastatus"] = "starting"
                desc["arcjobid"] = None
            self.dbpanda.updateJobsLazy(select, desc)

        if failedjobs or lostjobs or cancelledjobs:
            self.dbpanda.Commit()


    def cleanupLeftovers(self):
        """
        Clean jobs left behind in arcjobs table:
         - arcstate=tocancel or cancelling when cluster is empty
         - arcstate=done or cancelled or lost or donefailed when id not in pandajobs
         - arcstate=cancelled and actpandastatus=cancelled/donecancelled/failed/donefailed
        """
        select = "(arcstate='tocancel' or arcstate='cancelling') and (cluster='' or cluster is NULL)"
        jobs = self.dbarc.getArcJobsInfo(select, ['id', 'appjobid'])
        for job in jobs:
            self.log.info("%s: Deleting from arcjobs unsubmitted job %d", job['appjobid'], job['id'])
            self.dbarc.deleteArcJob(job['id'])

        select = "(arcstate='done' or arcstate='lost' or arcstate='cancelled' or arcstate='donefailed') \
                  and arcjobs.id not in (select arcjobid from pandajobs)"
        jobs = self.dbarc.getArcJobsInfo(select, ['id', 'appjobid', 'arcstate', 'JobID'])
        cleandesc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
        for job in jobs:
            # done jobs should not be there, log a warning
            if job['arcstate'] == 'done':
                self.log.warning("%s: Removing orphaned done job %d", job['appjobid'], job['id'])
            else:
                self.log.info("%s: Cleaning left behind %s job %d", job['appjobid'], job['arcstate'], job['id'])
            self.dbarc.updateArcJobLazy(job['id'], cleandesc)
            if job['JobID'] and job['JobID'].rfind('/') != -1:
                sessionid = job['JobID'][job['JobID'].rfind('/'):]
                localdir = str(self.arcconf.get(['tmp', 'dir'])) + sessionid
                shutil.rmtree(localdir, ignore_errors=True)
        if jobs:
            self.dbarc.Commit()
            
        select = "arcstate='cancelled' and (actpandastatus in ('cancelled', 'donecancelled', 'failed', 'donefailed')) " \
                 "and pandajobs.arcjobid = arcjobs.id"
        cleandesc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
        jobs = self.dbarc.getArcJobsInfo(select, ['arcjobs.id', 'arcjobs.appjobid', 'arcjobs.JobID'], tables='arcjobs, pandajobs')
        for job in jobs:
            self.log.info("%s: Cleaning cancelled job %d", job['appjobid'], job['id'])
            self.dbarc.updateArcJobLazy(job['id'], cleandesc)
            if job['JobID'] and job['JobID'].rfind('/') != -1:
                sessionid = job['JobID'][job['JobID'].rfind('/'):]
                localdir = str(self.arcconf.get(['tmp', 'dir'])) + sessionid
                shutil.rmtree(localdir, ignore_errors=True)
        if jobs:
            self.dbarc.Commit()


    def process(self):
        """
        Main loop
        """        
        try:
            self.log.info("Running")
            self.setSites()
            # Check for jobs that panda told us to kill and cancel them in ARC
            self.checkJobstoKill()
            # Check status of arcjobs
            # Query jobs that were submitted since last time
            self.updateStartingJobs()
            # Query jobs in running arcstate with tarcstate sooner than last run
            self.updateRunningJobs()
            # Query jobs in arcstate done and update pandajobs
            # Set to toclean
            self.updateFinishedJobs()
            # Query jobs in arcstate failed, set to tofetch
            # Query jobs in arcstate done, donefailed, cancelled and lost, set to toclean.
            # If they should be resubmitted, set arcjobid to null in pandajobs
            # If not do post-processing and fill status in pandajobs
            self.updateFailedJobs()
            # Clean up jobs left behind in arcjobs table
            self.cleanupLeftovers()
            
        except aCTSignal.ExceptInterrupt,x:
            print x
            return

        
if __name__ == '__main__':
    aas=aCTATLASStatus()
    aas.run()
