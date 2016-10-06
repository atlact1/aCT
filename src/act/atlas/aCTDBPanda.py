from act.db.aCTDB import aCTDB

class aCTDBPanda(aCTDB):
    
    def createTables(self):
        '''
         pandajobs:
           - id: Auto-incremented counter
           - modified: Auto-updated modification time
           - created: Creation time of panda job
           - pandaid: Panda job ID
           - pandajob: String of panda job description
           - siteName: Panda Resource
           - prodSourceLabel: Type of job (managed, test, etc)
           - arcjobid: Row ID of job in arcjobs table
           - pandastatus: Panda job status corresponding to state on the panda server
                 sent: job is retrieved from panda
                 starting: job is in aCT but not yet running
                 running: job is running on worker node
                 transferring: job is finished but uploading output files or in aCT post-processing
                 finished: job finished successfully
                 failed: job failed (code or grid failure)
           - actpandastatus: aCT internal state of panda jobs
                 In addition to above states:
                 tovalidate: job has finished or failed and output files should
                   be validated or cleaned
                 toresubmit: job will be resubmitted but first output files
                   should be cleaned
                 done: aCT is finished with this job, nothing more needs to be done
                 donefailed: aCT is finished, job failed
                 tobekilled: panda requests that the job is cancelled
                 cancelled: job was cancelled in ARC, still need to send final heartbeat
                 donecancelled: job was cancelled, nothing more needs to be done
           - theartbeat: Timestamp of last heartbeat (pstatus set)
           - priority: Job priority
           - node: Worker node on which the job is running
           - startTime: Job start time
           - endTime: Job end time
           - computingElement: CE where the job is running
           - proxyid: ID of proxy in proxies table to use for this job
           - sendhb: Flag to say whether or not to send heartbeat
           - eventranges: event ranges for event service jobs
           - corecount: Number of cores used by job
           
        pandaarchive:
          - Selected fields from above list:
            - pandaid, siteName, actpandastatus, startTime, endTime
        '''

        str="""
        create table pandajobs (
        id INTEGER PRIMARY KEY %s,
        modified TIMESTAMP,
        created TIMESTAMP,
        pandajob mediumtext,
        pandaid bigint, 
        siteName VARCHAR(255),
        prodSourceLabel VARCHAR(255),
        arcjobid integer,
        pandastatus VARCHAR(255),
        actpandastatus VARCHAR(255),
        theartbeat timestamp,
        priority integer,
        node VARCHAR(255),
        startTime TIMESTAMP,
        endTime TIMESTAMP,
        computingElement VARCHAR(255),
        proxyid integer,
        sendhb TINYINT(1) DEFAULT 1,
        eventranges mediumtext,
        corecount integer
    )
""" % self.autoincrement()

        c=self.getCursor()
        try:
            c.execute("drop table pandajobs")
        except:
            pass

        try:
            c.execute(str)
            # add indexes
            self.conn.commit()
            self.addIndex('pandajobs', 'arcjobid')
            self.addIndex('pandajobs', 'pandaid')
            self.addIndex('pandajobs', 'pandastatus')
            self.addIndex('pandajobs', 'actpandastatus')
            self.autoupdate('pandajobs', 'modified', 'id')
        except Exception as x:
            self.log.error("failed to create table pandajobs: %s" % x)
            return
        
        str="""
        create table pandaarchive (
        pandaid bigint, 
        siteName VARCHAR(255),
        actpandastatus VARCHAR(255),
        startTime TIMESTAMP DEFAULT 0,
        endTime TIMESTAMP
    )
"""
        c = self.getCursor()       
        try:
            c.execute("drop table pandaarchive")
        except:
            pass

        try:
            c.execute(str)
            self.conn.commit()
            self.addIndex('pandaarchive', 'pandaid')
        except Exception as x:
            self.log.error("failed to create table pandaarchive: %s" % x)
            return

        self.log.warning("Created panda tables")


    def insertJob(self,pandaid,pandajob,desc={}):
        desc['created']=self.getTimeStamp()
        desc['pandaid']=pandaid
        desc['pandajob']=pandajob
        desc['sendhb']=1
        s="insert into pandajobs (" + ",".join([k for k in desc.keys()]) + ") values (" + ",".join(['%s' for k in desc.keys()]) + ")"
        c=self.getCursor()
        c.execute(s,desc.values())
        self.conn.commit()
        
    def insertJobArchiveLazy(self,desc={}):
        s="insert into pandaarchive (" + ",".join([k for k in desc.keys()]) + ") values (" + ",".join(['%s' for k in desc.keys()]) + ")"
        c=self.getCursor()
        c.execute(s,desc.values())

    def deleteJob(self,pandaid):
        c=self.getCursor()
        c.execute("delete from pandajobs where pandaid="+str(pandaid))
        self.conn.commit()

    def updateJob(self,pandaid,desc):
        self.updateJobLazy(pandaid,desc)
        self.conn.commit()

    def updateJobLazy(self,pandaid,desc):
        desc['modified']=self.getTimeStamp()
        s="UPDATE pandajobs SET " + ",".join(['%s=%%s' % (k) for k in desc.keys()])
        s+=" WHERE pandaid="+str(pandaid)
        c=self.getCursor()
        c.execute(s,desc.values())

    def updateJobs(self, select, desc):
        self.updateJobsLazy(select, desc)
        self.conn.commit()

    def updateJobsLazy(self, select, desc):
        desc['modified']=self.getTimeStamp()
        s="UPDATE pandajobs SET " + ",".join(['%s=%%s' % (k) for k in desc.keys()])
        s+=" WHERE "+select
        c=self.getCursor()
        c.execute(s,desc.values())
        
    def getJob(self,pandaid,columns=[]):
        c=self.getCursor()
        c.execute("SELECT "+self._column_list2str(columns)+" FROM pandajobs WHERE pandaid="+str(pandaid))
        row=c.fetchone()
        return row

    def getJobs(self,select,columns=[]):
        c=self.getCursor()
        c.execute("SELECT "+self._column_list2str(columns)+" FROM pandajobs WHERE "+select)
        rows=c.fetchall()
        return rows

    def getNJobs(self,select):
        c=self.getCursor()
        c.execute("select count(*) from pandajobs where " + select)
        njobs=c.fetchone()['count(*)']
        return int(njobs)

    def getJobReport(self):
        c=self.getCursor()
        c.execute("select arcjobid,arcstatus from pandajobs")
        rows=c.fetchall()
        return rows

if __name__ == '__main__':
    import logging, sys
    log = logging.getLogger()
    out = logging.StreamHandler(sys.stdout)
    log.addHandler(out)
    
    adb = aCTDBPanda(log)
    adb.createTables()
