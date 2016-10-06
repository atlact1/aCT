#!/usr/bin/python

import re
import subprocess
import aCTLogger
from act.arc import aCTDBArc
from act.atlas import aCTDBPanda

class aCTStatus:
    
    def __init__(self):
        self.logger=aCTLogger.aCTLogger("aCTReport")
        self.log=self.logger()

        self.db=aCTDBArc.aCTDBArc(self.log)
        self.pandadb=aCTDBPanda.aCTDBPanda(self.log)

    def ProcessReport(self):
        actprocscmd = 'ps ax -ww -o etime,args'
        p = subprocess.Popen(actprocscmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        
        if err:
            print 'Error: could not run ps command: %s' % err
            return
        
        # Group processes by cluster
        cluster_procs = {}
        longprocesses = []
        for line in out.split('\n'):
            reg = re.match('\s*(.*) /.*python.* .*(aCT\w*)\.py\s?(\S*)', line)
            if reg:
                runningtime, process, cluster = reg.groups()
                # ignore Main and this process
                if process == 'aCTReport' or process == 'aCTMain':
                    continue
                if cluster == '':
                    cluster = '(no cluster defined)'
                elif not re.match('\d\d:\d\d$', runningtime):
                    # Check for overrunning processes
                    longprocesses.append((process, cluster, runningtime))
                if cluster in cluster_procs:
                    cluster_procs[cluster].append(process)
                else:
                    cluster_procs[cluster] = [process]
        
        print 'Active processes per cluster:'
        for cluster in sorted(cluster_procs):
            procs = cluster_procs[cluster]
            procs.sort()
            print '%38s: %s' % (cluster, ' '.join(procs))
        print
        for proc in longprocesses:
            print 'WARNING: %s for %s running for more than one hour (%s)' % proc
        print
        
    def PandaReport(self):
        c=self.db.conn.cursor()
        c.execute("select sitename, actpandastatus from pandajobs")
        rows=c.fetchall()
        rep={}
        rtot={}
        states = ["sent", "starting", "running", "tovalidate", "toresubmit",
                  "toclean", "finished", "done", "failed", "donefailed",
                  "tobekilled", "cancelled", "donecancelled"]

        print "All Panda jobs: %d" % len(rows)
        print "%29s %s" % (' ', ' '.join(['%9s' % s for s in states]))
        for r in rows:

            site, state = (str(r[0]), str(r[1]))
            reg=re.search('.+//([^:]+)',str(r[0]))
            cl=""
            try:
                cl=reg.group(1)
            except:
                cl='WaitingSubmission'

            jid=str(r[1])
            if jid == 'None':
                jid="Other"

            try:
                rep[site][state]+=1
            except:
                try:
                    rep[site][state]=1
                except:
                    rep[site]={}
                    rep[site][state]=1
            try:
                rtot[state]+=1
            except:
                rtot[state]=1

        for k in sorted(rep.keys()):
            log="%28s:" % k[:28]
            for s in states:
                try:
                    log += '%10s' % str(rep[k][s])
                except KeyError:
                    log += '%10s' % '-'
            print log
        log = "%28s:" % "Totals"
        for s in states:
            try:
                log += '%10s' % str(rtot[s])
            except:
                log += '%10s' % '-'
        print log+'\n\n'

    def JobReport(self):
        c=self.db.conn.cursor()
        c.execute("select jobid,state from arcjobs")
        rows=c.fetchall()
        rep={}
        rtot={}
        states = ["Undefined", "Accepted", "Preparing", "Submitting",
                 "Queuing", "Running", "Finishing", "Finished", "Hold", "Killed",
                 "Failed", "Deleted", "Other"]

        print "All ARC jobs: %d" % len(rows)
        print "%29s %s" % (' ', ' '.join(['%9s' % s for s in states]))
        for r in rows:

            reg=re.search('.+//([^:]+)',str(r[0]))
            cl=""
            try:
                cl=reg.group(1)
            except:
                cl='WaitingSubmission'

            jid=str(r[1])
            if jid == 'None':
                jid="Other"

            try:
                rep[cl][jid]+=1
            except:
                try:
                    rep[cl][jid]=1
                except:
                    rep[cl]={}
                    rep[cl][jid]=1
            try:
                rtot[jid]+=1
            except:
                rtot[jid]=1

        for k in sorted(rep.keys()):
            log="%28s:" % k[:28]
            for s in states:
                try:
                    log += '%10s' % str(rep[k][s])
                except KeyError:
                    log += '%10s' % '-'
            print log
        log = "%28s:" % "Totals"
        for s in states:
            try:
                log += '%10s' % str(rtot[s])
            except:
                log += '%10s' % '-'
        print log+'\n\n'

        

acts=aCTStatus()
acts.PandaReport()
acts.JobReport()
acts.ProcessReport()

