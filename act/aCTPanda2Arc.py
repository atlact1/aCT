from urlparse import urlparse
from aCTATLASProcess import aCTATLASProcess
from aCTPanda2Xrsl import aCTPanda2Xrsl

class aCTPanda2Arc(aCTATLASProcess):
    '''
    Take new jobs in Panda table and insert then into the arcjobs table.
    '''

    def __init__(self):
        aCTATLASProcess.__init__(self)
        
        self.sites = {}
        for sitename in self.conf.getList(["sites","site","name"]):
            self.sites[sitename] = {}
            self.sites[sitename]['endpoints'] = self.conf.getListCond(["sites","site"],"name=" + sitename ,["endpoints","item"])
            self.sites[sitename]['schedconfig'] = self.conf.getListCond(["sites","site"],"name=" + sitename ,["schedconfig"])[0]
        print self.sites

    def createArcJobs(self):

        jobs = self.dbpanda.getJobs("arcjobid is NULL limit 10000")

        for job in jobs:
            print job['pandajob']
            parser = aCTPanda2Xrsl(job['pandajob'], self.sites[job['siteName']]['schedconfig'])
            parser.parse()
            xrsl = parser.getXrsl()
            if xrsl is not None:
                print xrsl
                endpoints = self.sites[job['siteName']]['endpoints']
                cl = []
                for e in endpoints:
                    cl.append(urlparse(e).hostname)
                cls = ",".join(cl)
                self.log.info("Inserting job %i with clusterlist %s" % (job['id'], cls))
                aid = self.dbarc.insertArcJobDescription(xrsl, maxattempts=5, clusterlist=cls, proxyid=job['proxyid'])
                jd = {}
                jd['arcjobid'] = aid['LAST_INSERT_ID()']
                jd['actpandastatus'] = 'starting'
                self.dbpanda.updateJob(job['pandaid'], jd)
                

    def process(self):
        self.createArcJobs()


if __name__ == '__main__':

    am=aCTPanda2Arc()
    am.run()
    am.finish()
