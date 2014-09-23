# load agis.json
# if file changed since last load, reload
# getSites()
## returns dict with site info
## always check config first
## schedconfig=sitename?
## maxjobs defaults to 1M
## endpoints should be pulled out of "queues" (ce_endpoints)
## corecount defaults to 1
## catalog defaults to panda config value
import time
import os
import json
from act.common import aCTConfig

class aCTAGISParser:
    
    def __init__(self):
        self.tparse = time.time()
        self.conf=aCTConfig.aCTConfigATLAS()
        agisfile = self.conf.get(['agis','jsonfilename'])
        pilotmgr = self.conf.get(['agis','pilotmanager'])
        self.sites = self._parseAgisJson(agisfile, pilotmgr)
        self._mergeSiteDicts(self.sites, self._parseConfigSites())
        
        
    def _parseConfigSites(self):
        sites = {}
        for sitename in self.conf.getList(["sites","site","name"]):
            sites[sitename] = {}
            sites[sitename]['endpoints'] = self.conf.getListCond(["sites","site"],"name=" + sitename ,["endpoints","item"])
            try:
                sites[sitename]['schedconfig'] = self.conf.getListCond(["sites","site"],"name=" + sitename ,["schedconfig"])[0]
            except:
                sites[sitename]['schedconfig'] = sitename
            sites[sitename]['type'] = self.conf.getListCond(["sites","site"],"name=" + sitename ,["type"])[0]
            try:
                sites[sitename]['corecount'] = int(self.conf.getListCond(["sites","site"],"name=" + sitename ,["corecount"])[0])
            except:
                sites[sitename]['corecount'] = 1
            try:
                sites[sitename]['catalog'] = self.conf.getListCond(["sites","site"],"name=" + sitename ,["catalog"])[0]
            except:
                sites[sitename]['catalog'] = self.conf.get(["panda", "catalog"])
            try:
                sites[sitename]['maxjobs'] = int(self.conf.getListCond(["sites","site"],"name=" + sitename ,["maxjobs"])[0])
            except:
                sites[sitename]['maxjobs'] = 1000000
               
        return sites 
                                
    def _parseAgisJson(self, agisfilename, pilotmgr):
        # todo: first read from config, then read from agis and update if not already in sites list
        agisfile=open(agisfilename)
        agisjson=json.load(agisfile)
        sites=dict([(entry,agisjson[entry]) for entry in agisjson if agisjson[entry]['pilot_manager']==pilotmgr])
        for sitename in sites:
            if not sites[sitename].has_key('catalog'):
                sites[sitename]['catalog'] = self.conf.get(["panda", "catalog"])
            if not sites[sitename].has_key('schedconfig'):
                sites[sitename]['schedconfig'] = sitename
            if not sites[sitename].has_key('maxjobs'):
                sites[sitename]['maxjobs'] = 1000000
            if (not sites[sitename].has_key('corecount')) or (not sites[sitename]['corecount']):
                sites[sitename]['corecount'] = 1
            # pull out endpoints
            if not sites[sitename].has_key('endpoints'):
                sites[sitename]['endpoints'] = [queue['ce_endpoint'] for queue in sites[sitename]['queues']]
        return sites

    def _mergeSiteDicts(self, dict1, dict2):
        for d in dict2.keys():
            if dict1.has_key(d):
                dict1[d].update(dict2[d])
            else:
                dict1[d]=dict2[d]

    def getSites(self):
        agisfile = self.conf.get(['agis','jsonfilename'])
        pilotmgr = self.conf.get(['agis','pilotmanager'])
        # check if json file or config file changed before parsing
        if (self.tparse<os.stat(agisfile).st_mtime) or (self.tparse<os.stat(self.conf.configfile).st_mtime):
            self.sites = self._parseAgisJson(agisfile, pilotmgr)
            self._mergeSiteDicts(self.sites, self._parseConfigSites())
        return self.sites
    
if __name__ == '__main__':

    import pprint
    agisparser=aCTAGISParser()
    while 1:
        sites = agisparser.getSites()
        pprint.pprint(sites.keys())
        exit(1)
        time.sleep(1)
