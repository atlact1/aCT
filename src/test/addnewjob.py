#!/usr/bin/python

import logging
from act.arc.aCTDBArc import aCTDBArc
from act.common.aCTProxy import aCTProxy
from act.common.aCTLogger import aCTLogger

logger = aCTLogger('acttest', cluster='test')
log = logger()

db = aCTDBArc(log, "act")

xrsl = '''&(executable=/bin/sleep)
           (arguments=1)
           (stdout=stdout)
           (rerun=2)
           (gmlog=gmlog)
           (*inputfiles = (file1 "srm://srm.ndgf.org:8443;cache=no/atlas/disk/atlasdatadisk/rucio/mc15_13TeV/fe/a0/AOD.07849074._019904.pool.root.1")*)
           (outputfiles = (stdout gsiftp://pcoslo5.cern.ch/bla/file1))
           '''
p=aCTProxy(logging.getLogger(), 1)
voms="atlas"
attribute="" # e.g. attribute="/atlas/Role=production"
proxypath=p.conf.get(["voms", "proxypath"])
validHours=5
proxyid =1 # p.createVOMSAttribute(voms, attribute, proxypath, validHours)

db.insertArcJobDescription(xrsl, clusterlist='gsiftp://pcoslo5.cern.ch/fork', proxyid=proxyid, maxattempts=5)
