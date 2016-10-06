from act.common import aCTConfig

supported_dbms={}

try:
    from aCTDBSqlite import aCTDBSqlite
    supported_dbms['sqlite']=aCTDBSqlite
except:
    pass
try:
    from aCTDBMySQL import aCTDBMySQL
    supported_dbms['mysql']=aCTDBMySQL
except:
    pass
try:
    from aCTDBOracle import aCTDBOracle
    supported_dbms['oracle']=aCTDBOracle
except:
    pass

config=aCTConfig.aCTConfigARC()
dbtype=config.get(('db', 'type')).lower()

class aCTDBMS(supported_dbms[dbtype]):
    """Class for generic DB Mgmt System db operations. Inherit specifics from its speciallized superclass depending on configured dbtype."""
    
    def __init__(self, logger):
        self.log=logger
        # TODO: Find more generic way to get db config vars
        if dbtype=='sqlite':
            self.file=str(config.get(('db', 'file')))
            aCTDBSqlite.__init__(self, logger)
        elif dbtype=='mysql':
            self.socket=str(config.get(('db', 'socket')))
            self.dbname=str(config.get(('db', 'name')))
            self.user=str(config.get(('db', 'user')))
            self.passwd=str(config.get(('db', 'password')))
            self.host=str(config.get(('db', 'host')))
            self.port=str(config.get(('db', 'port')))
            aCTDBMySQL.__init__(self, logger)
        elif dbtype=='oracle':
            aCTDBOracle.__init__(self, logger)
        else:
            raise Exception, "DB type %s is not implemented."%dbtype

    def getCursor(self):
        return super(aCTDBMS, self).getCursor()

    def timeStampLessThan(self,column,timediff):
        return super(aCTDBMS, self).timeStampLessThan(column,timediff)
    
    def timeStampGreaterThan(self,column,timediff):
        return super(aCTDBMS, self).timeStampGreaterThan(column,timediff)

    def addLock(self):
        return super(aCTDBMS, self).addLock()

    def getMutexLock(self, lock_name, timeout=2):
        return super(aCTDBMS, self).getMutexLock(lock_name, timeout)

    def releaseMutexLock(self, lock_name):
        return super(aCTDBMS, self).releaseMutexLock(lock_name)
