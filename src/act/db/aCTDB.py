import datetime
from aCTDBMS import aCTDBMS

class aCTDB(aCTDBMS):

    def __init__(self, logger):
        # inherit DB Mgmt System from aCTDBMS
        aCTDBMS.__init__(self, logger)
    
    def _column_list2str(self,columns):
        s=""
        if columns:
            for col in columns:
                s+=col+", "
            s=s.strip(", ")
        else:
            s="*"
        return s
    
    def getTimeStamp(self, seconds=None):
        # todo: move to aCTDBMS and use internal dbms mechanisms for converting
        if seconds:
            return datetime.datetime.utcfromtimestamp(seconds).isoformat()
        else:
            return datetime.datetime.utcnow().isoformat()

    def Commit(self, lock=False):
        self.conn.commit()
        if lock:
            c=self.getCursor()
            c.execute("UNLOCK TABLES")
