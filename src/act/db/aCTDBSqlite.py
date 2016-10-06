import sqlite3 as sqlite

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

class aCTDBSqlite(object):
    """Class for Sqlite specific db operations."""
    
    def __init__(self,logger):
        try:
            self.conn=sqlite.connect(self.file, 1800, detect_types=sqlite.PARSE_DECLTYPES)
        except Exception, x:
            raise Exception, "Could not connect to sqlite: " + str(x)
        self.conn.row_factory=dict_factory
        self.conn.execute('''PRAGMA synchronous=OFF''')
        self.log.info("initialized aCTDBSqlite")

    def getCursor(self):
        return self.conn.cursor()

    def timeStampLessThan(self,column,timediff):
        return "datetime("+column+") < datetime('now', '-"+str(timediff)+" seconds')"

    def timeStampGreaterThan(self,column,timediff):
        return "datetime("+column+") > datetime('now', '-"+str(timediff)+" seconds')"

    def tableExists(self, tablename):
        c = self.getCursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' and name=?", (tablename,))
        row = c.fetchone()
        self.log.info(row)
        return row

    def autoincrement(self):
        return ''

    def autoupdate(self, tablename, column, uniqueid):
        c = self.getCursor()
        q = '''CREATE TRIGGER {t}_{c} 
                AFTER UPDATE ON {t}
                FOR EACH ROW BEGIN 
                UPDATE {t} SET {c} = CURRENT_TIMESTAMP 
                WHERE {id} = old.{id};
                END'''.format(t=tablename, c=column, id=uniqueid)
        c.execute(q)
        self.conn.commit()
         
    def addIndex(self, tablename, index):
        c = self.getCursor()
        c.execute("CREATE INDEX {t}_{i} on {t} ({i})".format(t=tablename, i=index))
        self.conn.commit()

    def lastInsertID(self, cursor):
        return cursor.lastrowid

    def addLock(self):
        # SQLite does not support row locking
        return ""

    def getMutexLock(self):
        # SQLite does not support mutex locking
        return
    
    def releaseMutexLock(self):
        # SQLite does not support mutex locking
        return
