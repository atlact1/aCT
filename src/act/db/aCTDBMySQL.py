import mysql.connector as mysql

class MySQLCursorDict(mysql.cursor.MySQLCursor):
    def _row_to_python(self, rowdata, desc=None):
        row = super(MySQLCursorDict, self)._row_to_python(rowdata, desc)
        if row:
            return dict(zip(self.column_names, row))
        return None

class aCTDBMySQL(object):
    """Class for MySQL specific db operations."""

    def __init__(self,logger):
        try:
            self._connect(self.dbname)
        except Exception, x:
            print Exception, x
            # if db doesnt exist, create it
            if x.errno!=1049:
                raise x
            self._connect()
            c=self.conn.cursor()
            c.execute("CREATE DATABASE "+self.dbname)

        self.log.debug("initialized aCTDBMySQL")
    
    def _connect(self, dbname=None):
        if self.socket != 'None':
            self.conn=mysql.connect(unix_socket=self.socket,db=dbname)
        elif self.user and self.passwd:
            if self.host != 'None' and self.port != 'None':
                self.conn=mysql.connect(user=self.user, password=self.passwd, host=self.host, port=self.port, db=dbname)
            else:
                self.conn=mysql.connect(user=self.user, password=self.passwd, db=dbname)
        
    def getCursor(self):
        # make sure cursor reads newest db state
        self.conn.commit()
        return self.conn.cursor(cursor_class=MySQLCursorDict)

    def timeStampLessThan(self,column,timediff):
        return "UNIX_TIMESTAMP("+column+") < UNIX_TIMESTAMP(UTC_TIMESTAMP()) - "+str(timediff)
    
    def timeStampGreaterThan(self,column,timediff):
        return "UNIX_TIMESTAMP("+column+") > UNIX_TIMESTAMP(UTC_TIMESTAMP()) - "+str(timediff)

    def tableExists(self, tablename):
        c = self.getCursor()
        c.execute("show tables like 'arcjobs'")
        row = c.fetchone()
        self.conn.commit()
        return row

    def autoincrement(self):
        return "AUTO_INCREMENT"
    
    def autoupdate(self, tablename, column, uniqueid):
        # MySQL automatically updates first timestamp column
        pass

    def addIndex(self, tablename, index):
        c = self.getCursor()
        c.execute("ALTER TABLE %s ADD INDEX (%s)" % (tablename, index))
        self.conn.commit()

    def lastInsertID(self, cursor):
        cursor.execute("SELECT LAST_INSERT_ID()")
        row = cursor.fetchone()
        return row['LAST_INSERT_ID()']

    def insertRow(self, tablename, attributes):
        c = self.getCursor()
        c.execute("INSERT INTO %s (%s) VALUES (%s)" %
                   (tablename, ','.join([k for k in attributes.keys()]),
                    ','.join(['%%s' for v in attributes.values])),
                  attributes.values())
        id = self.lastInsertID(c)
        self.conn.commit()
        return id

    def updateRow(self, tablename, attributes, whereclause):
        c = self.getCursor()
        c.execute("UPDATE %s SET %s where %s" % 
                  (tablename, ",".join(['%s=%%s' % (k) for k in attributes.keys()]), whereclause),
                  attributes.values())

    def getMutexLock(self, lock_name, timeout=2):
        """
        Function to get named lock. Returns 1 if lock was obtained, 0 if attempt timed out, None if error occured.
        """
        c=self.getCursor()
        select="GET_LOCK('"+lock_name+"',"+str(timeout)+")"
        c.execute("SELECT "+select)
        return c.fetchone()[select]
    
    def releaseMutexLock(self, lock_name):
        """
        Function to release named lock. Returns 1 if lock was released, 0 if someone else owns the lock, None if error occured.
        """
        c=self.getCursor()
        select="RELEASE_LOCK('"+lock_name+"')"
        c.execute("SELECT "+select)
        return c.fetchone()[select]
