__all__ = [ "OATime" ]

import time as coretime

class OATime(object):
    """Executes time queries on database, returning
    consistent time view to caller"""
    def __init__(self, dt=None, extcur=None):
        self.cur = extcur
        self.dt  = dt

    @property
    def now(self):
        if self.cur is None:
            from ._dao import OADao
            self.cur = OADao("openarc").cur
        self.cur.execute(self.SQL.get_current_time)
        return self.cur.fetchall()[0]['timezone']

    def to_unixtime(self):
        ms = (self.dt.microsecond/1000000.0)
        timetuple = coretime.mktime(self.dt.timetuple())
        return timetuple + ms

    class SQL(object):
        get_current_time =\
            "select now() at time zone 'utc'"
