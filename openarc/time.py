__all__ = [
    "OATime",
    "OATimer"
]

import datetime as coretime
import time

from ._env import oalog

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

class OATimer:
    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start
        suffix = f"- [{self.identifier}]" if self.identifier is not None else str()
        oalog.info(f"{self.interval*1000:>5.0f} {suffix}")

    def __init__(self, identifier=None):
        self.identifier = identifier
