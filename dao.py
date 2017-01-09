#!/usr/bin/env python2.7

import psycopg2
import psycopg2.extras
import psycopg2.extensions

from   env       import *

## Exportable classes

class LCDao(object):
    """Wrapper around psycopg2 with additional functionality
    for logging, connection management and sql execution"""
    def __init__(self, schema):
        """Schema refers to the api entity we're referring
        to: auth, trading etc"""
        self.schema = schema
        self.__dbinfo = getenv().dbinfo
        self.__enter__()

    def __enter__(self):
        self.dbconn = psycopg2.connect(dbname='openlibarc',
                                       user=self.__dbinfo['user'],
                                       host=self.__dbinfo['host'])
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def commit(self):
        """Proxy method for committing dbconnection actions"""
        self.dbconn.commit()

    def close(self):
        """Proxy method for closing dbconnection"""
        self.dbconn.close()

    @property
    def cur(self):
        cursor = self.dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("SET search_path TO %s", [self.schema])
        return cursor

    @property
    def isolation(self):
        return self._isolation_level
    @isolation.setter
    def isolation(self, value):
        self.dbconn.set_isolation_level(value)

    class Isolation(object):
        READCMT = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
        READRPT = psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ
        SERIAL  = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE

