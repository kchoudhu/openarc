#!/usr/bin/env python3

import psycopg2
import psycopg2.extras
import psycopg2.extensions

from   openarc.env import *

## Exportable classes

class OADao(object):
    """Wrapper around psycopg2 with additional functionality
    for logging, connection management and sql execution"""
    def __init__(self, schema, cdict=True):
        """Schema refers to the api entity we're referring
        to: auth, trading etc"""
        self.cdict  = cdict
        self.schema = schema
        self.__dbinfo = getenv().dbinfo
        self.dbinfo = self.__dbinfo
        self.__enter__()

    def __enter__(self):
        self.dbconn = psycopg2.connect(dbname=self.__dbinfo['dbname'],
                                       user=self.__dbinfo['user'],
                                       host=self.__dbinfo['host'],
                                       port=self.__dbinfo['port'])
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def commit(self):
        """Proxy method for committing dbconnection actions"""
        self.dbconn.commit()

    def close(self):
        """Proxy method for closing dbconnection"""
        self.dbconn.close()

    def rollback(self):
        """Proxy method for rolling back any existing action"""
        self.dbconn.rollback()

    @property
    def cur(self):
        if self.cdict:
            cursor = self.dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            cursor = self.dbconn.cursor()
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

