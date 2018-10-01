#!/usr/bin/env python3

from gevent import monkey
monkey.patch_all()

import psycopg2
import psycopg2.extras
import psycopg2.extensions

from   textwrap    import dedent as td
from   openarc.env import *


## Exportable classes

class OADao(object):
    """Wrapper around psycopg2 with additional functionality
    for logging, connection management and sql execution"""
    def __init__(self, schema, cdict=True, hold_commit=False):
        """Schema refers to the api entity we're referring
        to: auth, trading etc"""
        self.cdict   = cdict
        self.dbconn  = gctx().db_conn
        self.schema  = schema
        self._cursor = None
        self._hold_commit = hold_commit
        self.__enter__()

    def __enter__(self):
        return self

    def __exit__(self, exc, value, traceback):
        if exc:
            self.rollback()
        else:
            self.commit()
        self._cursor = None

    def commit(self):
        """Proxy method for committing dbconnection actions"""
        self.dbconn.commit()

    def rollback(self):
        """Proxy method for rolling back any existing action"""
        self.dbconn.rollback()

    @property
    def cur(self):
        if not self._cursor:
            if self.cdict:
                self._cursor = self.dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            else:
                self._cursor = self.dbconn.cursor()
            self._cursor.execute("SET search_path TO %s", [self.schema])
        return self._cursor

    @property
    def description(self):
        return self.cur.description

    def execute(self, query, params=[]):
        results = None
        if gctx().logger.SQL:
            print(td(self.cur.mogrify(query, params).decode('utf-8')))
        try:
            self.cur.execute(query, params)
            try:
                results = self.cur.fetchall()
            except:
                pass
            if not self._hold_commit:
                self.commit()
        except:
            if not self._hold_commit:
                self.rollback()
            raise
        return results

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

