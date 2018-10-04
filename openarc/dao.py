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
        self.cur_finalize(exc)

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

    def cur_finalize(self, exc):
        if exc:
            self.rollback()
        else:
            self.commit()
        self._cursor = None

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

class OADbTransaction(object):
    """Manipulates the global dbconn object so that all OAGs see the same
    cursor. This is the functional equivalent of a semantic transaction. Captures
    non-OAG database transactions, but only as an unintended side effect."""
    def __init__(self, transname):
        self.dao = None

    def __enter__(self):

        if not self.dao:
            gctx().db_txndao = OADao("openarc", hold_commit=True)
            self.dao = gctx().db_txndao
            self.dao.cur
        return self

    def __exit__(self, exc, value, traceback):
        self.dao.cur_finalize(exc)
        gctx().db_txndao = None
        self.dao = None
