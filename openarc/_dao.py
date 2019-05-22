__all__ = [
    'OADao',
    'OADbTransaction'
]

import binascii
import os
import psycopg2
import psycopg2.extras
import psycopg2.extensions

from   textwrap    import dedent as td

from   ._env       import oactx, oalog

from   openarc.exception import OAGraphStorageError

## Exportable classes

class OADao(object):
    """Wrapper around psycopg2 with additional functionality
    for logging, connection management and sql execution"""
    def __init__(self, schema, cdict=True, trans_commit_hold=False):
        """Schema refers to the api entity we're referring
        to: auth, trading etc"""
        self.cdict   = cdict
        self.dbconn  = oactx.db_conn
        self.schema  = schema
        self.trans_depth  = 1

        self._cursor = None
        self._trans_commit_hold = trans_commit_hold
        self.__enter__()

    ##################################################
    # OADaos should not be used in ctx, but whatever #
    ##################################################
    def __enter__(self):                             #
        return self                                  #
                                                     #
    def __exit__(self, exc, value, traceback):       #
        if not self._trans_commit_hold:              #
            self.cur_finalize(exc)                   #
    ##################################################

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

    def execute(self, query, params=[], savepoint=False, cdict=True, extcur=None):
        results = None

        cur = self.cur
        if cdict != self.cdict:
            cur = self.dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SET search_path TO %s", [self.schema])
        if type(extcur)==list:
            while len(extcur)>0:
                extcur.pop()
            extcur.append(cur)

        if savepoint:
            savepoint_name = 'sp_'+binascii.hexlify(os.urandom(7)).decode('utf-8')
            oalog.debug(f"Initializing savepoint [{savepoint_name}]", f='sql')
            cur.execute("SAVEPOINT %s" % savepoint_name)

        oalog.debug(f"{td(cur.mogrify(query, params).decode('utf-8'))}", f='sql')

        try:
            try:
                cur.execute(query, params)
            except Exception as e:
                raise OAGraphStorageError(str(e), e)
            try:
                results = cur.fetchall()
            except:
                pass
        except:
            if savepoint:
                oalog.debug(f"Rolling back to savepoint {savepoint_name}", f='sql')
                cur.execute("ROLLBACK TO SAVEPOINT %s" % savepoint_name)

            if not self._trans_commit_hold:
                self.rollback()
            raise
        else:
            if not self._trans_commit_hold:
                self.commit()

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
        self.dao = oactx.db_txndao

    def __enter__(self):
        if not self.dao:
            oactx.db_txndao = OADao("openarc", trans_commit_hold=True)
            self.dao = oactx.db_txndao
            self.dao.cur
        self.dao.trans_depth += 1
        return self

    def __exit__(self, exc, value, traceback):
        self.dao.trans_depth -= 1
        if self.dao.trans_depth == 1:
            self.dao.cur_finalize(exc)
            oactx.db_txndao = None
            self.dao = None
