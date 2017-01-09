#!/usr/bin/env python2.7

import unittest
import sys
from textwrap import dedent as td
import psycopg2

sys.path.append('../')

from openlibarc.test import TestOABase
from openlibarc.dao  import OADao
from openlibarc.env  import initenv, getenv

class TestOADao(unittest.TestCase, TestOABase):
    def setUp(self):
        self.setUp_db()

    def tearDown(self):
        self.tearDown_db()

    def test_connection_lifecycle(self):
        """Both constructor and ctxmgr should return consistent OADao"""

        # Constructor based lifecycle
        # 1) __init__ returns valid dao
        dao_init = OADao("test")
        self.assertEqual(type(dao_init).__name__, "OADao")
        # 2) Explicit close results in unusable dao
        dao_init.close()
        with self.assertRaises(psycopg2.InterfaceError):
            dao_init.cur

        # Ctxmgr based lifecyle
        # 1) ctxmgr returns valid dao
        with OADao("test") as dao_ctx:
            self.assertEqual(type(dao_ctx).__name__, "OADao")
        # 2) Leave "with" block results in unusable dao
        with self.assertRaises(psycopg2.InterfaceError):
            dao_ctx.cur

    def test_cursor_generation(self):
        """Property cur on OADao should return cursor with correct search_path"""
        with OADao("test") as dao:
            with dao.cur as cur:
                # Cursor generation returns dicts
                self.assertEqual(type(cur).__name__, "RealDictCursor")
                # Cursor points to correct search_path
                cur.execute(self.SQL.get_search_path)
                ret = cur.fetchall()
                self.assertEqual(ret[0]['search_path'], 'test')

    def test_dao_commit(self):
        """Uncommitted transactions should not show up in database"""
        with self.dbconn.cursor() as testcur:
            testcur.execute(self.SQL.create_sample_table)
            self.dbconn.commit()

        # Nothing committed if commit() method is not called
        with OADao("test") as dao:
            with dao.cur as cur:
                for i in xrange(10):
                    cur.execute(self.SQL.insert_sample_row, [i])
            with self.dbconn.cursor() as testcur:
                testcur.execute(self.SQL.get_rows_from_sample_table)
                self.assertEqual(testcur.rowcount, 0)

        # Data committed if commit() method is called
        with OADao("test") as dao:
            with dao.cur as cur:
                for i in xrange(10):
                    cur.execute(self.SQL.insert_sample_row, [i])
                dao.commit()
            with self.dbconn.cursor() as testcur:
                testcur.execute(self.SQL.get_rows_from_sample_table)
                self.assertEqual(testcur.rowcount, 10)

    class SQL(TestOABase.SQL):
        """Boilerplate SQL needed for rest of class"""
        get_search_path = td("""
            SHOW search_path""")
        create_sample_table = td("""
            CREATE TABLE test.sample_table( field1 serial, field2 int NOT NULL)""")
        insert_sample_row = td("""
            INSERT INTO test.sample_table( field2 ) VALUES ( %s )""")
        get_rows_from_sample_table = td("""
            SELECT field2 FROM test.sample_table""")
