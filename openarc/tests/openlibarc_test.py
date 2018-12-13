#!/usr/bin/env python3

import unittest
import sys
from textwrap import dedent as td
import psycopg2

sys.path.append('../')

from openarc.test import TestOABase
from openarc.dao  import OADao, OADbTransaction
from openarc.env  import initenv, getenv

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
        self.assertEqual(type(dao_init), OADao)

        # Ctxmgr based lifecyle
        # 1) ctxmgr returns valid dao
        with OADao("test") as dao_ctx:
            self.assertEqual(type(dao_ctx), OADao)

    def test_cursor_generation(self):
        """Property cur on OADao should return cursor with correct search_path"""
        with OADao("test") as dao:
            # Cursor generation returns dicts
            self.assertEqual(type(dao.cur).__name__, "RealDictCursor")
            # Cursor points to correct search_path
            ret = dao.execute(self.SQL.get_search_path)
            self.assertEqual(ret[0]['search_path'], 'test')

    def test_dao_commit(self):
        """Uncommitted transactions should not show up in database"""
        with self.dbconn.cursor() as testcur:
            testcur.execute(self.SQL.create_sample_table)
            self.dbconn.commit()

        # Nothing committed if exception is raised in ctxmgr
        try:
            with OADao("test", trans_commit_hold=True) as dao:
                for i in range(10):
                    dao.execute(self.SQL.insert_sample_row, [i])
                raise Exception()
        except:
            pass

        with self.dbconn.cursor() as testcur:
            testcur.execute(self.SQL.get_rows_from_sample_table)
            self.assertEqual(testcur.rowcount, 0)

        # Nothing committed if rollback explicitly called
        with OADao("test", trans_commit_hold=True) as dao:
            for i in range(10):
                dao.execute(self.SQL.insert_sample_row, [i])
            dao.rollback()

        with self.dbconn.cursor() as testcur:
            testcur.execute(self.SQL.get_rows_from_sample_table)
            self.assertEqual(testcur.rowcount, 0)

        # Data committed if commit() method is called
        with OADao("test", trans_commit_hold=True) as dao:
            for i in range(10):
                dao.execute(self.SQL.insert_sample_row, [i])
            dao.commit()

        with self.dbconn.cursor() as testcur:
            testcur.execute(self.SQL.get_rows_from_sample_table)
            self.assertEqual(testcur.rowcount, 10)

        # Data committed even if commit is not called
        with OADao("test") as dao:
            for i in range(10):
                dao.execute(self.SQL.insert_sample_row, [i])

        with self.dbconn.cursor() as testcur:
            testcur.execute(self.SQL.get_rows_from_sample_table)
            self.assertEqual(testcur.rowcount, 20)

    def test_nested_transactions(self):
        with OADbTransaction("Level 1") as trans1:
            self.assertEqual(trans1.dao.trans_depth, 2)
            with OADbTransaction("Level 2") as trans2:
                self.assertEqual(trans1.dao, trans2.dao)
                self.assertEqual(trans2.dao.trans_depth, 3)
                with OADbTransaction("Level 3") as trans3:
                    self.assertEqual(trans1.dao, trans3.dao)
                    self.assertEqual(trans3.dao.trans_depth, 4)
                self.assertEqual(trans2.dao.trans_depth, 3)
            self.assertEqual(trans1.dao.trans_depth, 2)
        self.assertEqual(trans1.dao, None)

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
