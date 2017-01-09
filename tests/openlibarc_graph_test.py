#!/usr/bin/env python2.7

import unittest
import sys
from textwrap import dedent as td

sys.path.append('../')

from openlibarc.dao       import *
from openlibarc.exception import *
from openlibarc.graph     import *
from openlibarc.test      import *

class TestOAGraphRootNode(unittest.TestCase, TestOABase):
    def setUp(self):
        self.setUp_db()

    def tearDown(self):
        self.tearDown_db()

    def test_uniquenode_creation(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            self.dbconn.commit()

        lc = OAGraphUniqNode().create({
                'field2' : 485,
                'field3' : 486
             })
        lc_chk = OAGraphUniqNode((485,))

        self.assertEqual(lc.field2, lc_chk.field2)
        self.assertEqual(lc.field3, lc_chk.field3)

    def test_multinode_creation(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            self.dbconn.commit()

        with OADao("test") as dao:
            with dao.cur as cur:
                for i in xrange(10):
                    lc = OAGraphMultiNode(extcur=cur).create({
                            'field2' : i,
                            'field3' : 2
                         })
            dao.commit()

        i = 0
        mn_chk = OAGraphMultiNode((2,))
        for mn in mn_chk:
            self.assertEqual(mn.field3, 2)
            self.assertEqual(mn.field2, i)
            i += 1

    def test_db_to_property_translation_uniquenode(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            for i in xrange(10):
                setupcur.execute(self.SQL.insert_sample_row, [i, 2])
            self.dbconn.commit()

        lc = OAGraphUniqNode((2,))
        self.assertEqual(lc._field1, 3)
        self.assertEqual(lc.field2, 2)
        self.assertEqual(lc.field3, 2)

    def test_uniquenode_prop_based_update(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            for i in xrange(10):
                setupcur.execute(self.SQL.insert_sample_row, [i, 2])
            self.dbconn.commit()

        lc = OAGraphUniqNode((2,))
        lc.field2 = 14
        lc.field3 = 14
        lc.update()

        lc_upd = OAGraphUniqNode((14,))
        self.assertEqual(lc_upd._field1, lc._field1)
        self.assertEqual(lc_upd.field2, 14)
        self.assertEqual(lc_upd.field3, 14)

    def test_multinode_prop_based_update(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            for i in xrange(10):
                setupcur.execute(self.SQL.insert_sample_row, [i, 2])
            self.dbconn.commit()

        nm = OAGraphMultiNode((2,))
        n = next(nm)
        n.field2 = 14
        n.update()

        nm_upd = OAGraphMultiNode((2,))
        nu = next(nm_upd)
        self.assertEqual(nu._field1, n._field1)
        self.assertEqual(nu.field2, 14)

    def test_data_retrieval_failure(self):
        """Attempt to retrieve no data fails with exception"""
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            self.dbconn.commit()
        with self.assertRaises(OAGraphRetrieveError):
            lc = OAGraphUniqNode((2,))

    def test_data_retrieval_with_external_cursor(self):
        """Graph nodes can do internal queries with external cursors"""
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            self.dbconn.commit()
            for i in xrange(2):
                setupcur.execute(self.SQL.insert_sample_row, [2, 2])
            # Before commit, internal cursor impl throws retrieval error,
            # but external cursor impl sees contents
            with self.assertRaises(OAGraphRetrieveError):
                lc = OAGraphUniqNode((2,))
            with self.assertRaises(OAGraphIntegrityError):
                lc = OAGraphUniqNode((2,), extcur=setupcur)

    def test_mutltiquery_node_retrieval(self):
        """Mutliple queries can be used to retrieve data for graph node"""
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            setupcur.execute(self.SQL.insert_sample_row, [2, 2])
            self.dbconn.commit()

        # Default "id" query does not throw
        lc = OAGraphUniqNode((2,))
        self.assertEqual(lc.is_unique, True)

        # Secondary "id_2" query also works
        lc = OAGraphUniqNode((2,2), indexparm="id_2")
        self.assertEqual(lc.is_unique, True)

    def test_uniquenode_data_integrity(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            for i in xrange(2):
                setupcur.execute(self.SQL.insert_sample_row, [2, 2])
            self.dbconn.commit()

        with self.assertRaises(Exception):
            lc = OAGraphUniqNode((2,))

    def test_db_to_property_translation_multinode(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            for i in xrange(10):
                setupcur.execute(self.SQL.insert_sample_row, [i, 2])
            self.dbconn.commit()

        node_multi = OAGraphMultiNode((2,))

        for idx, lc in enumerate(node_multi):
            self.assertEqual(idx, lc._field1-1)
            self.assertEqual(idx, lc.field2)
            self.assertEqual(2, lc.field3)

    def test_multinode_reuse(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            for i in xrange(10):
                setupcur.execute(self.SQL.insert_sample_row, [i, 2])
            self.dbconn.commit()

        node_multi = OAGraphMultiNode((2,))

        # Exhausted MultiNode is implicitly refreshed
        for lc in node_multi:
            continue
        accumulator = [ lc._field1 for lc in node_multi ]
        self.assertEqual(len(accumulator), 10)

        # Explicitly refreshed MultiNode allows full iteration again
        for i, lc in enumerate(node_multi):
            if i == 5:
                break
        node_multi.refresh()
        accumulator_refreshed = [ lc._field1 for lc in node_multi ]
        self.assertEqual(len(accumulator_refreshed), 10)

    def test_lcnode_size_reporting(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            for i in xrange(10):
                setupcur.execute(self.SQL.insert_sample_row, [i, 2])
            self.dbconn.commit()

        node_multi = OAGraphMultiNode((2,))
        self.assertEqual(node_multi.size, 10)

    def test_lcnode_lcgprop_caching(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            for i in xrange(10):
                setupcur.execute(self.SQL.insert_sample_row, [i, 2])
            self.dbconn.commit()

        node_uniq = OAGraphUniqNode((2,))
        self.assertEqual(len(node_uniq._lcgcache), 0)
        sub_node_uniq = node_uniq.subnode
        self.assertEqual(sub_node_uniq._field1, node_uniq._field1)
        self.assertEqual(len(node_uniq._lcgcache), 1)

    def test_multinode_lcgprop_cache_invalidation(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            for i in xrange(10):
                setupcur.execute(self.SQL.insert_sample_row, [i, 2])
            self.dbconn.commit()

        node_multi = OAGraphMultiNode((2,))

        for i, nm in enumerate(node_multi):
            self.assertEqual(len(nm._lcgcache), 0)
            sub_node_nm = nm.subnode
            self.assertEqual(len(nm._lcgcache), 1)

    class SQL(TestOABase.SQL):
        """Boilerplate SQL needed for rest of class"""
        get_search_path = td("""
            SHOW search_path""")
        create_sample_table = td("""
            CREATE TABLE test.sample_table( _field1 serial, field2 int NOT NULL, field3 int NOT NULL)""")
        insert_sample_row = td("""
            INSERT INTO test.sample_table( field2, field3 ) VALUES ( %s, %s )""")
        get_rows_from_sample_table = td("""
            SELECT field2 FROM test.sample_table""")

class OAGraphUniqNode(OAGraphRootNode):
    @property
    def is_unique(self): return True

    @property
    def dbcontext(self): return "test"

    @lcgprop
    def subnode(self): return OAGraphUniqNode((self.field2,))

    @property
    def SQL(self):
      return {
        "read" : {
          "id" : td("""
              SELECT _field1, field2, field3
                FROM test.sample_table
               WHERE field2=%s
            ORDER BY _field1"""),
          "id_2" : td("""
              SELECT _field1, field2, field3
                FROM test.sample_table
               WHERE field2=%s
                     AND field3=%s
            ORDER BY _field1""")
        },
        "update" : {
          "id" : td("""
             UPDATE test.sample_table
                SET %s
              WHERE _field1=%s""")
        },
        "insert" : {
          "id" : td("""
        INSERT INTO test.sample_table(%s)
             VALUES (%s)
          RETURNING field2""")
        }
      }

class OAGraphMultiNode(OAGraphRootNode):
    @property
    def is_unique(self): return False

    @property
    def dbcontext(self): return "test"

    @lcgprop
    def subnode(self): return OAGraphMultiNode((self.field3,))

    @property
    def SQL(self):
      return {
        "read" : {
          "id" : td("""
              SELECT _field1, field2, field3
                FROM test.sample_table
               WHERE field3=%s
            ORDER BY _field1""")
        },
        "update" : {
          "id" : td("""
              UPDATE test.sample_table
                 SET %s
               WHERE _field1=%s""")
        },
        "insert" : {
          "id" : td("""
        INSERT INTO test.sample_table(%s)
             VALUES (%s)
          RETURNING field3""")
        }
      }
