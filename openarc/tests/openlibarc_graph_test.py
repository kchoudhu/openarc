#!/usr/bin/env python2.7

import unittest
import sys
from textwrap import dedent as td

sys.path.append('../')

from openarc.dao       import *
from openarc.exception import *
from openarc.graph     import *
from openarc.test      import *

class TestOAGraphRootNode(unittest.TestCase, TestOABase):
    def setUp(self):
        self.setUp_db()

    def tearDown(self):
        self.tearDown_db()

    def test_graph_isolation_control(self):
        with self.dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            self.dbconn.commit()

            # Object not created until commit
            oa = OAG_UniqNode(extcur=setupcur).create({
                    'field2' : 485,
                    'field3' : 486
                 })

            with self.assertRaises(OAGraphRetrieveError):
                oa_chk = OAG_UniqNode((485,))

            self.dbconn.commit()
            oa_chk = OAG_UniqNode((485,))
            self.assertEqual(oa.field2, oa_chk.field2)
            self.assertEqual(oa.field3, oa_chk.field3)

            # Object not updated until commit
            oa.field2 = 487
            oa.update()
            # -> no change yet, retrieval ok
            oa_chk = OAG_UniqNode((485,))
            self.assertEqual(oa_chk.field2, 485)
            self.assertEqual(oa_chk.field3, 486)
            self.dbconn.commit()
            # -> update commited, retrieval fails
            with self.assertRaises(OAGraphRetrieveError):
                oa_chk = OAG_UniqNode((485,))
            oa_chk = OAG_UniqNode((487,))
            self.assertEqual(oa.field2, oa_chk.field2)
            self.assertEqual(oa.field3, oa_chk.field3)

    def test_uniquenode_creation(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            self.dbconn.commit()

        oa = OAG_UniqNode().create({
                'field2' : 485,
                'field3' : 486
             })
        oa_chk = OAG_UniqNode((485,))

        self.assertEqual(oa.field2, oa_chk.field2)
        self.assertEqual(oa.field3, oa_chk.field3)

    def test_multinode_creation(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            self.dbconn.commit()

        with OADao("test") as dao:
            with dao.cur as cur:
                for i in xrange(10):
                    oa = OAG_MultiNode(extcur=cur).create({
                            'field2' : i,
                            'field3' : 2
                         })
            dao.commit()

        i = 0
        mn_chk = OAG_MultiNode((2,))
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

        oa = OAG_UniqNode((2,))
        self.assertEqual(oa._field1, 3)
        self.assertEqual(oa.field2, 2)
        self.assertEqual(oa.field3, 2)

    def test_uniquenode_prop_based_update(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            for i in xrange(10):
                setupcur.execute(self.SQL.insert_sample_row, [i, 2])
            self.dbconn.commit()

        oa = OAG_UniqNode((2,))
        oa.field2 = 14
        oa.field3 = 14
        oa.update()

        oa_upd = OAG_UniqNode((14,))
        self.assertEqual(oa_upd._field1, oa._field1)
        self.assertEqual(oa_upd.field2, 14)
        self.assertEqual(oa_upd.field3, 14)

    def test_uniquenode_dict_based_update(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            for i in xrange(10):
                setupcur.execute(self.SQL.insert_sample_row, [i, 2])
            self.dbconn.commit()

        oa = OAG_UniqNode((2,))
        oa.update({
            'field2' : 14,
            'field3' : 14
        })

        oa_upd = OAG_UniqNode((14,))
        self.assertEqual(oa_upd._field1, oa._field1)
        self.assertEqual(oa_upd.field2, 14)
        self.assertEqual(oa_upd.field3, 14)

    def test_multinode_prop_based_update(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            for i in xrange(10):
                setupcur.execute(self.SQL.insert_sample_row, [i, 2])
            self.dbconn.commit()

        nm = OAG_MultiNode((2,))
        n = next(nm)
        n.field2 = 14
        n.update()

        nm_upd = OAG_MultiNode((2,))
        nu = next(nm_upd)
        self.assertEqual(nu._field1, n._field1)
        self.assertEqual(nu.field2, 14)

    def test_multinode_dict_based_update(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            for i in xrange(10):
                setupcur.execute(self.SQL.insert_sample_row, [i, 2])
            self.dbconn.commit()

        nm = OAG_MultiNode((2,))
        n = next(nm)
        n.update({
            'field2' : 14
        })

        nm_upd = OAG_MultiNode((2,))
        nu = next(nm_upd)
        self.assertEqual(nu._field1, n._field1)
        self.assertEqual(nu.field2, 14)

    def test_data_retrieval_failure(self):
        """Attempt to retrieve no data fails with exception"""
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            self.dbconn.commit()
        with self.assertRaises(OAGraphRetrieveError):
            oa = OAG_UniqNode((2,))

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
                oa = OAG_UniqNode((2,))
            with self.assertRaises(OAGraphIntegrityError):
                oa = OAG_UniqNode((2,), extcur=setupcur)

    def test_mutltiquery_node_retrieval(self):
        """Mutliple queries can be used to retrieve data for graph node"""
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            setupcur.execute(self.SQL.insert_sample_row, [2, 2])
            self.dbconn.commit()

        # Default "id" query does not throw
        oa = OAG_UniqNode((2,))
        self.assertEqual(oa.is_unique, True)

        # Secondary "id_2" query also works
        oa = OAG_UniqNode((2,2), indexprm="id_2")
        self.assertEqual(oa.is_unique, True)

    def test_uniquenode_data_integrity(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            for i in xrange(2):
                setupcur.execute(self.SQL.insert_sample_row, [2, 2])
            self.dbconn.commit()

        with self.assertRaises(Exception):
            oa = OAG_UniqNode((2,))

    def test_db_to_property_translation_multinode(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            for i in xrange(10):
                setupcur.execute(self.SQL.insert_sample_row, [i, 2])
            self.dbconn.commit()

        node_multi = OAG_MultiNode((2,))

        for idx, oa in enumerate(node_multi):
            self.assertEqual(idx, oa._field1-1)
            self.assertEqual(idx, oa.field2)
            self.assertEqual(2, oa.field3)

    def test_multinode_reuse(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            for i in xrange(10):
                setupcur.execute(self.SQL.insert_sample_row, [i, 2])
            self.dbconn.commit()

        node_multi = OAG_MultiNode((2,))

        # Exhausted MultiNode is implicitly refreshed
        for oa in node_multi:
            continue
        accumulator = [ oa._field1 for oa in node_multi ]
        self.assertEqual(len(accumulator), 10)

        # Explicitly refreshed MultiNode allows full iteration again
        for i, oa in enumerate(node_multi):
            if i == 5:
                break
        node_multi.refresh()
        accumulator_refreshed = [ oa._field1 for oa in node_multi ]
        self.assertEqual(len(accumulator_refreshed), 10)

    def test_oanode_size_reporting(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            for i in xrange(10):
                setupcur.execute(self.SQL.insert_sample_row, [i, 2])
            self.dbconn.commit()

        node_multi = OAG_MultiNode((2,))
        self.assertEqual(node_multi.size, 10)

    def test_oanode_oagprop_caching(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            for i in xrange(10):
                setupcur.execute(self.SQL.insert_sample_row, [i, 2])
            self.dbconn.commit()

        node_uniq = OAG_UniqNode((2,))
        self.assertEqual(len(node_uniq._oagcache), 0)
        sub_node_uniq = node_uniq.subnode
        self.assertEqual(sub_node_uniq._field1, node_uniq._field1)
        self.assertEqual(len(node_uniq._oagcache), 1)

    def test_multinode_oagprop_cache_invalidation(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            for i in xrange(10):
                setupcur.execute(self.SQL.insert_sample_row, [i, 2])
            self.dbconn.commit()

        node_multi = OAG_MultiNode((2,))

        for i, nm in enumerate(node_multi):
            self.assertEqual(len(nm._oagcache), 0)
            sub_node_nm = nm.subnode
            self.assertEqual(len(nm._oagcache), 1)

    def test_infname_functionality(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            for i in xrange(10):
                setupcur.execute(self.SQL.insert_sample_row, [i, 2])
            self.dbconn.commit()

        # In memory/database retrieved objects are the same
        node_uniq = OAG_UniqNode((2,))
        node_uniq2 =\
            OAG_UniqNode(initprms={
                '_field1' : 3,
                'field2' : 2,
                'field3' : 2
            })
        self.assertEqual(node_uniq.infname,node_uniq2.infname)

        # Changing initparm changes infname for relevant item
        node_uniq3 =\
            OAG_UniqNode(initprms={
                '_field1' : 3,
                'field2' : 3,
                'field3' : 2
            })
        self.assertNotEqual(node_uniq.infname, node_uniq3.infname)

        # Changing in-memory index value doesn't alter infname
        node_uniq4 =\
            OAG_UniqNode(initprms={
                '_field1' : 22,
                'field2' : 2,
                'field3' : 2
            })
        self.assertEqual(node_uniq.infname,node_uniq4.infname)

        # Infname cannot be calculated until cframe is set on multinode
        node_multi = OAG_MultiNode((2,))
        with self.assertRaises(OAError):
            print node_multi.infname

        # Looping through multinode results in different infnames
        hashes =[]
        for x in node_multi:
            hashes.append(x.infname)
        no_dupe_hashes = list(set(hashes))
        self.assertEqual(len(hashes), len(no_dupe_hashes))

        # Infname doesn't change if non-specified field changed
        # when custom infname_field list defined
        node_cust_multi = next(OAG_MultiWithCustomInfnameList((2,)))
        infname1 = node_cust_multi.infname
        node_cust_multi.field3 = 'i am a very fake value'
        infname2 = node_cust_multi.infname
        self.assertEqual(infname1, infname2)

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

class OAG_UniqNode(OAGraphRootNode):
    @property
    def is_unique(self): return True

    @property
    def dbcontext(self): return "test"

    @oagprop
    def subnode(self): return OAG_UniqNode((self.field2,))

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

class OAG_MultiNode(OAGraphRootNode):
    @property
    def is_unique(self): return False

    @property
    def dbcontext(self): return "test"

    @oagprop
    def subnode(self): return OAG_MultiNode((self.field3,))

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

class OAG_MultiWithCustomInfnameList(OAG_MultiNode):
    @property
    def infname_fields(self):
        return [ 'field2' ]
