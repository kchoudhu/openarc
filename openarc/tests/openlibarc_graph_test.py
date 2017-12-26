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
        pass

    def __check_autonode_equivalence(self, oag1, oag2):
        for oagkey in oag1.dbstreams.keys():
            if oag1.is_oagnode(oagkey):
                self.assertEqual(getattr(oag1, oagkey, "").id, getattr(oag2, oagkey, "").id)
            else:
                self.assertEqual(getattr(oag1, oagkey, ""), getattr(oag2, oagkey, ""))

    def __generate_autonode_system(self):
        logger = OALog()
        #logger.RPC = False
        #logger.Graph = False
        #logger.SQL = False

        a2 =\
            OAG_AutoNode2(logger=logger).create({
                'field4' :  1,
                'field5' : 'this is an autonode2'
            })

        a3 =\
            OAG_AutoNode3(logger=logger).create({
                'field7' : 8,
                'field8' : 'this is an autonode3'
            })

        a1 =\
            OAG_AutoNode1a(logger=logger).create({
                'field2'   : 2,
                'field3'   : 2,
                'subnode1' : a2,
                'subnode2' : a3
            })

        return (a1, a2, a3)

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

    def test_autonode_create_inmemory_with_userprms(self):
        a2 =\
            OAG_AutoNode2(initprms={
                'field4' :  1,
                'field5' : 'this is an autonode2'
            })

        self.assertEqual(a2.field4,  1)
        self.assertEqual(a2.field5, 'this is an autonode2')

        with self.assertRaises(OAGraphRetrieveError):
            OAG_AutoNode2((a2.field4,), "id_2")

    def test_autonode_create_with_null_userprms(self):
        a1 = OAG_AutoNode1a()
        for stream, streaminfo in OAG_AutoNode1a.dbstreams.items():
            self.assertEqual(getattr(a1, stream), None)

    def test_autonode_create_via_create_call(self):
        with self.assertRaises(OAGraphIntegrityError):
            a3 =\
                OAG_AutoNode3().create({
                    'field7' : 8,
                    #'field8' : 'this is an autonode3'
                })

        a2 =\
            OAG_AutoNode2().create({
                'field4' :  1,
                'field5' : 'this is an autonode2'
            })

        a3 =\
            OAG_AutoNode3().create({
                'field7' : 8,
                'field8' : 'this is an autonode3'
            })

        a1 =\
            OAG_AutoNode1a().create({
                'field2'   : 2,
                'field3'   : 2,
                'subnode1' : a2,
                'subnode2' : a3
            })

        # dbstreams are identical
        self.assertEqual(a2.field4, 1)
        self.assertEqual(a2.field5, 'this is an autonode2')
        self.assertEqual(a3.field7, 8)
        self.assertEqual(a3.field8, 'this is an autonode3')
        self.assertEqual(a1.field2, 2)
        self.assertEqual(a1.field3, 2)

        # sub-oags map to the same object as those in initprms
        self.assertEqual(a1.subnode1, a2)
        self.assertEqual(a1.subnode2, a3)

        # reconstituted oag is value-identical (no assertions about memory equality)
        a3_chk = OAG_AutoNode3((a3.id,))
        self.__check_autonode_equivalence(a3, a3_chk)

    def test_autonode_create_nested(self):
        (a1, a2, a3) = self.__generate_autonode_system()
        a1_chk = OAG_AutoNode1a((a1[0].id,))
        self.__check_autonode_equivalence(next(a1), next(a1_chk))

    def test_autonode_create_with_properties(self):
        (a1, a2, a3) = self.__generate_autonode_system()

        a1 = OAG_AutoNode1a()
        for oagkey in a1.dbstreams.keys():
            self.assertEqual(getattr(a1, oagkey, ""), None)

        a1.field2 = 3
        a1.field3 = 3
        a1.subnode1 = a2

        with self.assertRaises(OAGraphIntegrityError):
            a1.create()

        a1.subnode2 = a3
        a1.create()

        self.assertEqual(a1.subnode1, a2)
        self.assertEqual(a1.subnode2, a3)

        next(a1)
        a1_chk = next(OAG_AutoNode1a((a1.id,)))
        self.__check_autonode_equivalence(a1, a1_chk)

    def test_autonode_update_with_userprms(self):
        (a1,   a2,   a3)   = self.__generate_autonode_system()
        (a1_b, a2_b, a3_b) = self.__generate_autonode_system()
        a3.update({
            'field8' : 'this is an updated autonode3'
        })

        a3_chk = OAG_AutoNode3((a3.id,))
        self.assertEqual(a3.field8, a3_chk.field8)

        a1.next().update({
            'subnode1' : a2_b
        })


        self.assertEqual(a1.subnode1, a2_b)

        a1_chk = next(OAG_AutoNode1a((a1.id,)))
        self.__check_autonode_equivalence(a1_chk.subnode1, a2_b)

    def test_autonode_update_with_properties(self):
        (a1,   a2,   a3)   = self.__generate_autonode_system()
        (a1_b, a2_b, a3_b) = self.__generate_autonode_system()
        a3.field8 = 'this is an updated autonode3'
        a3.update()

        a3_chk = OAG_AutoNode3((a3.id,))
        self.assertEqual(a3.field8, a3_chk.field8)

        a1.next().subnode1 = a2_b
        a1.update()

        self.assertEqual(a1.subnode1, a2_b)

        a1_chk =next(OAG_AutoNode1a((a1.id,)))
        self.__check_autonode_equivalence(a1_chk.subnode1, a2_b)

    def test_autonode_fwdoag_creation(self):

        a2 =\
            OAG_AutoNode2().create({
                'field4' :  1,
                'field5' : 'this is an autonode2'
            })


        a3 =\
            OAG_AutoNode3().create({
                'field7' :  8,
                'field8' : 'this is an autonode3'
            })

        for x in xrange(0,10):
            a1a =\
                OAG_AutoNode1a().create({
                    'field2'   : x,
                    'field3'   : 10-x,
                    'subnode1' : a2,
                    'subnode2' : a3
                })
            a1b =\
                OAG_AutoNode1b().create({
                    'field2'   : 10-x,
                    'field3'   : x,
                    'subnode1' : a2,
                    'subnode2' : a3
                })

        with self.assertRaises(AttributeError):
            a2.auto_node1a
        with self.assertRaises(AttributeError):
            a2.auto_node1b
        with self.assertRaises(AttributeError):
            a3.auto_node1a
        with self.assertRaises(AttributeError):
            a3.auto_node1b


        # refresh a2 and a3 to generate fwdoags
        a2.refresh(gotodb=True)
        a3.refresh(gotodb=True)

        self.assertEqual(a2.auto_node1a.size, 10)
        self.assertEqual(a2.auto_node1b.size, 10)
        self.assertEqual(a3.auto_node1a.size, 10)
        self.assertEqual(a3.auto_node1b.size, 10)


    def test_multinode_indexing(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            for i in xrange(10):
                setupcur.execute(self.SQL.insert_sample_row, [i, 2])
            self.dbconn.commit()

        # Can't index unique OAGs
        node_uniq = OAG_UniqNode((2,))
        with self.assertRaises(OAError):
            node_uniq[2]

        node_multi = OAG_MultiNode((2,))

        # Normal indexing works as expected
        for i in xrange(10):
            self.assertEqual(node_multi[i].field2, i)

        # Negative indexing works
        self.assertEqual(node_multi[-1].field2, 9)

        # Out of bounds indexing throws
        with self.assertRaises(IndexError):
            node_multi[294]

        # Vanilla slicing works as expected
        node_multi = node_multi[2:8]
        self.assertEqual(node_multi.size, 6)

        # Stepped slicing works
        node_multi = node_multi[2:8:2]
        self.assertEqual(node_multi.size, 3)

        # Slicing can be "reset"
        node_multi.refresh()
        self.assertEqual(node_multi.size, 10)

    def test_oag_interconnection_with_dbpersist(self):
        logger = OALog()
        #logger.RPC = True
        #logger.Graph = True

        a2 =\
            OAG_AutoNode2(logger=logger).create({
                'field4' :  1,
                'field5' : 'this is an autonode2'
            })

        a3a =\
            OAG_AutoNode3(logger=logger).create({
                'field7' :  8,
                'field8' : 'this is an autonode3'
            })

        a3b =\
            OAG_AutoNode3(logger=logger).create({
                'field7' :  9,
                'field8' : 'this is an autonode3'
            })

        a1a =\
            OAG_AutoNode1a(logger=logger).create({
                'field2'   : 1,
                'field3'   : 2,
                'subnode1' : a2,
                'subnode2' : a3a
            })

        a4 =\
            OAG_AutoNode4(logger=logger).create({
                'subnode1' : a1a[0]
            })

        # Assert initial state
        # Cache state
        self.assertEqual(a4._oagcache['subnode1'], a1a)
        self.assertEqual(a4.subnode1._oagcache['subnode1'], a2)
        self.assertEqual(a4.subnode1._oagcache['subnode2'], a3a)
        self.assertEqual(a4.subnode1.subnode1._oagcache, {})
        self.assertEqual(a4.subnode1.subnode2._oagcache, {})
        # Actual return
        self.assertEqual(a4.subnode1, a1a)
        self.assertEqual(a4.subnode1.subnode1, a2)
        self.assertEqual(a4.subnode1.subnode2, a3a)


        # Change subnode's oag: a4's oagcache should be blown
        a4.subnode1.subnode2 = a3b
        # Cache state
        self.assertEqual(a4._oagcache, {})
        self.assertEqual(a4.subnode1._oagcache['subnode1'], a2)
        self.assertEqual(a4.subnode1._oagcache['subnode2'], a3b)
        self.assertEqual(a4.subnode1.subnode1._oagcache, {})
        self.assertEqual(a4.subnode1.subnode2._oagcache, {})
        # Actual return
        self.assertEqual(a4.subnode1, a1a)
        self.assertEqual(a4.subnode1.subnode1, a2)
        self.assertEqual(a4.subnode1.subnode2, a3b)


        # Change sub-subnode's dbstream
        a4.subnode1.subnode2.field8 = 'this is pretty hot stuff'
        # Cache state
        self.assertEqual(a4._oagcache, {})
        self.assertEqual(a4.subnode1._oagcache['subnode1'], a2)
        with self.assertRaises(KeyError):
            self.assertEqual(a4.subnode1._oagcache['subnode2'], a3b)
        self.assertEqual(a4.subnode1.subnode1._oagcache, {})
        self.assertEqual(a4.subnode1.subnode2._oagcache, {})
        # Actual return
        self.assertEqual(a4.subnode1, a1a)
        self.assertEqual(a4.subnode1.subnode1, a2)
        self.assertEqual(a4.subnode1.subnode2, a3b)

        # Change subnode back
        a4.subnode1.subnode2 = a3a
        # Cache state
        self.assertEqual(a4._oagcache, {})
        self.assertEqual(a4.subnode1._oagcache['subnode1'], a2)
        self.assertEqual(a4.subnode1._oagcache['subnode2'], a3a)
        self.assertEqual(a4.subnode1.subnode1._oagcache, {})
        self.assertEqual(a4.subnode1.subnode2._oagcache, {})
        # Actual return
        self.assertEqual(a4.subnode1, a1a)
        self.assertEqual(a4.subnode1.subnode1, a2)
        self.assertEqual(a4.subnode1.subnode2, a3a)

    def test_oag_interconnection_in_memory(self):
        logger = OALog()
        #logger.RPC = True
        #logger.SQL = True
        #logger.Graph = True

        a2 =\
            OAG_AutoNode2(initprms={
                'field4' :  1,
                'field5' : 'this is an autonode2'
            }, logger=logger)

        a3a =\
            OAG_AutoNode3(initprms={
                'field7' :  8,
                'field8' : 'this is an autonode3'
            }, logger=logger)

        a3b =\
            OAG_AutoNode3(initprms={
                'field7' :  9,
                'field8' : 'this is an autonode3'
            }, logger=logger)

        a1a =\
            OAG_AutoNode1a(initprms={
                'field2'   : 1,
                'field3'   : 2,
                'subnode1' : a2,
                'subnode2' : a3a
            }, logger=logger)

        a4 =\
            OAG_AutoNode4(initprms={
                'subnode1' : a1a
            }, logger=logger)

        # Assert initial state
        # Cache state
        self.assertEqual(a4._oagcache['subnode1'], a1a)
        self.assertEqual(a4.subnode1._oagcache['subnode1'], a2)
        self.assertEqual(a4.subnode1._oagcache['subnode2'], a3a)
        self.assertEqual(a4.subnode1.subnode1._oagcache, {})
        self.assertEqual(a4.subnode1.subnode2._oagcache, {})
        # Actual return
        self.assertEqual(a4.subnode1, a1a)
        self.assertEqual(a4.subnode1.subnode1, a2)
        self.assertEqual(a4.subnode1.subnode2, a3a)


        # Change subnode's oag: a4's oagcache should be blown
        a4.subnode1.subnode2 = a3b
        # Cache state
        self.assertEqual(a4._oagcache, {})
        self.assertEqual(a4.subnode1._oagcache['subnode1'], a2)
        self.assertEqual(a4.subnode1._oagcache['subnode2'], a3b)
        self.assertEqual(a4.subnode1.subnode1._oagcache, {})
        self.assertEqual(a4.subnode1.subnode2._oagcache, {})
        # # Actual return
        self.assertEqual(a4.subnode1, a1a)
        self.assertEqual(a4.subnode1.subnode1, a2)
        self.assertEqual(a4.subnode1.subnode2, a3b)


        # Change sub-subnode's dbstream
        a4.subnode1.subnode2.field8 = 'this is pretty hot stuff'
        # Cache state
        self.assertEqual(a4._oagcache, {})
        self.assertEqual(a4.subnode1._oagcache['subnode1'], a2)
        with self.assertRaises(KeyError):
            self.assertEqual(a4.subnode1._oagcache['subnode2'], a3b)
        self.assertEqual(a4.subnode1.subnode1._oagcache, {})
        self.assertEqual(a4.subnode1.subnode2._oagcache, {})
        # Actual return
        self.assertEqual(a4.subnode1, a1a)
        self.assertEqual(a4.subnode1.subnode1, a2)
        self.assertEqual(a4.subnode1.subnode2, a3b)

        # Change subnode back
        a4.subnode1.subnode2 = a3a
        # Cache state
        self.assertEqual(a4._oagcache, {})
        self.assertEqual(a4.subnode1._oagcache['subnode1'], a2)
        self.assertEqual(a4.subnode1._oagcache['subnode2'], a3a)
        self.assertEqual(a4.subnode1.subnode1._oagcache, {})
        self.assertEqual(a4.subnode1.subnode2._oagcache, {})
        # Actual return
        self.assertEqual(a4.subnode1, a1a)
        self.assertEqual(a4.subnode1.subnode1, a2)
        self.assertEqual(a4.subnode1.subnode2, a3a)

    def test_oag_remote_proxy_simple(self):
        logger = OALog()
        #logger.RPC = True
        #logger.Graph = True

        (a1, a2, a3) = self.__generate_autonode_system()

        next(a1)

        a1_prox = OAG_AutoNode1a(initurl=a1.proxyurl, logger=logger)
        with self.assertRaises(OAError):
            a1_prox.field2 = 32

        self.__check_autonode_equivalence(a1, a1_prox)

    def test_oag_remote_proxy_fwdoag_functionality(self):
        logger = OALog()
        #logger.RPC = True
        #logger.Graph = True

        a2 =\
            OAG_AutoNode2().create({
                'field4' :  1,
                'field5' : 'this is an autonode2'
            })


        a3 =\
            OAG_AutoNode3().create({
                'field7' :  8,
                'field8' : 'this is an autonode3'
            })

        for x in xrange(0,10):
            a1a =\
                OAG_AutoNode1a().create({
                    'field2'   : x,
                    'field3'   : 10-x,
                    'subnode1' : a2,
                    'subnode2' : a3
                })
            a1b =\
                OAG_AutoNode1b().create({
                    'field2'   : 10-x,
                    'field3'   : x,
                    'subnode1' : a2,
                    'subnode2' : a3
                })

        a2_proxy = OAG_AutoNode2(initurl=a2.proxyurl, logger=logger)
        a3_proxy = OAG_AutoNode3(initurl=a3.proxyurl, logger=logger)

        with self.assertRaises(AttributeError):
            a2_proxy.auto_node1a
        with self.assertRaises(AttributeError):
            a2_proxy.auto_node1b
        with self.assertRaises(AttributeError):
            a3_proxy.auto_node1a
        with self.assertRaises(AttributeError):
            a3_proxy.auto_node1b

        # refresh a2 and a3 to generate fwdoags
        a2.refresh(gotodb=True)
        a3.refresh(gotodb=True)

        self.assertEqual(a2_proxy.auto_node1a.size, 10)
        self.assertEqual(a2_proxy.auto_node1b.size, 10)
        self.assertEqual(a3_proxy.auto_node1a.size, 10)
        self.assertEqual(a3_proxy.auto_node1b.size, 10)

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

class OAG_AutoNode1a(OAG_RootNode):
    @property
    def is_unique(self): return False

    @property
    def dbcontext(self): return "test"

    @staticproperty
    def dbstreams(cls): return {
        'field2'   : [ 'int', 0 ],
        'field3'   : [ 'int', 0 ],
        'subnode1' : [ OAG_AutoNode2 ],
        'subnode2' : [ OAG_AutoNode3 ]
    }

class OAG_AutoNode1b(OAG_RootNode):
    @property
    def is_unique(self): return False

    @property
    def dbcontext(self): return "test"

    @staticproperty
    def dbstreams(cls): return {
        'field2'   : [ 'int', 0 ],
        'field3'   : [ 'int', 0 ],
        'subnode1' : [ OAG_AutoNode2 ],
        'subnode2' : [ OAG_AutoNode3 ]
    }

class OAG_AutoNode2(OAG_RootNode):
    @property
    def is_unique(self): return True

    @property
    def dbcontext(self): return "test"

    @staticproperty
    def dbstreams(cls): return {
        'field4'   : [ 'int', 0 ],
        'field5'   : [ 'varchar(50)', 0 ],
    }

    @property
    def sql_local(self):
        return{
          "read" : {
            "id_2" : self.SQLpp("""
                SELECT *
                  FROM {0}.{1}
                 WHERE field4=%s
              ORDER BY {2}""")
          }
        }

class OAG_AutoNode3(OAG_RootNode):
    @property
    def is_unique(self): return True

    @property
    def dbcontext(self): return "test"

    @staticproperty
    def dbstreams(cls): return {
        'field7'   : [ 'int', 0 ],
        'field8'   : [ 'varchar(50)', 0 ],
    }

class OAG_AutoNode4(OAG_RootNode):
    @property
    def is_unique(self): return True

    @property
    def dbcontext(self): return "test"

    @staticproperty
    def dbstreams(cls): return {
        'subnode1' : [ OAG_AutoNode1a ]
    }