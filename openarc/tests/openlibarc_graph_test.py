#!/usr/bin/env python3

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
        for oagkey in oag1.streams.keys():
            if oag1.is_oagnode(oagkey):
                self.assertEqual(getattr(oag1, oagkey, "").id, getattr(oag2, oagkey, "").id)
            else:
                self.assertEqual(getattr(oag1, oagkey, ""), getattr(oag2, oagkey, ""))

    def __generate_autonode_system(self, logger=OALog()):
        a2 =\
            OAG_AutoNode2(logger=logger).db.create({
                'field4' :  1,
                'field5' : 'this is an autonode2'
            })

        a3 =\
            OAG_AutoNode3(logger=logger).db.create({
                'field7' : 8,
                'field8' : 'this is an autonode3'
            })

        a1 =\
            OAG_AutoNode1a(logger=logger).db.create({
                'field2'   : 2,
                'field3'   : 2,
                'subnode1' : a2,
                'subnode2' : a3
            })

        return (a1, a2, a3)

    def test_graph_isolation_control(self, logger=OALog()):
        with self.dbconn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            self.dbconn.commit()

            # Object not created until commit
            oa = OAG_AutoNode8(extcur=setupcur, logger=logger).db.create({
                'field3' : 2,
                'field4' : 2,
                'field5' : 'infname multinode test',
            })

            with self.assertRaises(OAGraphRetrieveError):
                oa_chk = OAG_AutoNode8(2, 'by_f4_idx', logger=logger)

            self.dbconn.commit()
            oa_chk = OAG_AutoNode8(2, 'by_f4_idx', logger=logger)
            self.__check_autonode_equivalence(oa[0], oa_chk[0])

            # Object not updated until commit
            oa.field4 = 33
            oa.db.update()
            # -> no change yet, retrieval by previous index succeeds
            oa_chk = OAG_AutoNode8(2, 'by_f4_idx', logger=logger)[0]
            self.assertEqual(oa_chk.field4, 2)
            self.dbconn.commit()
            # -> update commited, retrieval fails
            with self.assertRaises(OAGraphRetrieveError):
                oa_chk = OAG_AutoNode8(2, 'by_f4_idx', logger=logger)
            # -> but retrieval by new value succeeds
            oa_chk = OAG_AutoNode8(33, 'by_f4_idx', logger=logger)[0]
            self.__check_autonode_equivalence(oa, oa_chk)

    def test_data_retrieval_failure(self):
        """Attempt to retrieve no data fails with exception"""
        with self.assertRaises(OAGraphRetrieveError):
            oa = OAG_AutoNode2(1)

    def test_uniquenode_data_integrity(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.create_sample_table)
            for i in range(2):
                setupcur.execute(self.SQL.insert_sample_row, [2, 2])
            self.dbconn.commit()

        with self.assertRaises(Exception):
            oa = OAG_UniqNode((2,))

    def test_multinode_refresh_behavior(self):

        for i in range(10):
            OAG_AutoNode8().db.create({
                'field3' : i,
                'field4' : 2,
                'field5' : 'infname multinode test',
            })

        # Exhausted MultiNode is implicitly refreshed
        node_multi = OAG_AutoNode8(2, 'by_f4_idx')

        for oa in node_multi:
            continue
        accumulator = [ oa.field3 for oa in node_multi ]
        self.assertEqual(len(accumulator), 10)

        # Explicitly refreshed MultiNode allows full iteration again
        for i, oa in enumerate(node_multi):
            if i == 5:
                break
        node_multi.reset()
        accumulator_refreshed = [ oa.field3 for oa in node_multi ]
        self.assertEqual(len(accumulator_refreshed), 10)

    def test_oanode_size_reporting(self):
        for i in range(10):
            OAG_AutoNode8().db.create({
                'field3' : i,
                'field4' : 2,
                'field5' : 'infname multinode test',
            })

        node_multi = OAG_AutoNode8(2, 'by_f4_idx')

        self.assertEqual(node_multi.size, 10)

    def test_oanode_oagprop_caching(self, logger=OALog()):
        """Assigned OAG can be retrieved without new object being generated"""

        (a1, a2, a3) = self.__generate_autonode_system(logger)

        a1_dupe = OAG_AutoNode1a(a1[0].id, logger=logger)

        # No oagprops have been access, so cache should be empty
        self.assertEqual(len(a1_dupe[0].cache.state), 0)

        # Having accessed subnode1, it should now be stored in cache
        self.__check_autonode_equivalence(a1_dupe.subnode1, a2)
        self.assertEqual(len(a1_dupe.cache.state), 1)

    def test_uniqnode_infname_functionality(self):
        for i in range(10):
            OAG_AutoNode2().db.create({
                'field4' : i,
                'field5' : 'infname_test'
            })

        node_uniq = OAG_AutoNode2(3)


        # In memory and database items have same infnames
        node_uniq_in_mem1 =\
            OAG_AutoNode2(initprms={
                # databases are 1 indexed (who knew?)
                'field4' : 2,
                'field5' : 'infname_test'
            })
        self.assertEqual(node_uniq.infname, node_uniq_in_mem1.infname)

        # Changing initparm changes infname
        node_uniq_in_mem2=\
            OAG_AutoNode2(initprms={
                'field4' : 3,
                'field5' : 'infname_test'
            })
        self.assertNotEqual(node_uniq.infname, node_uniq_in_mem2.infname)

        # Changing field via property setting changes infname
        node_uniq_in_mem3 =\
            OAG_AutoNode2(initprms={
                'field4' : 2,
                'field5' : 'infname_test'
            })
        self.assertEqual(node_uniq.infname, node_uniq_in_mem3.infname)
        node_uniq_in_mem3.field4 = 33
        self.assertNotEqual(node_uniq.infname, node_uniq_in_mem3.infname)

        # Changing non-infname field doesn't change infname
        node_uniq_in_mem4 =\
            OAG_AutoNode2(initprms={
                'field4' : 2,
                'field5' : 'infname_test'
            })
        self.assertEqual(node_uniq.infname, node_uniq_in_mem4.infname)
        node_uniq_in_mem4.field5 = 'infname_morph_test'
        self.assertEqual(node_uniq.infname, node_uniq_in_mem4.infname)

    def test_multinode_infname_functionality(self):

        for i in range(10):
            OAG_AutoNode8().db.create({
                'field3' : i,
                'field4' : 2,
                'field5' : 'infname multinode test',
            })

        # Retrieve unintiialized multinode
        node_multi = OAG_AutoNode8(2)

        # Infname cannot be calculated until cframe is set on multinode
        with self.assertRaises(OAError):
            print( node_multi.infname)

        # Looping through multinode results in different infnames
        node_multi_idx1 = OAG_AutoNode8(2, 'by_f4_idx')
        hashes = [x.infname for x in node_multi_idx1]
        no_dupe_hashes = list(set(hashes))
        self.assertEqual(len(hashes), 10)
        self.assertEqual(len(hashes), len(no_dupe_hashes))

        # Changing non-infname field doesn't change infname
        node_multi_idx2 = OAG_AutoNode8(2, 'by_f4_idx')
        for nmi in node_multi_idx2:
            infname = nmi.infname
            nmi.field5 = 'infname_morph_test'
            self.assertEqual(infname, nmi.infname)

    def test_autonode_retrieval_styles(self, logger=OALog()):
        """Graph retrieval succeeds with no tuple"""
        (a1, a2, a3) = self.__generate_autonode_system(logger)

        # Retrieve with subnode
        a1_chk_sn = OAG_AutoNode1a(a1[0])
        self.__check_autonode_equivalence(a1[0], a1_chk_sn[0])

        # Retrieve with list
        a1_chk_list = OAG_AutoNode1a((a1[0],))
        self.__check_autonode_equivalence(a1[0], a1_chk_list[0])

        # Retreive with str
        a1_chk_str = OAG_AutoNode1a(a1[0].id)
        self.__check_autonode_equivalence(a1[0], a1_chk_str[0])

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
        for stream, streaminfo in OAG_AutoNode1a.streams.items():
            self.assertEqual(getattr(a1, stream), None)

    def test_autonode_create_via_create_call(self):
        with self.assertRaises(OAGraphIntegrityError):
            a3 =\
                OAG_AutoNode3().db.create({
                    'field7' : 8,
                    #'field8' : 'this is an autonode3'
                })

        a2 =\
            OAG_AutoNode2().db.create({
                'field4' :  1,
                'field5' : 'this is an autonode2'
            })

        a3 =\
            OAG_AutoNode3().db.create({
                'field7' : 8,
                'field8' : 'this is an autonode3'
            })

        a1 =\
            OAG_AutoNode1a().db.create({
                'field2'   : 2,
                'field3'   : 2,
                'subnode1' : a2,
                'subnode2' : a3
            })

        # streams are identical
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

        a1a = OAG_AutoNode1a()
        for oagkey in a1a.streams.keys():
            self.assertEqual(getattr(a1a, oagkey), None)

        # non-oag instream marked None is missing, should not throw
        a1a.field2   = 3
        ### a1a.field3 is marked None, should not throw
        a1a.subnode1 = a2
        a1a.subnode2 = a3

        a1a.db.create().next()

        a1a_chk = OAG_AutoNode1a((a1a.id,))
        self.__check_autonode_equivalence(a1a, next(a1a_chk))

        # oag dbstream missing, should throw
        a1b = OAG_AutoNode1a()

        a1b.field2 = 3
        a1b.field3 = 34
        ### a1b.subnode1 is missing
        a1b.subnode2 = a3

        with self.assertRaises(OAGraphIntegrityError):
            a1b.db.create()

        # non-oag instream with default value is missing, should throw
        a1c = OAG_AutoNode1a()

        ###a1c.field2 = 3
        a1c.field3 = 34
        a1c.subnode1 = a2
        a1c.subnode2 = a3

        with self.assertRaises(OAGraphIntegrityError):
            a1c.db.create()

    def test_autonode_update_with_userprms(self):
        (a1,   a2,   a3)   = self.__generate_autonode_system()
        (a1_b, a2_b, a3_b) = self.__generate_autonode_system()
        a3.db.update({
            'field8' : 'this is an updated autonode3'
        })

        a3_chk = OAG_AutoNode3((a3.id,))
        self.assertEqual(a3.field8, a3_chk.field8)

        a1.next().db.update({
            'subnode1' : a2_b
        })


        self.assertEqual(a1.subnode1, a2_b)

        a1_chk = next(OAG_AutoNode1a((a1.id,)))
        self.__check_autonode_equivalence(a1_chk.subnode1, a2_b)

    def test_autonode_update_with_properties(self):
        (a1,   a2,   a3)   = self.__generate_autonode_system()
        (a1_b, a2_b, a3_b) = self.__generate_autonode_system()
        a3.field8 = 'this is an updated autonode3'
        a3.db.update()

        a3_chk = OAG_AutoNode3((a3.id,))
        self.assertEqual(a3.field8, a3_chk.field8)

        a1.next().subnode1 = a2_b
        a1.db.update()

        self.assertEqual(a1.subnode1, a2_b)

        a1_chk =next(OAG_AutoNode1a((a1.id,)))
        self.__check_autonode_equivalence(a1_chk.subnode1, a2_b)

    def test_autonode_fwdoag_creation(self, logger=OALog()):

        a2 =\
            OAG_AutoNode2(logger=logger).db.create({
                'field4' :  1,
                'field5' : 'this is an autonode2'
            })

        a3 =\
            OAG_AutoNode3(logger=logger).db.create({
                'field7' :  8,
                'field8' : 'this is an autonode3'
            })

        for x in range(0,10):
            a1a =\
                OAG_AutoNode1a(logger=logger).db.create({
                    'field2'   : x,
                    'field3'   : 10-x,
                    'subnode1' : a2,
                    'subnode2' : a3
                })
            a1b =\
                OAG_AutoNode1b().db.create({
                    'field2'   : 10-x,
                    'field3'   : x,
                    'subnode1' : a2,
                    'subnode2' : a3
                })

        self.assertEqual(a2.auto_node1a.size, 10)
        self.assertEqual(a2.auto_node1b.size, 10)
        self.assertEqual(a3.auto_node1a.size, 10)
        self.assertEqual(a3.auto_node1b.size, 10)

    def test_oag_interconnection_with_dbpersist(self):
        logger = OALog()
        #logger.RPC = True
        #logger.Graph = True

        a2 =\
            OAG_AutoNode2(logger=logger).db.create({
                'field4' :  1,
                'field5' : 'this is an autonode2'
            })

        a3a =\
            OAG_AutoNode3(logger=logger).db.create({
                'field7' :  8,
                'field8' : 'this is an autonode3'
            })

        a3b =\
            OAG_AutoNode3(logger=logger).db.create({
                'field7' :  9,
                'field8' : 'this is an autonode3'
            })

        a1a =\
            OAG_AutoNode1a(logger=logger).db.create({
                'field2'   : 1,
                'field3'   : 2,
                'subnode1' : a2,
                'subnode2' : a3a
            })

        a4 =\
            OAG_AutoNode4(logger=logger).db.create({
                'subnode1' : a1a[0]
            })

        # Assert initial state
        # Cache state
        self.assertEqual(a4.cache.state['subnode1'], a1a)
        self.assertEqual(a4.subnode1.cache.state['subnode1'], a2)
        self.assertEqual(a4.subnode1.cache.state['subnode2'], a3a)
        self.assertEqual(a4.subnode1.subnode1.cache.state, {})
        self.assertEqual(a4.subnode1.subnode2.cache.state, {})
        # Actual return
        self.assertEqual(a4.subnode1, a1a)
        self.assertEqual(a4.subnode1.subnode1, a2)
        self.assertEqual(a4.subnode1.subnode2, a3a)

        # Change subnode's oag: a4's oagcache should be blown
        a4.subnode1.subnode2 = a3b
        # Cache state
        self.assertEqual(a4.cache.state, {})
        self.assertEqual(a4.subnode1.cache.state['subnode1'], a2)
        self.assertEqual(a4.subnode1.cache.state['subnode2'], a3b)
        self.assertEqual(a4.subnode1.subnode1.cache.state, {})
        self.assertEqual(a4.subnode1.subnode2.cache.state, {})
        # Actual return
        self.assertEqual(a4.subnode1, a1a)
        self.assertEqual(a4.subnode1.subnode1, a2)
        self.assertEqual(a4.subnode1.subnode2, a3b)

        # Change sub-subnode's dbstream
        a4.subnode1.subnode2.field8 = 'this is pretty hot stuff'
        # Assert cache state by level
        self.assertEqual(a4.cache.state, {})
        self.assertEqual(a4.subnode1.cache.state['subnode1'], a2)
        with self.assertRaises(KeyError):
            self.assertEqual(a4.subnode1.cache.state['subnode2'], a3b)

        # Change second sub-subnode's dbstream
        a4.subnode1.subnode1.field4 = 'this is pretty weird'
        # Assert Cache state
        self.assertEqual(a4.cache.state, {})
        with self.assertRaises(KeyError):
            self.assertEqual(a4.subnode1.cache.state['subnode1'], a2)
        with self.assertRaises(KeyError):
            self.assertEqual(a4.subnode1.cache.state['subnode2'], a3b)
        self.assertEqual(a4.subnode1.subnode1.cache.state, {})
        self.assertEqual(a4.subnode1.subnode2.cache.state, {})

        # Actual return for system
        self.assertEqual(a4.subnode1, a1a)
        self.assertEqual(a4.subnode1.subnode1, a2)
        self.assertEqual(a4.subnode1.subnode2, a3b)

        # Change subnode back
        a4.subnode1.subnode2 = a3a
        # Cache state
        self.assertEqual(a4.cache.state, {})
        self.assertEqual(a4.subnode1.cache.state['subnode1'], a2)
        self.assertEqual(a4.subnode1.cache.state['subnode2'], a3a)
        self.assertEqual(a4.subnode1.subnode1.cache.state, {})
        self.assertEqual(a4.subnode1.subnode2.cache.state, {})
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
        self.assertEqual(a4.cache.state['subnode1'], a1a)
        self.assertEqual(a4.subnode1.cache.state['subnode1'], a2)
        self.assertEqual(a4.subnode1.cache.state['subnode2'], a3a)
        self.assertEqual(a4.subnode1.subnode1.cache.state, {})
        self.assertEqual(a4.subnode1.subnode2.cache.state, {})
        # Actual return
        self.assertEqual(a4.subnode1, a1a)
        self.assertEqual(a4.subnode1.subnode1, a2)
        self.assertEqual(a4.subnode1.subnode2, a3a)

        # Change subnode's oag: a4's oagcache should be blown
        a4.subnode1.subnode2 = a3b
        # Cache state
        self.assertEqual(a4.cache.state, {})
        self.assertEqual(a4.subnode1.cache.state['subnode1'], a2)
        self.assertEqual(a4.subnode1.cache.state['subnode2'], a3b)
        self.assertEqual(a4.subnode1.subnode1.cache.state, {})
        self.assertEqual(a4.subnode1.subnode2.cache.state, {})
        # Actual return
        self.assertEqual(a4.subnode1, a1a)
        self.assertEqual(a4.subnode1.subnode1, a2)
        self.assertEqual(a4.subnode1.subnode2, a3b)

        # Change sub-subnode's dbstream
        a4.subnode1.subnode2.field8 = 'this is pretty hot stuff'
        # Assert cache state by level
        self.assertEqual(a4.cache.state, {})
        self.assertEqual(a4.subnode1.cache.state['subnode1'], a2)
        with self.assertRaises(KeyError):
            self.assertEqual(a4.subnode1.cache.state['subnode2'], a3b)
        # Change second sub-subnode's dbstream
        a4.subnode1.subnode1.field4 = 'this is pretty weird'
        # Assert Cache state
        self.assertEqual(a4.cache.state, {})
        with self.assertRaises(KeyError):
            self.assertEqual(a4.subnode1.cache.state['subnode1'], a2)
        with self.assertRaises(KeyError):
            self.assertEqual(a4.subnode1.cache.state['subnode2'], a3b)
        self.assertEqual(a4.subnode1.subnode1.cache.state, {})
        self.assertEqual(a4.subnode1.subnode2.cache.state, {})

        # Actual return for system
        self.assertEqual(a4.subnode1, a1a)
        self.assertEqual(a4.subnode1.subnode1, a2)
        self.assertEqual(a4.subnode1.subnode2, a3b)

        # Change subnode back
        a4.subnode1.subnode2 = a3a
        # Cache state
        self.assertEqual(a4.cache.state, {})
        self.assertEqual(a4.subnode1.cache.state['subnode1'], a2)
        self.assertEqual(a4.subnode1.cache.state['subnode2'], a3a)
        self.assertEqual(a4.subnode1.subnode1.cache.state, {})
        self.assertEqual(a4.subnode1.subnode2.cache.state, {})
        # Actual return
        self.assertEqual(a4.subnode1, a1a)
        self.assertEqual(a4.subnode1.subnode1, a2)
        self.assertEqual(a4.subnode1.subnode2, a3a)

    def test_oag_remote_proxy_simple(self, logger=OALog()):

        (a1, a2, a3) = self.__generate_autonode_system(logger=logger)

        next(a1)



        a1_prox = OAG_AutoNode1a(initurl=a1.oagurl, logger=logger)

        with self.assertRaises(OAError):
            a1_prox.field2 = 32

        self.__check_autonode_equivalence(a1, a1_prox)

    def test_oag_remote_proxy_fwdoag_functionality(self, logger=OALog()):

        a2 =\
            OAG_AutoNode2(logger=logger).db.create({
                'field4' :  1,
                'field5' : 'this is an autonode2'
            })


        a3 =\
            OAG_AutoNode3(logger=logger).db.create({
                'field7' :  8,
                'field8' : 'this is an autonode3'
            })

        for x in range(0,10):
            a1a =\
                OAG_AutoNode1a(logger=logger).db.create({
                    'field2'   : x,
                    'field3'   : 10-x,
                    'subnode1' : a2,
                    'subnode2' : a3
                })
            a1b =\
                OAG_AutoNode1b(logger=logger).db.create({
                    'field2'   : 10-x,
                    'field3'   : x,
                    'subnode1' : a2,
                    'subnode2' : a3
                })

        a2_proxy = OAG_AutoNode2(initurl=a2.oagurl, logger=logger)
        a3_proxy = OAG_AutoNode3(initurl=a3.oagurl, logger=logger)

        self.assertEqual(a2_proxy.auto_node1a.size, 10)
        self.assertEqual(a2_proxy.auto_node1b.size, 10)
        self.assertEqual(a3_proxy.auto_node1a.size, 10)
        self.assertEqual(a3_proxy.auto_node1b.size, 10)

    def test_oag_remote_proxy_invalidation_with_dbpersist(self, logger=OALog()):

        a2 =\
            OAG_AutoNode2(logger=logger).db.create({
                'field4' :  1,
                'field5' : 'this is an autonode2'
            })

        a3a =\
            OAG_AutoNode3(logger=logger).db.create({
                'field7' :  8,
                'field8' : 'this is an autonode3'
            })

        a3b =\
            OAG_AutoNode3(logger=logger).db.create({
                'field7' :  9,
                'field8' : 'this is an autonode3'
            })

        a1a =\
            OAG_AutoNode1a(logger=logger).db.create({
                'field2'   : 1,
                'field3'   : 2,
                'subnode1' : a2,
                'subnode2' : a3a
            }).next()

        a1a_proxy = OAG_AutoNode1a(initurl=a1a.oagurl, logger=logger)

        a4 =\
            OAG_AutoNode4(logger=logger).db.create({
                'subnode1' : a1a_proxy
            })

        # Assert initial state
        # Cache state
        self.assertEqual(a4.cache.state['subnode1'], a1a_proxy)
        self.assertEqual(a1a_proxy.cache.state, {})
        self.assertEqual(a1a_proxy.rpc.proxied_url, a1a.oagurl)
        self.assertEqual(a1a.cache.state['subnode1'], a2)
        self.assertEqual(a1a.cache.state['subnode2'], a3a)
        self.assertEqual(a1a.subnode1.cache.state, {})
        self.assertEqual(a1a.subnode2.cache.state, {})
        # Actual return
        self.assertEqual(a4.subnode1, a1a_proxy)
        self.assertEqual(a4.subnode1.subnode1.oagurl, a2.oagurl)
        self.assertEqual(a4.subnode1.subnode2.oagurl, a3a.oagurl)

        # Change subnode's oag: a4's oagcache should be blown
        #logger.RPC = True


        with self.assertRaises(OAError):
            a4.subnode1.subnode2 = a3b
        a1a.subnode2 = a3b
        # Cache state

        self.assertEqual(a4.cache.state, {})
        self.assertEqual(a1a_proxy.cache.state, {})
        self.assertEqual(a1a.cache.state['subnode1'], a2)
        self.assertEqual(a1a.cache.state['subnode2'], a3b)
        self.assertEqual(a1a.subnode1.cache.state, {})
        self.assertEqual(a1a.subnode2.cache.state, {})
        # Actual return
        self.assertEqual(a4.subnode1, a1a_proxy)
        self.assertEqual(a4.subnode1.subnode1.oagurl, a2.oagurl)
        self.assertEqual(a4.subnode1.subnode2.oagurl, a3b.oagurl)

        # Change sub-subnode's dbstream
        with self.assertRaises(OAError):
            a4.subnode1.subnode2.field8 = 'this is pretty hot stuff'
        a1a.subnode2.field8 = 'this is pretty hot stuff'
        # Cache state
        self.assertEqual(a4.cache.state, {})
        self.assertEqual(a1a.cache.state['subnode1'], a2)
        with self.assertRaises(KeyError):
            self.assertEqual(a1a.cache.state['subnode2'], a3b)
        with self.assertRaises(OAError):
            a4.subnode1.subnode1.field4 = 'this is pretty weird'
        a1a.subnode1.field4 = 'this is pretty weird'
        # Assert Cache state
        self.assertEqual(a4.cache.state, {})
        with self.assertRaises(KeyError):
            self.assertEqual(a1a.subnode1.cache.state['subnode1'], a2)
        with self.assertRaises(KeyError):
            self.assertEqual(a1a.subnode1.cache.state['subnode2'], a3b)
        self.assertEqual(a1a.subnode1.cache.state, {})
        self.assertEqual(a1a.subnode2.cache.state, {})
        # Actual return
        self.assertEqual(a4.subnode1, a1a_proxy)
        self.assertEqual(a4.subnode1.subnode1.oagurl, a2.oagurl)
        self.assertEqual(a4.subnode1.subnode2.oagurl, a3b.oagurl)

        # Change subnode back
        with self.assertRaises(OAError):
            a4.subnode1.subnode2 = a3a
        a1a.subnode2 = a3a
        # Cache state
        self.assertEqual(a4.cache.state, {})
        self.assertEqual(a1a.cache.state['subnode1'], a2)
        self.assertEqual(a1a.cache.state['subnode2'], a3a)
        self.assertEqual(a1a.subnode1.cache.state, {})
        self.assertEqual(a1a.subnode2.cache.state, {})
        # Actual return
        self.assertEqual(a4.subnode1, a1a_proxy)
        self.assertEqual(a4.subnode1.subnode1.oagurl, a2.oagurl)
        self.assertEqual(a4.subnode1.subnode2.oagurl, a3a.oagurl)

    def test_oag_remote_proxy_invalidation_in_memory(self):
        logger = OALog()
        #logger.RPC = True
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

        a1a_proxy = OAG_AutoNode1a(initurl=a1a.oagurl, logger=logger)

        a4 =\
            OAG_AutoNode4(initprms={
                'subnode1' : a1a_proxy
            }, logger=logger)

        # Assert initial state
        # Cache state
        self.assertEqual(a4.cache.state['subnode1'], a1a_proxy)
        self.assertEqual(a1a_proxy.cache.state, {})
        self.assertEqual(a1a_proxy.rpc.proxied_url, a1a.oagurl)
        self.assertEqual(a1a.cache.state['subnode1'], a2)
        self.assertEqual(a1a.cache.state['subnode2'], a3a)
        self.assertEqual(a1a.subnode1.cache.state, {})
        self.assertEqual(a1a.subnode2.cache.state, {})
        # Actual return
        self.assertEqual(a4.subnode1, a1a_proxy)
        self.assertEqual(a4.subnode1.subnode1.oagurl, a2.oagurl)
        self.assertEqual(a4.subnode1.subnode2.oagurl, a3a.oagurl)

        # Change subnode's oag: a4's oagcache should be blown
        with self.assertRaises(OAError):
            a4.subnode1.subnode2 = a3b
        a1a.subnode2 = a3b
        # Cache state
        self.assertEqual(a4.cache.state, {})
        self.assertEqual(a1a_proxy.cache.state, {})
        self.assertEqual(a1a.cache.state['subnode1'], a2)
        self.assertEqual(a1a.cache.state['subnode2'], a3b)
        self.assertEqual(a1a.subnode1.cache.state, {})
        self.assertEqual(a1a.subnode2.cache.state, {})
        # Actual return
        self.assertEqual(a4.subnode1, a1a_proxy)
        self.assertEqual(a4.subnode1.subnode1.oagurl, a2.oagurl)
        self.assertEqual(a4.subnode1.subnode2.oagurl, a3b.oagurl)

        # Change sub-subnode's dbstream
        with self.assertRaises(OAError):
            a4.subnode1.subnode2.field8 = 'this is pretty hot stuff'
        a1a.subnode2.field8 = 'this is pretty hot stuff'
        # Cache state
        self.assertEqual(a4.cache.state, {})
        self.assertEqual(a1a.cache.state['subnode1'], a2)
        with self.assertRaises(KeyError):
            self.assertEqual(a1a.cache.state['subnode2'], a3b)
        with self.assertRaises(OAError):
            a4.subnode1.subnode1.field4 = 'this is pretty weird'
        a1a.subnode1.field4 = 'this is pretty weird'
        # Assert Cache state
        self.assertEqual(a4.cache.state, {})
        with self.assertRaises(KeyError):
            self.assertEqual(a1a.subnode1.cache.state['subnode1'], a2)
        with self.assertRaises(KeyError):
            self.assertEqual(a1a.subnode1.cache.state['subnode2'], a3b)
        self.assertEqual(a1a.subnode1.cache.state, {})
        self.assertEqual(a1a.subnode2.cache.state, {})
        # Actual return
        self.assertEqual(a4.subnode1, a1a_proxy)
        self.assertEqual(a4.subnode1.subnode1.oagurl, a2.oagurl)
        self.assertEqual(a4.subnode1.subnode2.oagurl, a3b.oagurl)

        # Change subnode back
        with self.assertRaises(OAError):
            a4.subnode1.subnode2 = a3a
        a1a.subnode2 = a3a
        # Cache state
        self.assertEqual(a4.cache.state, {})
        self.assertEqual(a1a.cache.state['subnode1'], a2)
        self.assertEqual(a1a.cache.state['subnode2'], a3a)
        self.assertEqual(a1a.subnode1.cache.state, {})
        self.assertEqual(a1a.subnode2.cache.state, {})
        # Actual return
        self.assertEqual(a4.subnode1, a1a_proxy)
        self.assertEqual(a4.subnode1.subnode1.oagurl, a2.oagurl)
        self.assertEqual(a4.subnode1.subnode2.oagurl, a3a.oagurl)

    def test_uniquenode_deletion(self, logger=OALog()):

        a2 = OAG_AutoNode2(logger=logger).db.create({
                'field4' : 3847,
                'field5' : 'this is an autonode2'
             })

        a2_id = a2.id

        a2_chk = OAG_AutoNode2((a2_id,), logger=logger)

        self.__check_autonode_equivalence(a2, a2_chk)

        a2.db.delete()

        self.assertEqual(a2.id, None)
        self.assertEqual(a2.field4, None)
        self.assertEqual(a2.field5, None)

        with self.assertRaises(OAGraphRetrieveError):
            oa_chk = OAG_AutoNode2((a2_id,), logger=logger)

    def test_multinode_deletion(self, logger=OALog()):
        a2 =\
            OAG_AutoNode2(logger=logger).db.create({
                'field4' :  1,
                'field5' : 'this is an autonode2'
            })

        a3 =\
            OAG_AutoNode3(logger=logger).db.create({
                'field7' :  8,
                'field8' : 'this is an autonode3'
            })

        for x in range(0,10):
            a1a =\
                OAG_AutoNode1a(logger=logger).db.create({
                    'field2'   : x,
                    'field3'   : 10-x,
                    'subnode1' : a2,
                    'subnode2' : a3
                })

        self.assertEqual(a2.auto_node1a.size, 10)

        a2.auto_node1a[0].db.delete()

        a2.reset()

        self.assertEqual(a2.auto_node1a.size, 9)

        for x in a2.auto_node1a:
            a2.auto_node1a.db.delete()

        self.assertEqual(a2.auto_node1a.size, 0)

    def test_alternative_index_lookup(self, logger=OALog()):

        a2 =\
            OAG_AutoNode2().db.create({
                'field4' :  1,
                'field5' : 'this is an autonode2'
            })

        a3 =\
            OAG_AutoNode3().db.create({
                'field7' :  8,
                'field8' : 'this is an autonode3'
            })

        for x in range(0,10):
            a1a =\
                OAG_AutoNode1a().db.create({
                    'field2'   : x,
                    'field3'   : 10-x,
                    'subnode1' : a2,
                    'subnode2' : a3
                })

        # Lookup by dict - out of order
        a1a_chk_1 =\
            OAG_AutoNode1a({
                'field3' : 9,
                'field2' : 1,
            }, 'by_a3_idx', logger=logger)
        self.assertEqual(a1a_chk_1.size, 1)

        # Lookup by dict - in order
        a1a_chk_2 =\
            OAG_AutoNode1a({
                'field2' : 1,
                'field3' : 9,
            }, 'by_a3_idx', logger=logger)
        self.__check_autonode_equivalence(a1a_chk_1[0], a1a_chk_2[0])

        # Lookup by list
        a1a_chk_3 =\
            OAG_AutoNode1a([1, 9], 'by_a3_idx', logger=logger)
        self.__check_autonode_equivalence(a1a_chk_1[0], a1a_chk_3[0])

    def test_rpc_discovery(self, logger=OALog()):

        a2 =\
            OAG_AutoNode2(logger=logger)\
            .db.create({
                'field4' :  1,
                'field5' : 'this is an autonode2'
            })

        self.assertEqual(a2.rpc.discoverable, False)

        with a2:
            a2_remote =\
                OAG_AutoNode2(initprms={
                    'field4' :  1,
                    'field5' : 'this is an autonode2'
                }, logger=logger).rpc.discover()

            self.__check_autonode_equivalence(a2, a2_remote)

        self.assertEqual(a2.rpc.discoverable, False)

        with self.assertRaises(OAGraphRetrieveError):
            OAG_RpcDiscoverable({
                'rpcinfname' : a2.infname
            }, 'by_rpcinfname_idx', rpc=False)

    @unittest.skip("long running time")
    def test_rpc_discovery_cleanup(self, logger=OALog()):

        a2 =\
            OAG_AutoNode2(logger=logger, heartbeat=False)\
            .db.create({
                'field4' :  1,
                'field5' : 'this is an autonode2'
            })

        with a2:
            a2_dupe =\
                OAG_AutoNode2(logger=logger, heartbeat=False)\
                .db.create({
                    'field4' :  1,
                    'field5' : 'this is an autonode2'
                })

            # Attempt to immediately make duplicate oag discoverable
            with self.assertRaises(OAError):
                a2_dupe.rpc.discoverable = True

            # Wait 5 seconds, and then try to make duplicate discoverable
            time.sleep(getenv().rpctimeout)
            a2_dupe.rpc.discoverable = True
            rpcdisc =\
                OAG_RpcDiscoverable({
                    'rpcinfname' : a2_dupe.infname
                }, 'by_rpcinfname_idx', rpc=False)

            # ...Previous rpcdisc has been evicted, new one available
            self.assertNotEqual(rpcdisc[0].id, a2.rpc._rpc_discovery[0].id)

            a2_dupe.rpc.discoverable = False

        # Context manager works as expected
        with self.assertRaises(OAGraphRetrieveError):
            OAG_RpcDiscoverable({
                'rpcinfname' : a2.infname
            }, 'by_rpcinfname_idx', rpc=False)

    @unittest.skip("long running time")
    def test_rpc_discovery_underlying_env_change(self, logger=OALog()):
        def test_func():
            import gevent

            a2 =\
                OAG_AutoNode2(logger=logger).db.create({
                    'field4' :  1,
                    'field5' : 'this is an autonode2'
                })

            with a2:
                i = 0;
                while i < 3:
                    if i == 2:
                        with self.dbconn.cursor() as setupcur:
                            setupcur.execute("update openarc.rpc_discoverable set envid='%s'" % 'this_is_a_fake_id')
                            self.dbconn.commit()
                    gevent.sleep(getenv().rpctimeout)
                    i += 1

        with self.assertRaises(SystemExit):
            glets = [gevent.spawn(test_func)]
            gevent.joinall(glets)
            gevent.sleep(5)

    @unittest.skip("long running time")
    def test_rpc_discovery_underlying_db_row_removal(self, logger=OALog()):
        self._envid  = base64.b16encode(os.urandom(16))
        def test_func():
            import gevent

            logger =OALog()

            a2 =\
                OAG_AutoNode2(logger=logger).db.create({
                    'field4' :  1,
                    'field5' : 'this is an autonode2'
                })

            with a2:
                i = 0;
                while i < 3:
                    if i == 2:
                        with self.dbconn.cursor() as setupcur:
                            setupcur.execute("delete from openarc.rpc_discoverable where 1=1")
                            self.dbconn.commit()
                    gevent.sleep(getenv().rpctimeout)
                    i += 1

        with self.assertRaises(SystemExit):
            glets = [gevent.spawn(test_func)]
            gevent.joinall(glets)
            gevent.sleep(5)

    def test_nondb_oagprop_behavior(self):
        logger = OALog()
        #logger.RPC = True
        #logger.Graph = True
        #logger.SQL = True

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

        # deriv prop is calculated cleanly
        self.assertEqual(a4.cacheable_deriv_prop, 10)
        # cache state
        self.assertEqual(a4.cache.state['subnode1'], a1a)
        self.assertEqual(a4.cache.state['cacheable_deriv_prop'], 10)

        a3a.field7 = 99

        with self.assertRaises(KeyError):
            a4.cache.state['subnode1']
        with self.assertRaises(KeyError):
            a4.cache.state['cacheable_deriv_prop']

        self.assertEqual(a4.cacheable_deriv_prop, 101)
        self.assertEqual(a4.cache.state['subnode1'], a1a)
        self.assertEqual(a4.cache.state['cacheable_deriv_prop'], 101)

    def test_invalidation_oag_handler(self):
        logger = OALog()
        #logger.RPC = True
        #logger.Graph = True
        #logger.SQL = True

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

        a3a.field7 = 22

        self.assertEqual(a4.invcount, 1)

        a3a.field8 = 'this is an updated autonode3'

        self.assertEqual(a4.invcount, 2)

    def test_multinode_indexing_on_update(self, logger=OALog()):
        a2 =\
            OAG_AutoNode2(initprms={
                'field4' :  1,
                'field5' : 'this is an autonode2'
            }, logger=logger).db.create()

        a3a =\
            OAG_AutoNode3(initprms={
                'field7' :  8,
                'field8' : 'this is an autonode3'
            }, logger=logger).db.create()

        a3b =\
            OAG_AutoNode3(initprms={
                'field7' :  8,
                'field8' : 'this is an autonode3 beta'
            }, logger=logger).db.create()


        for i in range(10):
            a1a =\
                OAG_AutoNode1a(logger=logger).db.create({
                    'field2'   : 1,
                    'field3'   : i,
                    'subnode1' : a2,
                    'subnode2' : a3a
                })

        a1a_chk = OAG_AutoNode1a(1, 'by_a2_idx')[5]

        self.assertEqual(a1a_chk.field3, 5)

        x = a1a_chk[5]
        x.field3 = 43
        x.db.update()

        self.assertEqual(a1a_chk.field3, 43)

        a1a_chk2 = OAG_AutoNode1a(1, 'by_a2_idx')[5]

        self.assertEqual(a1a_chk2.field3, 43)

        y = a1a_chk2[5]

        y.db.update({
            'field3'   : 96,
            'subnode2' : a3b
        })

        self.assertEqual(y.field3, 96)
        self.assertEqual(y.subnode2, a3b)

        a1a_chk3 = OAG_AutoNode1a(1, 'by_a2_idx')[5]

        self.assertEqual(a1a_chk3.field3, 96)
        self.__check_autonode_equivalence(a3b, a1a_chk3.subnode2)

    def test_autonode_uniq_null_subnode(self, logger=OALog()):
        a2 =\
            OAG_AutoNode2(initprms={
                'field4' :  1,
                'field5' : 'this is an autonode2'
            }, logger=logger).db.create()

        a3 =\
            OAG_AutoNode3(initprms={
                'field7' :  8,
                'field8' : 'this is an autonode3'
            }, logger=logger).db.create()

        # ok
        a5a =\
            OAG_AutoNode5(initprms={
                'subnode1' : a2,
                'subnode2' : a3
            }, logger=logger).db.create()

        self.__check_autonode_equivalence(a5a.subnode1, a2)
        self.__check_autonode_equivalence(a5a.subnode2, a3)

        # ok
        a5b =\
            OAG_AutoNode5(initprms={
                'subnode1' : a2,
            }, logger=logger).db.create()

        self.__check_autonode_equivalence(a5b.subnode1, a2)
        self.assertEqual(a5b.subnode2, None)
        a5b_chk = OAG_AutoNode5(a5b.id)
        self.assertEqual(a5b_chk.subnode2, None)

        a5c =\
            OAG_AutoNode5(initprms={
                'subnode1' : a2,
                'subnode2' : None
            }, logger=logger).db.create()

        self.__check_autonode_equivalence(a5c.subnode1, a2)
        self.assertEqual(a5c.subnode2, None)
        a5c_chk = OAG_AutoNode5(a5c.id)
        self.assertEqual(a5c_chk.subnode2, None)

        a5d =\
            OAG_AutoNode5(initprms={
                'subnode1' : a2,
                'subnode2' : None
            }, logger=logger).db.create()

        a5d.db.update({
            'subnode2' : a3
        })

        a5d_chk = OAG_AutoNode5(a5d.id)

        self.__check_autonode_equivalence(a5d_chk.subnode2, a3)

    def test_autonode_uniq_null_subnode(self, logger=OALog()):
        a2 =\
            OAG_AutoNode2(initprms={
                'field4' :  1,
                'field5' : 'this is an autonode2'
            }, logger=logger).db.create()

        a3 =\
            OAG_AutoNode3(initprms={
                'field7' :  8,
                'field8' : 'this is an autonode3'
            }, logger=logger).db.create()

        a6a =\
            OAG_AutoNode6(initprms={
                'subnode1' : a2,
                'subnode2' : None
            }, logger=logger).db.create()

        self.assertEqual(a6a[-1].subnode2, None)

        a6a.db.update({
            'subnode2' : a3
        })

        self.assertEqual(a6a[-1].subnode2, a3)

        a6a_chk = OAG_AutoNode6(a6a.id)[-1]
        self.__check_autonode_equivalence(a6a_chk.subnode1, a2)
        self.__check_autonode_equivalence(a6a_chk.subnode2, a3)

    def test_autonode_boolean_handling(self, logger=OALog()):
        a7 =\
            OAG_AutoNode7(initprms={
                'field1' : True
            }, logger=logger).db.create()

        self.assertEqual(a7.field1, True)

        a7.db.update({
            'field1' : False
        })

        self.assertEqual(a7.field1, False)

        a7_chk = OAG_AutoNode7(a7.id)
        self.assertEqual(a7_chk.field1, a7.field1)

        a7_null =\
            OAG_AutoNode7(initprms={}, logger=logger).db.create()

        self.assertEqual(a7_null.field1, None)

    def test_autonode_cloning(self, logger=OALog()):
        a2 =\
            OAG_AutoNode2(initprms={
                'field4' :  1,
                'field5' : 'this is an autonode2'
            }, logger=logger).db.create()

        a3a =\
            OAG_AutoNode3(initprms={
                'field7' :  8,
                'field8' : 'this is an autonode3'
            }, logger=logger).db.create()

        a1a =\
            OAG_AutoNode1a(initprms={
                'field2'   : 1,
                'field3'   : 2,
                'subnode1' : a2,
                'subnode2' : a3a
            }, logger=logger).db.create()

        a1a_copy = a1a.clone()

        self.__check_autonode_equivalence(a1a_copy[0], a1a[0])

    def test_multinode_iteration_behavior_cache(self, logger=OALog()):

        a2 =\
            OAG_AutoNode2(logger=logger).db.create({
                'field4' :  1,
                'field5' : 'this is an autonode2'
            })

        a3 =\
            OAG_AutoNode3(logger=logger).db.create({
                'field7' : 8,
                'field8' : 'this is an autonode3'
            })

        for i in range(10):

            a1 =\
                OAG_AutoNode1a(logger=logger).db.create({
                    'field2'   : 2,
                    'field3'   : i,
                    'subnode1' : a2,
                    'subnode2' : a3
                })

        node_multi = OAG_AutoNode1a(2, 'by_a2_idx', logger=logger)

        for i, nm in enumerate(node_multi):
            # Each iteration clears the cache
            self.assertEqual(len(nm.cache.state), 0)

            # Accessing a subnode adds one item to the cache
            self.__check_autonode_equivalence(nm.subnode1, a2)
            self.assertEqual(len(nm.cache.state), 1)

            # Accessing second subnode adds another item to the cache
            self.__check_autonode_equivalence(nm.subnode2, a3)
            self.assertEqual(len(nm.cache.state), 2)

    def test_multinode_iteration_behavior_properties(self, logger=OALog()):

        # AutoNodes we are creating
        a1s = []

        a2 =\
            OAG_AutoNode2(logger=logger).db.create({
                'field4' :  1,
                'field5' : 'this is an autonode2'
            })

        a3 =\
            OAG_AutoNode3(logger=logger).db.create({
                'field7' : 8,
                'field8' : 'this is an autonode3'
            })

        for i in range(10):
            a1s.append(\
                OAG_AutoNode1a(logger=logger).db.create({
                    'field2'   : 2,
                    'field3'   : i,
                    'subnode1' : a2,
                    'subnode2' : a3
                })
            )

            # The objects returned by the subnodes are equivalent to what was
            # initially assigned
            self.assertEqual(a1s[i][0].subnode1, a2)
            self.assertEqual(a1s[i][0].subnode2, a3)
            self.assertEqual(a1s[i].subnode1, a2)
            self.assertEqual(a1s[i].subnode2, a3)

        for a1 in a1s:
            self.assertTrue(a1.oagid in OAG_AutoNode1a.oagprofiles.keys())
            self.assertEqual(a1[0].subnode1, a2)
            self.assertEqual(a1[0].subnode2, a3)

        a1_chk1 = OAG_AutoNode1a(2, 'by_a2_idx')
        a1_chk2 = OAG_AutoNode1a(2, 'by_a2_idx')

        self.assertEqual(a1_chk1.size, a1_chk2.size)

        for i in range(a1_chk1.size):
            self.__check_autonode_equivalence(a1_chk1[i], a1_chk2[i])
            self.__check_autonode_equivalence(a1_chk1, a1_chk2)
            self.__check_autonode_equivalence(a1_chk1[i], a1_chk2[i])

        for i, nm in enumerate(a1_chk1):
            self.assertEqual(a1_chk1.field2, 2)
            self.assertEqual(a1_chk1.field3, i)
            self.__check_autonode_equivalence(a1_chk2[2].subnode1, nm.subnode1)
            self.__check_autonode_equivalence(nm.subnode1, a2)
            self.__check_autonode_equivalence(nm.subnode2, a3)

    def test_autonode_naming_reversibility(self, logger=OALog()):
        with self.assertRaises(OAError):
            OAG_AUTONodeNonReversible().db.create()

    def test_autonode_with_many_subnodes_of_same_type(self, logger=OALog()):
        a2 =\
            OAG_AutoNode2(logger=logger).db.create({
                'field4' :  1,
                'field5' : 'this is an autonode2'
            })

        a3a =\
            OAG_AutoNode3(logger=logger).db.create({
                'field7' : 8,
                'field8' : 'this is an autonode3a'
            })

        a3b =\
            OAG_AutoNode3(logger=logger).db.create({
                'field7' : 8,
                'field8' : 'this is an autonode3b'
            })

        a10 =\
            OAG_AutoNode10(logger=logger).db.create({
                'subnode1' : a2,
                'subnode2' : a3a,
                'subnode3' : a3b
            })

        self.__check_autonode_equivalence(a10.subnode1, a2)
        self.__check_autonode_equivalence(a10.subnode2, a3a)
        self.__check_autonode_equivalence(a10.subnode3, a3b)

        a10.subnode3 = a3a

        self.__check_autonode_equivalence(a10.subnode3, a3a)

        a10.db.update()

        a10_chk = OAG_AutoNode10(a10.id)

        self.__check_autonode_equivalence(a10, a10_chk)
        self.__check_autonode_equivalence(a10_chk.subnode3, a3a)
        self.__check_autonode_equivalence(a10_chk.subnode2, a3a)
        self.__check_autonode_equivalence(a10_chk.subnode1, a2)

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

class OAG_AutoNode1a(OAG_RootNode):
    @staticproperty
    def context(cls): return "test"

    @staticproperty
    def dbindices(cls) : return {
        'a2_idx' : [['field2'],             False, None],
        'a3_idx' : [['field2', 'field3'],   False, None],
        'a5_idx' : [['field3', 'subnode2'], False, {'subnode2' : 1}],
    }

    @staticproperty
    def streams(cls): return {
        'field2'   : [ 'int',         0,    None ],
        'field3'   : [ 'int',         None, None ],
        'subnode1' : [ OAG_AutoNode2, True, None ],
        'subnode2' : [ OAG_AutoNode3, True, None ],
    }

class OAG_AutoNode1b(OAG_RootNode):
    @staticproperty
    def context(cls): return "test"

    @staticproperty
    def streams(cls): return {
        'field2'   : [ 'int',         0,    None ],
        'field3'   : [ 'int',         0,    None ],
        'subnode1' : [ OAG_AutoNode2, True, None ],
        'subnode2' : [ OAG_AutoNode3, True, None ],
    }

class OAG_AutoNode2(OAG_RootNode):
    @staticproperty
    def is_unique(self): return True

    @staticproperty
    def context(cls): return "test"

    @staticproperty
    def streams(cls): return {
        'field4'   : [ 'int',         0, None ],
        'field5'   : [ 'varchar(50)', 0, None ],
    }

    @staticproperty
    def infname_fields(cls): return [ 'field4' ]

    @property
    def dblocalsql(self):
        return{
          "read" : {
            "id_2" : self.db.SQLpp("""
                SELECT *
                  FROM {0}.{1}
                 WHERE field4=%s
              ORDER BY {2}""")
          }
        }

class OAG_AutoNode3(OAG_RootNode):
    @staticproperty
    def is_unique(cls): return True

    @staticproperty
    def context(cls): return "test"

    @staticproperty
    def streams(cls): return {
        'field7'   : [ 'int',         0, None ],
        'field8'   : [ 'varchar(50)', 0, None ],
    }

class OAG_AutoNode4(OAG_RootNode):
    @staticproperty
    def is_unique(cls): return True

    @staticproperty
    def context(cls): return "test"

    @staticproperty
    def streams(cls): return {
        'subnode1' : [ OAG_AutoNode1a, True, 'ev_test_handler' ],
    }

    @oagprop
    def cacheable_deriv_prop(self):
        return self.subnode1.field2 + self.subnode1.subnode1.field4 + self.subnode1.subnode2.field7

    invcount = 0
    def ev_test_handler(self):
        self.invcount += 1

class OAG_AutoNode5(OAG_RootNode):
    @staticproperty
    def is_unique(cls): return True

    @staticproperty
    def context(cls): return "test"

    @staticproperty
    def streams(cls): return {
        'subnode1' : [ OAG_AutoNode2, True,  None ],
        'subnode2' : [ OAG_AutoNode3, False, None ]
    }

class OAG_AutoNode6(OAG_RootNode):
    @staticproperty
    def context(cls): return "test"

    @staticproperty
    def streams(cls): return {
        'subnode1' : [ OAG_AutoNode2, True,  None ],
        'subnode2' : [ OAG_AutoNode3, False, None ]
    }

class OAG_AutoNode7(OAG_RootNode):
    @staticproperty
    def is_unique(cls): return True

    @property
    def context(cls): return "test"

    @staticproperty
    def streams(cls): return {
        'subnode1' : [  OAG_AutoNode3, False,  None ],
        'field1'   : [ 'boolean',      None,   None ],
    }

class OAG_AutoNode8(OAG_RootNode):
    @staticproperty
    def context(cls): return "test"

    @staticproperty
    def streams(cls): return {
        'field3'   : [ 'int',         0, None ],
        'field4'   : [ 'int',         0, None ],
        'field5'   : [ 'varchar(50)', 0, None ],
    }

    @staticproperty
    def dbindices(cls) : return {
        'f4_idx' : [['field4'], False,   None ],
    }

    @staticproperty
    def infname_fields(cls): return [ 'field3', 'field4' ]

class OAG_AutoNode10(OAG_RootNode):
    @staticproperty
    def is_unique(cls): return True

    @staticproperty
    def context(cls): return "test"

    @staticproperty
    def streams(cls): return {
        'subnode1' : [ OAG_AutoNode2, True,  None ],
        'subnode2' : [ OAG_AutoNode3, True, None ],
        'subnode3' : [ OAG_AutoNode3, True, None ]
    }

class OAG_AUTONodeNonReversible(OAG_RootNode):
    @staticproperty
    def context(cls): return "test"

    @staticproperty
    def streams(cls): return {
        'field7'   : [ 'int',         0, None ],
        'field8'   : [ 'varchar(50)', 0, None ],
    }
