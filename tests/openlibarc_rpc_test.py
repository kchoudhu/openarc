#!/usr/bin/env python2.7

import unittest
import sys

from datetime                 import datetime
from openlibarc.exception     import *
from openlibarc.rpc           import *
from openlibarc.test          import TestOABase

class TestOARpc(unittest.TestCase, TestOABase):
    initialized = False

    def setUp(self):
        self.setUp_db()
        if self.initialized is False:
            self.nuke_database()
            self.initialized = True

    def tearDown(self):
        self.tearDown_db()

    def test_rpc_registration(self):
        """Status checks, registration and deregistration work as expected"""
        # Lookup
        svc = RpcReqSink(42)
        with self.assertRaises(OAGraphRetrieveError):
            status = svc.status
            print status[0]._rawdata

        # Register
        svc.start()
        self.assertEqual(svc.status[0].role, 'rep')
        self.assertEqual(svc.status[0].owner_id, 42)

        # Deregister
        svc.stop()
        with self.assertRaises(OAGraphRetrieveError):
            status = svc.status

    def test_reqrep(self):
        """Messages can reliably be sent on req->rep->req roundtrip"""

        rep = RpcReqSink(42).start()
        req = RpcReqPump(42).start()

        req_send_string = "[%s] req" % OATime().now
        req._send(req_send_string)
        rep_recv_string = rep._recv()
        rep._send(rep_recv_string)
        req_recv_string = req._recv()

        self.assertEqual(rep_recv_string, req_send_string)
        self.assertEqual(req_send_string, req_recv_string)

    def test_broker_reqrep(self):
        """Messages can reliably sent on req-rep->req roundtrip over broker"""
        brkr = RpcBroker(42).start()
        req  = RpcBrokeredReqPump(42).start()
        rep  = RpcBrokeredReqSink(42).start()

        req_send_string = "[%s] req" % OATime().now
        req._send(req_send_string)
        rep_recv_string = rep._recv()
        rep._send(rep_recv_string)
        req_recv_string = req._recv()

        self.assertEqual(rep_recv_string, req_send_string)
        self.assertEqual(req_send_string, req_recv_string)

    def test_pubsub(self):
        """Messages sent on pub are received by subscribed services"""
        pub  = RpcPub(42).start()
        sub1 = RpcSub(42, 1).start()
        sub2 = RpcSub(42, 2).start()

        pub_send_string_1 = "[%s] pub" % OATime().now
        pub._send(pub_send_string_1)
        sub1_recv_string_1 = sub1._recv()
        sub2_recv_string_1 = sub2._recv()

        self.assertEqual(pub_send_string_1, sub1_recv_string_1)
        self.assertEqual(pub_send_string_1, sub2_recv_string_1)
        self.assertEqual(sub1_recv_string_1, sub2_recv_string_1)

        pub_send_string_2 = "[%s] pub" % OATime().now
        pub._send(pub_send_string_2)
        sub1_recv_string_2 = sub1._recv()
        sub2_recv_string_2 = sub2._recv()

        self.assertEqual(pub_send_string_2, sub1_recv_string_2)
        self.assertEqual(pub_send_string_2, sub2_recv_string_2)
        self.assertEqual(sub1_recv_string_2, sub2_recv_string_2)

    class SQL(TestOABase.SQL):
        pass

class RpcBroker(OARpcBase):
    """Test RPC service"""
    def __init__(self, entity_id):
        self.servicename   =  "lcreqbrk"
        self.roles         = ["dealer", "router"]
        self._owning_class =  "entities"
        self._owner_id     =   entity_id
        super(RpcBroker, self).__init__()

class RpcPub(OARpcBase):
    def __init__(self, entity_id):
        self.servicename   =  "lcpub"
        self.roles         = ["pub"]
        self._owning_class =  "entities"
        self._owner_id     =   entity_id
        super(RpcPub, self).__init__()

class RpcSub(OARpcBase):
    def __init__(self, entity_id, count):
        self.servicename   =  "lcsub_%s" % count
        self.roles         = ["sub"]
        self._owning_class =  "entities"
        self._owner_id     =   entity_id
        self.connects_to   =   OAG_RpcService(("lcpub",
                                                self._owning_class,
                                                self._owner_id,
                                               "pub"), "property")
        super(RpcSub, self).__init__()

class RpcReqPump(OARpcBase):
    def __init__(self, entity_id):
        self.roles         = ["req"]
        self._owning_class =  "entities"
        self._owner_id     =   entity_id
        self.servicename   =  "lcreqpump"
        try:
            self.connects_to  = OAG_RpcService(("lcreqsink",
                                                 self._owning_class,
                                                 self._owner_id,
                                                "rep"), "property")
        except OAGraphRetrieveError:
            self.connects_to  = None
        super(RpcReqPump, self).__init__()

class RpcReqSink(OARpcBase):
    def __init__(self, entity_id):
        self.servicename   =  "lcreqsink"
        self.roles         = ["rep"]
        self._owning_class =  "entities"
        self._owner_id     =   entity_id
        super(RpcReqSink, self).__init__()

class RpcBrokeredReqPump(RpcReqPump):
    def __init__(self, entity_id):
        super(RpcBrokeredReqPump, self).__init__(entity_id)
        self.servicename   =  "lcbrkreqpump"
        self.connects_to   = OAG_RpcService(("lcreqbrk",
                                              self._owning_class,
                                              self._owner_id,
                                             "router"), "property")

class RpcBrokeredReqSink(RpcReqSink):
    def __init__(self, entity_id):
        super(RpcBrokeredReqSink, self).__init__(entity_id)
        self.servicename   =  "lcbrkreqsink"
        self.connects_to   = OAG_RpcService(("lcreqbrk",
                                              self._owning_class,
                                              self._owner_id,
                                             "dealer"), "property")
