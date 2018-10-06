#!/usr/bin/env python3

import zmq.green as zmq
from zmq.utils.garbage import gc
_zmqctx = zmq.Context()
_zmqctx.max_sockets = 32768
gc.context = _zmqctx

import base64
import datetime
import gevent
import msgpack
import os
import secrets
import socket
import types
import weakref

from ._util            import oagprop

from openarc.exception import *
from openarc.oatime    import *
from openarc.env       import *

class OARpc(object):

    @property
    def addr(self): return "tcp://%s:%s" % (self.runhost, self.port)

    @property
    def port(self): return self._ctxsoc.LAST_ENDPOINT.decode().split(":")[-1]

    @property
    def runhost(self): return socket.gethostname()

    @staticmethod
    def rpcprocfn(fn):
        def wrapfn(self, *args, **kwargs):
            ret = {
                'status'  : 'OK',
                'conv_id' : args[0]['conv_id'],
                'message' : None,
                'payload' : {},
            }

            try:
                # Quick cleanup
                self._routing_table = {k:v for k,v in self._routing_table.items() if v() is not None}
                if gctx().logger.RPC:
                    print("rtr: %d items in routing table" % len(self._routing_table))

                # Find OAG request relates
                oag = self._routing_table[args[0]['to']]()

                # Check ACL
                acl_policy = oag._rpc_proxy._rpc_acl_policy
                if acl_policy == ACL.LOCAL_ALL:
                    if args[0]['authtoken'] != getenv().envid:
                        raise OAError("Client unauthorized")
                elif acl_policy == ACL.REMOTE_ALL:
                    pass


                fn(self, oag, ret, args[0]['args'])
            except KeyError as ke:
                ret['status'] = 'DEAD'
                ret['message'] = "%s is not present on this host" % args[0]['to']
            except OAError as e:
                ret['status'] = 'FAIL'
                ret['message'] = e.message

            return ret

        return wrapfn

    @staticmethod
    def rpcfn(fn):
        def wrapfn(self, target, *args, gc_run=True, **kwargs):

            # Let's take a moment to process all garbage collected items
            if gc_run:
                try:
                    while True:
                        (removee, notifyee, stream) = gctx().rm_queue.get_nowait()
                        reqcls(self._oag).deregister(notifyee, removee, stream, gc_run=False)
                except gevent.queue.Empty:
                    # Nothing to be GC'd right now
                    pass

            # Back to business
            if isinstance(target, OARpc):
                addr = target.addr
            else:
                addr = target

            (protocol, tcpaddr, oagbang) = [c for c in addr.split('/') if len(c)>0]

            to = protocol+'//'+tcpaddr

            if gctx().logger.TRANSPORT:
                print('Connecting %s' % to)
            self._ctxsoc.connect(to)

            payload = fn(self, args, kwargs)
            payload['action'] = fn.__name__

            # Let router know who this refers to
            payload['to'] = oagbang

            # This should eventually derive from the Auth mgmt object
            # used to initialize the OARpc
            payload['authtoken'] = getenv().envid

            # An identifier for this conversation
            payload['conv_id'] = base64.b16encode(os.urandom(5)).decode('utf-8')

            if self._oag.logger.RPC:
                print("========>")
                if addr==target:
                    toaddr = addr
                else:
                    toaddr = target.id
                print("[%s:req:%s] Sending RPC request with payload [%s] to [%s]" % (payload['conv_id'], self._oag.rpc.id, payload, toaddr))

            self._ctxsoc.send(msgpack.dumps(payload))
            reply = self._ctxsoc.recv()

            if gctx().logger.TRANSPORT:
                print('Disconnecting from %s' % to)
            self._ctxsoc.close()

            rpcret = msgpack.loads(reply, raw=False)

            if self._oag.logger.RPC:
                print("[%s:req:%s] Received reply [%s]" % (rpcret['conv_id'], self._oag.rpc.id, rpcret))
                print("<======== ")

            if rpcret['status'] == 'OK':
                return rpcret
            if rpcret['status'] == 'DEAD':
                self._oag.rpc.registration_invalidate(self._oag.url)
                return rpcret
            if rpcret['status'] == 'FAIL':
                raise OAError("[%s:req] Failed with status [%s] and message [%s]" % (self.id,
                                                                                     rpcret['status'],
                                                                                     rpcret['message']))

            ### This should NEVER happen
            raise OAError("This should never be triggered")

        return wrapfn

class OARpc_RTR_Requests(OARpc):
    """Process all RPC calls from other OARpc_REQ_Request"""
    cxncount = 0

    def __init__(self):

        self._ctxsoc  = _zmqctx.socket(zmq.ROUTER)
        self._routing_table = {}
        self.procglet = None

    def __repr__(self):
        return "<%s on %s>" % (self.__class__.__name__, self.addr)

    def _recv(self):

        self.__class__.cxncount += 1
        sender  = self._ctxsoc.recv()
        empty   = self._ctxsoc.recv()
        payload = msgpack.loads(self._ctxsoc.recv(), raw=False)

        if gctx().logger.TRANSPORT:
            print("=======>")
            print('rtrrecv [conns]  : ', self.__class__.cxncount)
            print('rtrrecv [sender] : ', sender)
            print('rtrrecv [payload]: ', payload)

        return (sender, payload)

    def _send(self, sender, payload):

        if gctx().logger.TRANSPORT:
            print('rtrsend [sender] : ', sender)
            print('rtrsend [payload]: ', payload)
            print("<=======")

        self._ctxsoc.send(sender, zmq.SNDMORE)
        self._ctxsoc.send(str().encode('utf-8'), zmq.SNDMORE)
        self._ctxsoc.send(msgpack.dumps(payload))

    @OARpc.rpcprocfn
    def proc_deregister(self, oag, ret, args):
        oag.rpc.registration_invalidate(args['deregister_addr'])

    @OARpc.rpcprocfn
    def proc_getstream(self, oag, ret, args):
        from .graph import OAG_RootNode
        attr = getattr(oag, args['stream'], None)
        if isinstance(attr, OAG_RootNode):
            ret['payload']['type']  = 'redirect'
            ret['payload']['value'] = attr.rpc.url
            ret['payload']['redir_id'] = attr.rpc.id
            ret['payload']['class'] = attr.__class__.__name__
        else:
            ret['payload']['type']  = 'value'
            ret['payload']['value'] = attr

    @OARpc.rpcprocfn
    def proc_invalidate(self, oag, ret, args):

        invstream = args['stream']

        if oag.logger.RPC:
            print('[%s:rtr] invalidation signal received' % (ret['conv_id']))

        oag.cache.invalidate(invstream)

        # Inform upstream
        for addr, stream in oag.rpc.registrations.items():
            OARpc_REQ_Request(oag).invalidate(addr, stream)

        # Execute any event handlers
        try:
            if invstream in oag.streams.keys():
                evhdlr = oag.streams[invstream][2]
                if evhdlr:
                    getattr(oag, evhdlr, None)()
        except KeyError as e:
            pass

    @OARpc.rpcprocfn
    def proc_register(self, oag, ret, args):
        oag.rpc.registration_add(args['addr'], args['stream'])

    @OARpc.rpcprocfn
    def proc_register_proxy(self, oag, ret, args):
        oag.rpc.registration_add(args['addr'], args['stream'])

        rawprops = list(oag.streams.keys())\
                   + [p for p in dir(oag.__class__) if isinstance(getattr(oag.__class__, p), property)]\
                   + [p for p in dir(oag.__class__) if isinstance(getattr(oag.__class__, p), oagprop)]\
                   + list(oag.props._oagprops.keys())

        ret['payload'] = [p for p in list(set(rawprops)) if p not in oag.rpc.stoplist]

    @OARpc.rpcprocfn
    def proc_update_broadcast(self, oag, ret, args):
        if oag.logger.RPC:
            print('[%s:rtr:%s] update broadcast signal received from %s' % (oag.rpc.id, ret['conv_id'], args['addr']))
        oag.db.search()

        # Tell upstream
        if oag.logger.RPC:
            print('[%s:rtr:%s] sending updates to %s' % (oag.rpc.id, ret['conv_id'], oag.rpc.registrations))
        for addr, stream in oag.rpc.registrations.items():
            OARpc_REQ_Request(oag).invalidate(addr, stream)

    def register_oag(self, oagbang, oag):

        self._routing_table[oagbang] = weakref.ref(oag)

        # To do: prune routing table
        return True

    @property
    def ctxsoc(self):
        return self._ctxsoc

    def start(self):

        def rpcproc(sender, payload):
            rpcret = {
                'deregister'       : self.proc_deregister,
                'getstream'        : self.proc_getstream,
                'invalidate'       : self.proc_invalidate,
                'register'         : self.proc_register,
                'register_proxy'   : self.proc_register_proxy,
                'update_broadcast' : self.proc_update_broadcast,
            }[payload['action']](payload)

            self._send(sender, rpcret)

        # Prep gevent
        from gevent import monkey
        monkey.patch_all()

        # Bind to incoming port
        self._ctxsoc.bind("tcp://*:0")

        if gctx().logger.RPC:
            print("[rtr] Listening for RPC requests")

        while True:
            (sender, payload) = self._recv()

            if gctx().logger.RPC:
                print("[%s:rtr] Received message [%s]" % (payload['conv_id'], payload))

            gevent.spawn(rpcproc, sender, payload)


class OARpc_REQ_Request(OARpc):
    """Make RPC calls to another node's OARpc_RTR_Requests"""
    def __init__(self, oag):
        self._ctxsoc = _zmqctx.socket(zmq.REQ)
        self._oag = weakref.ref(oag)

    @OARpc.rpcfn
    def deregister(self, *args, **kwargs):
        return  {
            'args'      : {
                'deregister_addr' : args[0][0],
                'stream' : args[0][1],
                'addr'   : self._oag.rpc.url
            }
        }

    @OARpc.rpcfn
    def getstream(self, *args, **kwargs):
        return {
            'args'      : {
                'stream' : args[0][0]
            }
        }

    @OARpc.rpcfn
    def invalidate(self, *args, **kwargs):
        return {
            'args'      : {
                'stream' : args[0][0]
            }
        }

    @OARpc.rpcfn
    def register(self, *args, **kwargs):
        return {
            'args'      : {
                'stream' : args[0][0],
                'addr'   : self._oag.rpc.url
            }
        }

    @OARpc.rpcfn
    def register_proxy(self, *args, **kwargs):
        return {
            'args'      : {
                'stream' : args[0][0],
                'addr'   : self._oag.rpc.url
            }
        }

    @OARpc.rpcfn
    def update_broadcast(self, *args, **kwargs):
        return {
            'args' : {
                'addr'   : self._oag.rpc.url
            }
        }

    def __getattribute__(self, attrname):
        attr = object.__getattribute__(self, attrname)
        return attr() if type(attr)==weakref.ref else attr

reqcls = OARpc_REQ_Request

class RpcTransaction(object):
    def __init__(self, rpc_proxy):
        self.is_active = False
        self.notify_upstream = False
        self._rpc_proxy = rpc_proxy

    def __enter__(self):
        self.is_active = True

    def __exit__(self, type, value, traceback):
        if not self.is_active:
            # can this really ever happen?
            return

        if self.notify_upstream:
            for addr, stream_to_invalidate in self._rpc_proxy._rpcreqs.items():
                reqcls(self.rpc._oag).invalidate(addr, stream_to_invalidate)

        self.is_active = False

class RpcProxy(object):
    """Manipulates rpc functionality for OAG"""
    def __init__(self,
                 oag,
                 initurl=None,
                 rpc_enabled=True,
                 rpc_acl_policy=ACL.LOCAL_ALL,
                 rpc_dbupdate_listen=False,
                 rpc_discovery_timeout=0,
                 heartbeat_enabled=True):

        ### Store reference to OAG
        self._oag = weakref.ref(oag)

        ### Spin up rpc infrastructure

        # A unique identifier for this OAG's rpc infra
        self._rpc_id = base64.b16encode(os.urandom(5)).decode('utf-8')

        # Is RPC initialization complete?
        self._rpc_init_done = False

        # A very basic question...
        self._rpc_enabled = rpc_enabled

        # Who's allowed to access this node?
        self._rpc_acl_policy = rpc_acl_policy

        # Registrations received from other OAGs
        self._rpcreqs = {}

        # Holding spot for RPC discoverability - default off
        self._rpc_discovery = None

        # How long to keep OAG discoverable. Default 0 = forever
        self._rpc_discovery_timeout = rpc_discovery_timeout

        # Listen for dbupdates elsewhere
        self._rpc_dbupdate_listen = rpc_dbupdate_listen

        # Should you heartbeat?
        self._rpc_heartbeat = heartbeat_enabled

        # Invalidation cache
        self._rpc_transaction = RpcTransaction(self)

        # Stoplist of OAG streams that shouldn't be exposed over RPC
        self._rpc_stop_list = [
            'cache',
            'db',
            'discoverable',
            'logger',
            'props',
            'rdf',
            'rpc',
        ] + [attr for attr in dir(self._oag) if attr[0]=='_']

        ### Set up OAG proxying infrastructure

        # Are we proxying for another OAG?
        self._proxy_mode = False

        # OAG URL this one is proxying
        self._proxy_url = initurl

        # List of props we are making RPC calls for
        self._proxy_oags = []

        ### Carry out ininitialization

        # Is this OAG RPC enabled? If no, don't proceed
        if not self._rpc_enabled:
            return

        # Proxying
        if self._proxy_url:
            self._proxy_mode = True

        # Register OAG with router
        self._rpc_init_done = gctx().rpcrtr.register_oag(self._rpc_id, self._oag)

    def __getattribute__(self, attrname):
        attr = object.__getattribute__(self, attrname)
        return attr() if type(attr)==weakref.ref else attr

    def clone(self, src):

        pass

    def discover(self):
        from .graph import OAG_RpcDiscoverable
        try:
            remote_oag =\
                OAG_RpcDiscoverable({
                    'rpcinfname' : self._oag.infname_semantic
                }, 'by_rpcinfname_idx', rpc=False)
        except OAGraphRetrieveError:
            raise OADiscoveryError("Nothing to discover yet")

        if not remote_oag[0].is_valid:
            raise OADiscoveryError("Stale discoverable detected")

        return self._oag.__class__(initurl=remote_oag[0].url, rpc_acl=self._rpc_acl_policy)

    @property
    def discoverable(self): return self._rpc_discovery is not None

    @discoverable.setter
    def discoverable(self, value):

        if self._rpc_discovery==value:
            return

        from .graph import OAG_RpcDiscoverable

        if value is False:
            kill_count = gctx().kill_glet(self, 'heartbeat')
            if self._oag.logger.RPC:
                print("[%s] Killing [%d] heartbeat greenlets" % (self.id, kill_count))
            kill_count = gctx().kill_glet(self, 'discovery')
            if self._oag.logger.RPC:
                print("[%s] Killing [%d] discovery greenlets" % (self.id, kill_count))
            self._rpc_discovery.db.delete()
            self._rpc_discovery = None
        else:
            # Cleanup previous messes
            try:

                number_active = 0
                prevrpcs = OAG_RpcDiscoverable(self._oag.infname_semantic, 'by_rpcinfname_idx', rpc=False, heartbeat=False)
                for rpc in prevrpcs:
                    if rpc.is_valid:
                        number_active += 1
                    else:
                        if self._oag.logger.RPC:
                            print("[%s] Removing stale discoverable [%s]-[%d], last HA at [%s]"
                                   % (self.id, rpc.type, rpc.stripe, rpc.heartbeat))
                        rpc.db.delete()

                # Is there already an active subscription there?
                if number_active > 0:
                    if not self.fanout:
                        message = "[%s] Active OAG already on inferred name [%s], last HA at [%s]"\
                                   % (self.id, rpc.rpcinfname, rpc.heartbeat)
                        if self._oag.logger.RPC:
                            print(message)
                        raise OAError(message)
                    else:
                        raise OAError("Fanout not implemented yet")
            except OAGraphRetrieveError as e:
                pass

            # Create new database entry
            self._rpc_discovery =\
                OAG_RpcDiscoverable(rpc=False,
                                    heartbeat=self._rpc_heartbeat)\
                .db.create({
                    'rpcinfname' : self._oag.infname_semantic,
                    'stripe'     : 0,
                    'url'        : self._oag.rpc.url,
                    'type'       : self._oag.__class__.__name__,
                    'envid'      : getenv().envid,
                    'heartbeat'  : OATime().now,
                    'listen'     : self._rpc_dbupdate_listen,
                }).next()

            # Spin off other threads as necessary
            self.start_heartbeat()
            self.start_discovery_timeout()

    @property
    def fanout(self): return False

    @property
    def id(self): return self._rpc_id

    @property
    def is_enabled(self):
        return self._rpc_enabled

    @property
    def is_init(self):

        return getattr(self, '_rpc_init_done', False)

    @property
    def is_heartbeat(self):

        return self._rpc_heartbeat

    @property
    def is_timedout(self):

        return False if self._rpc_discovery_timeout is 0 else True

    @property
    def is_proxy(self):

        return self._proxy_mode

    @property
    def proxied_url(self):

        return self._proxy_url

    @property
    def proxied_streams(self):

        return self._proxy_oags

    @proxied_streams.setter
    def proxied_streams(self, oag_array):

        self._proxy_oags = oag_array

    @property
    def registrations(self):
        return self._rpcreqs

    def registration_add(self, registering_oag_addr, registering_stream):
        self._rpcreqs[registering_oag_addr] = registering_stream
        gctx().put_ka(self._oag)

    def registration_invalidate(self, deregistering_oag_addr):
        self._rpcreqs = {rpcreq:self._rpcreqs[rpcreq] for rpcreq in self._rpcreqs if rpcreq != deregistering_oag_addr}
        gctx().rm_ka(self._oag)

    def start_discovery_timeout(self):
        if self.is_timedout:
            if self._oag.logger.RPC:
                print("[%s] Starting timeout greenlet at [%s]" % (self.id, datetime.datetime.now().isoformat()))
            gctx().put_glet(self, gevent.spawn(self.__cb_discovery_timeout), glet_type='discovery')

    def start_heartbeat(self):
        if self.is_heartbeat:
            if self._oag.logger.RPC:
                print("[%s] Starting heartbeat greenlet at [%s]" % (self.id, datetime.datetime.now().isoformat()))
            gctx().put_glet(self, gevent.spawn(self.__cb_heartbeat), glet_type='heartbeat')

    @property
    def stoplist(self):
        return self._rpc_stop_list

    @property
    def transaction(self):
        return self._rpc_transaction

    @property
    def url(self):
        return '%s/%s' % (gctx().rpcrtr.addr, self.id)

    def __cb_heartbeat(self):
        while True:
            # Did our underlying db control row evaporate? If so, holy shit.
            try:
                from .graph import OAG_RpcDiscoverable
                rpcdisc = OAG_RpcDiscoverable(self._rpc_discovery.id, rpc=False)[0]
            except OAGraphRetrieveError as e:
                if self._oag.logger.RPC:
                    print("[%s] Underlying db controller row is missing for [%s]-[%d], exiting"
                            % (self.id, self._rpc_discovery.rpcinfname, self._rpc_discovery.stripe))
                sys.exit(1)

            # Did environment change?
            if self._rpc_discovery.envid != rpcdisc.envid:
                if self._oag.logger.RPC:
                    print("[%s] Environment changed from [%s] to [%s], exiting"
                            % (self.id, self._rpc_discovery.envid, rpcdisc.envid))
                sys.exit(1)

            self._rpc_discovery.heartbeat = OATime().now
            if self._oag.logger.RPC:
                print("[%s] heartbeat %s" % (self.id, self._rpc_discovery.heartbeat))
            self._rpc_discovery.db.update()

            gevent.sleep(getenv().rpctimeout)

    def __cb_discovery_timeout(self):
        if self._oag.logger.RPC:
            print("[%s] Starting discovery timeout at [%s]" % (self.id, datetime.datetime.now().isoformat()))

        gevent.sleep(self._rpc_discovery_timeout)

        if len(self._rpcreqs)==0:
            if self._oag.logger.RPC:
                print("[%s] After [%d] second timeout, [%s] has no clients, making it undiscoverable at [%s]"
                    % (self.id, self._rpc_discovery_timeout, self._oag, datetime.datetime.now().isoformat()))
            self.discoverable = False

class RestProxy(object):
    def __init__(self, oag, rest_enabled):

        ### Store reference to OAG
        self._oag = oag

        self._app = None

        self._rest_enabled = rest_enabled

        self._rest_addr = None

        # Generate wrapper around REST API
        import requests
        for endpoint, details in self._oag.restapi.items():
            def apifn(self, prms, endpoint=endpoint, details=details):

                # Figure out load balanced url to call
                all_stripes = self._oag.__class__('all', searchidx='by_all')
                if all_stripes.size == 0:
                    return {}
                all_urls = ["http://%s:%s%s"% (stripe.host, stripe.port, endpoint) for stripe in all_stripes ]
                url = secrets.choice(all_urls)

                # Build request
                dispatch = {
                    'GET'  : requests.get,
                    'POST' : requests.post
                }

                # Execute request
                return dispatch[details[1][0]](url, prms)

            apifn_name = endpoint[1:].replace('/', '_')
            apifn.__name__ = apifn_name
            setattr(self, apifn_name, types.MethodType(apifn, self))

    @property
    def addr(self):
        if self._rest_enabled:
            return self._rest_addr
        else:
            OAError("REST API has not yet been enabled")

    def start(self, port):
        if self._rest_enabled:
            from flask import Flask, request, redirect, make_response
            self._app = Flask(getenv().envid)

        for endpoint, details in self._oag.restapi.items():
            rootfn = getattr(self._oag, details[0], None)
            self._app.route(endpoint, methods=details[1])(rootfn)

        from socket        import gethostname
        from gevent.pywsgi import WSGIServer

        http_server = WSGIServer(('0.0.0.0', port), self._app)
        self._rest_addr = "%s:%d" % (gethostname(), port)
        print('Serving on: [%s]' % self.addr)
        http_server.serve_forever()
