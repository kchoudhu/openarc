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

class OAGRPC(object):

    @property
    def addr(self): return "tcp://%s:%s" % (self.runhost, self.port)

    @property
    def id(self): return self._hash

    @property
    def port(self): return self._ctxsoc.LAST_ENDPOINT.decode().split(":")[-1]

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
            if isinstance(target, OAGRPC):
                addr = target.addr
            else:
                addr = target

            self._ctxsoc.connect(addr)

            payload = fn(self, args, kwargs)
            payload['action'] = fn.__name__

            # This should eventually derive from the Auth mgmt object
            # used to initialize the OAGRPC
            payload['authtoken'] = getenv().envid

            # An identifier for this conversation
            payload['conv_id'] = base64.b16encode(os.urandom(5))

            if self._oag.logger.RPC:
                print("========>")
                if addr==target:
                    toaddr = addr
                else:
                    toaddr = target.id
                print("[%s:req:%s] Sending RPC request with payload [%s] to [%s]" % (self._oag.rpc.router.id, payload['conv_id'], payload, toaddr))

            self._ctxsoc.send(msgpack.dumps(payload))
            reply = self._ctxsoc.recv()

            rpcret = msgpack.loads(reply, raw=False)

            if self._oag.logger.RPC:
                print("[%s:req:%s] Received reply [%s]" % (self._oag.rpc.router.id, rpcret['conv_id'], rpcret))
                print("<======== ")

            if rpcret['status'] == 'OK':
                return rpcret
            if rpcret['status'] == 'DEAD':
                self._oag.rpc.registration_invalidate(self.addr)
                return rpcret
            if rpcret['status'] == 'FAIL':
                raise OAError("[%s:req] Failed with status [%s] and message [%s]" % (self.id,
                                                                                     rpcret['status'],
                                                                                     rpcret['message']))

            ### This should NEVER happen
            raise OAError("This should never be triggered")

        return wrapfn

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
                acl_policy = self._oag._rpc_proxy._rpc_acl_policy
                if acl_policy == ACL.LOCAL_ALL:
                    if args[0]['authtoken'] != getenv().envid:
                        raise OAError("Client unauthorized")
                elif acl_policy == ACL.REMOTE_ALL:
                    pass

                fn(self, ret, args[0]['args'])
            except OAError as e:
                ret['status'] = 'FAIL'
                ret['message'] = e.message

            return ret

        return wrapfn

    @property
    def runhost(self): return socket.gethostname()

    def __init__(self, zmqtype, oag):
        self.zmqtype  = zmqtype
        self._ctxsoc  = _zmqctx.socket(zmqtype)
        self._oag     = weakref.ref(oag)
        self._hash    = base64.b16encode(os.urandom(5))

    def __getattribute__(self, attrname):
        attr = object.__getattribute__(self, attrname)
        return attr() if type(attr)==weakref.ref else attr

class OAGRPC_RTR_Requests(OAGRPC):
    """Process all RPC calls from other OAGRPC_REQ_Requests"""
    def __init__(self, oag):

        super(OAGRPC_RTR_Requests, self).__init__(zmq.ROUTER, oag)

    def start(self):
        self._ctxsoc.bind("tcp://*:0")

    def _send(self, sender, payload):
        self._ctxsoc.send(sender, zmq.SNDMORE)
        self._ctxsoc.send(str().encode('ascii'), zmq.SNDMORE)
        self._ctxsoc.send(msgpack.dumps(payload))

    def _recv(self):
        sender  = self._ctxsoc.recv()
        empty   = self._ctxsoc.recv()
        payload = msgpack.loads(self._ctxsoc.recv(), raw=False)

        return (sender, payload)

    @OAGRPC.rpcprocfn
    def proc_deregister(self, ret, args):
        self._oag.rpc.registration_invalidate(args['deregister_addr'])

    @OAGRPC.rpcprocfn
    def proc_getstream(self, ret, args):
        from .graph import OAG_RootNode
        attr = getattr(self._oag, args['stream'], None)
        if isinstance(attr, OAG_RootNode):
            ret['payload']['type']  = 'redirect'
            ret['payload']['value'] = attr.rpc.router.addr
            ret['payload']['redir_id'] = attr.rpc.router.id
            ret['payload']['class'] = attr.__class__.__name__
        else:
            ret['payload']['type']  = 'value'
            ret['payload']['value'] = attr

    @OAGRPC.rpcprocfn
    def proc_invalidate(self, ret, args):

        invstream = args['stream']

        if self._oag.logger.RPC:
            print('[%s:rtr:%s] invalidation signal received' % (self._oag.rpc.router.id, ret['conv_id']))

        self._oag.cache.invalidate(invstream)

        # Inform upstream
        for addr, stream in self._oag.rpc.registrations.items():
            OAGRPC_REQ_Requests(self._oag).invalidate(addr, stream)

        # Execute any event handlers
        try:
            if invstream in self._oag.streams.keys():
                evhdlr = self._oag.streams[invstream][2]
                if evhdlr:
                    getattr(self._oag, evhdlr, None)()
        except KeyError as e:
            pass

    @OAGRPC.rpcprocfn
    def proc_register(self, ret, args):
        self._oag.rpc.registration_add(args['addr'], args['stream'])

    @OAGRPC.rpcprocfn
    def proc_register_proxy(self, ret, args):
        self._oag.rpc.registration_add(args['addr'], args['stream'])

        rawprops = list(self._oag.streams.keys())\
                   + [p for p in dir(self._oag.__class__) if isinstance(getattr(self._oag.__class__, p), property)]\
                   + [p for p in dir(self._oag.__class__) if isinstance(getattr(self._oag.__class__, p), oagprop)]\
                   + list(self._oag.propmgr._oagprops.keys())

        ret['payload'] = [p for p in list(set(rawprops)) if p not in self._oag.rpc.stoplist]

    @OAGRPC.rpcprocfn
    def proc_update_broadcast(self, ret, args):
        if self._oag.logger.RPC:
            print('[%s:rtr:%s] update broadcast signal received from %s' % (self._oag.rpc.router.id, ret['conv_id'], args['addr']))
        self._oag.db.search()

        # Tell upstream
        if self._oag.logger.RPC:
            print('[%s:rtr:%s] sending updates to %s' % (self._oag.rpc.router.id, ret['conv_id'], self._oag.rpc.registrations))
        for addr, stream in self._oag.rpc.registrations.items():
            OAGRPC_REQ_Requests(self._oag).invalidate(addr, stream)


rtrcls = OAGRPC_RTR_Requests

class OAGRPC_REQ_Requests(OAGRPC):
    """Make RPC calls to another node's OAGRPC_RTR_Requests"""
    def __init__(self, oag):
        super(OAGRPC_REQ_Requests, self).__init__(zmq.REQ, oag)

    @OAGRPC.rpcfn
    def deregister(self, *args, **kwargs):
        return  {
            'args'      : {
                'deregister_addr' : args[0][0],
                'stream' : args[0][1],
                'addr'   : self._oag.rpc.router.addr
            }
        }

    @OAGRPC.rpcfn
    def getstream(self, *args, **kwargs):
        return {
            'args'      : {
                'stream' : args[0][0]
            }
        }

    @OAGRPC.rpcfn
    def invalidate(self, *args, **kwargs):
        return {
            'args'      : {
                'stream' : args[0][0]
            }
        }

    @OAGRPC.rpcfn
    def register(self, *args, **kwargs):
        return {
            'args'      : {
                'stream' : args[0][0],
                'addr'   : self._oag.rpc.router.addr
            }
        }

    @OAGRPC.rpcfn
    def register_proxy(self, *args, **kwargs):
        return {
            'args'      : {
                'stream' : args[0][0],
                'addr'   : self._oag.rpc.router.addr
            }
        }

    @OAGRPC.rpcfn
    def update_broadcast(self, *args, **kwargs):
        return {
            'args' : {
                'addr'   : self._oag.rpc.router.addr
            }
        }

reqcls = OAGRPC_REQ_Requests

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
                reqcls(self._rpc_proxy._oag).invalidate(addr, stream_to_invalidate)

        self.is_active = False

class RpcProxy(object):
    """Manipulates rpc functionality for OAG"""
    def __init__(self,
                 oag,
                 initurl=None,
                 rpc_enabled=True,
                 rpc_acl_policy=ACL.LOCAL_ALL,
                 rpc_async=True,
                 rpc_dbupdate_listen=False,
                 rpc_discovery_timeout=0,
                 heartbeat_enabled=True):

        ### Store reference to OAG
        self._oag = weakref.ref(oag)

        ### Spin up rpc infrastructure

        # Imports
        from gevent import spawn, monkey
        from gevent.lock import BoundedSemaphore

        # Is RPC initialization complete?
        self._rpc_init_done = False

        # A very basic question...
        self._rpc_enabled = rpc_enabled

        # Who's allowed to access this node?
        self._rpc_acl_policy = rpc_acl_policy

        # Serialize access to RPC
        self._rpcsem = BoundedSemaphore(1)

        # Routes all incoming RPC requests (dead to start with)
        self._rpcrtr = None

        # Registrations received from other OAGs
        self._rpcreqs = {}

        # Async
        self._rpc_async = rpc_async

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
            'propmgr',
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

        # RPC routing
        if not self._rpc_init_done:

            monkey.patch_all()

            with self._rpcsem:
                self._rpcrtr = OAGRPC_RTR_Requests(self._oag)
                self._rpcrtr.start()
                if self._rpc_async:
                    # Force execution of newly spawned greenlets
                    g = spawn(self.start)
                    g.name = "%s/%s" % (str(self._rpcrtr.id), self._oag)
                    gctx().put_glet(self._oag, g)
                    gevent.sleep(0)

            # Avoid double RPC initialization
            self._rpc_init_done = True

        # Proxying
        if self._proxy_url:
            self._proxy_mode = True

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
                print("[%s] Killing [%d] heartbeat greenlets" % (self.router.id, kill_count))
            kill_count = gctx().kill_glet(self, 'discovery')
            if self._oag.logger.RPC:
                print("[%s] Killing [%d] discovery greenlets" % (self.router.id, kill_count))
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
                                   % (self.router.id, rpc.type, rpc.stripe, rpc.heartbeat))
                        rpc.db.delete()

                # Is there already an active subscription there?
                if number_active > 0:
                    if not self.fanout:
                        message = "[%s] Active OAG already on inferred name [%s], last HA at [%s]"\
                                   % (self.router.id, rpc.rpcinfname, rpc.heartbeat)
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
                    'url'        : self._oag.oagurl,
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
    def is_async(self):
        return self._rpc_async

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
    def router(self):
        if not self._rpc_enabled:
            raise OAError("This OAG is not RPC enabled")
        return self._rpcrtr

    @property
    def registrations(self):
        return self._rpcreqs

    def registration_add(self, registering_oag_addr, registering_stream):
        self._rpcreqs[registering_oag_addr] = registering_stream
        gctx().put_ka(self._oag)

    def registration_invalidate(self, deregistering_oag_addr):
        self._rpcreqs = {rpcreq:self._rpcreqs[rpcreq] for rpcreq in self._rpcreqs if rpcreq != deregistering_oag_addr}
        gctx().rm_ka(self._oag)

    def start(self):

        rpc_dispatch = {
            'deregister'       : self._rpcrtr.proc_deregister,
            'getstream'        : self._rpcrtr.proc_getstream,
            'invalidate'       : self._rpcrtr.proc_invalidate,
            'register'         : self._rpcrtr.proc_register,
            'register_proxy'   : self._rpcrtr.proc_register_proxy,
            'update_broadcast' : self._rpcrtr.proc_update_broadcast,
        }

        if self._oag.logger.RPC:
            print("[%s:rtr] Listening for RPC requests [%s]" % (self._rpcrtr.id, self._oag.__class__.__name__))

        while True:
            (sender, payload) = self._rpcrtr._recv()
            if self._oag is None:
                rpcret = {
                    'status'  : 'DEAD',
                    'conv_id' : payload['conv_id'],
                    'message' : None,
                    'payload' : {},
                }
            else:
                if self._oag.logger.RPC:
                    print("[%s:rtr:%s] Received message [%s]" % (self._rpcrtr.id, payload['conv_id'], payload))
                rpcret = rpc_dispatch[payload['action']](payload)

            self._rpcrtr._send(sender, rpcret)
            if not self.is_async and self.is_proxy:
                reqcls(self._oag).deregister(self.proxied_url, self._oag.router.addr, 'proxy')
                break

    def start_discovery_timeout(self):
        if self.is_timedout:
            if self._oag.logger.RPC:
                print("[%s] Starting timeout greenlet at [%s]" % (self.router.id, datetime.datetime.now().isoformat()))
            gctx().put_glet(self, gevent.spawn(self.__cb_discovery_timeout), glet_type='discovery')

    def start_heartbeat(self):
        if self.is_heartbeat:
            if self._oag.logger.RPC:
                print("[%s] Starting heartbeat greenlet at [%s]" % (self.router.id, datetime.datetime.now().isoformat()))
            gctx().put_glet(self, gevent.spawn(self.__cb_heartbeat), glet_type='heartbeat')

    @property
    def stoplist(self):
        return self._rpc_stop_list

    @property
    def transaction(self):
        return self._rpc_transaction

    def __cb_heartbeat(self):
        while True:
            # Did our underlying db control row evaporate? If so, holy shit.
            try:
                from .graph import OAG_RpcDiscoverable
                rpcdisc = OAG_RpcDiscoverable(self._rpc_discovery.id, rpc=False)[0]
            except OAGraphRetrieveError as e:
                if self._oag.logger.RPC:
                    print("[%s] Underlying db controller row is missing for [%s]-[%d], exiting"
                            % (self.router.id, self._rpc_discovery.rpcinfname, self._rpc_discovery.stripe))
                sys.exit(1)

            # Did environment change?
            if self._rpc_discovery.envid != rpcdisc.envid:
                if self._oag.logger.RPC:
                    print("[%s] Environment changed from [%s] to [%s], exiting"
                            % (self.router.id, self._rpc_discovery.envid, rpcdisc.envid))
                sys.exit(1)

            self._rpc_discovery.heartbeat = OATime().now
            if self._oag.logger.RPC:
                print("[%s] heartbeat %s" % (self.router.id, self._rpc_discovery.heartbeat))
            self._rpc_discovery.db.update()

            gevent.sleep(getenv().rpctimeout)

    def __cb_discovery_timeout(self):
        if self._oag.logger.RPC:
            print("[%s] Starting discovery timeout at [%s]" % (self.router.id, datetime.datetime.now().isoformat()))

        gevent.sleep(self._rpc_discovery_timeout)

        if len(self._rpcreqs)==0:
            if self._oag.logger.RPC:
                print("[%s] After [%d] second timeout, [%s] has no clients, making it undiscoverable at [%s]"
                    % (self.router.id, self._rpc_discovery_timeout, self._oag, datetime.datetime.now().isoformat()))
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
