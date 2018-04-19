#!/usr/bin/env python3

from gevent import monkey, sleep, spawn
monkey.patch_all()

import hashlib
import inspect
import signal
import socket
import sys

from ._db   import *
from ._rdf  import *
from ._rpc  import rtrcls, reqcls, RpcTransaction, RpcProxy, RestProxy
from ._util import oagprop, staticproperty

from openarc.env       import *
from openarc.exception import *
from openarc.oatime    import *

class OAG_RootNode(object):

    ##### Class variables
    _fkframe = []

    ##### Proxies
    @property
    def cache(self):

        return self._cache_proxy

    @property
    def db(self):

        return self._db_proxy

    @property
    def rdf(self):

        return self._rdf_proxy

    @property
    def propmgr(self):

        return self._prop_proxy

    @property
    def rpc(self):

        return self._rpc_proxy

    @property
    def REST(self):

        return self._rest_proxy

    ##### User defined via inheritance
    @staticproperty
    def context(cls):

        raise NotImplementedError("Must be implemented in deriving OAGraph class")

    @staticproperty
    def dbindices(cls): return {}

    @staticproperty
    def dblocalsql(cls): return {}

    @staticproperty
    def infname_fields(cls):
        """Override in deriving classes as necessary"""
        return sorted([k for k, v in cls.streams.items()])

    @staticproperty
    def is_unique(cls): return False

    @staticproperty
    def restapi(cls): return []

    @staticproperty
    def streamable(cls): return True

    @staticproperty
    def streams(cls):

        raise NotImplementedError("Must be implemented in deriving OAGraph class")

    ##### Derivative fields
    @staticproperty
    def dbpkname(cls): return "_%s_id" % cls.dbtable

    @staticproperty
    def dbtable(cls):
        if not getattr(cls, '_dbtable_name', None):
            db_table_name = inflection.underscore(cls.__name__)[4:]
            reverse_class_name = "OAG_"+inflection.camelize(db_table_name)
            if reverse_class_name != cls.__name__:
                raise OAError("This table name isn't reversible: [%s]->[%s]->[%s]" % (cls.__name__, db_table_name, reverse_class_name))
            setattr(cls, '_dbtable_name', db_table_name)
        return cls._dbtable_name

    @classmethod
    def is_oagnode(cls, stream):
        streaminfo = cls.streams[stream][0]
        if type(streaminfo).__name__=='type':
            return 'OAG_RootNode' in [x.__name__ for x in inspect.getmro(streaminfo)]
        else:
            return False

    @staticproperty
    def stream_db_mapping(cls):
        if not getattr(cls, '_stream_db_mapping', None):
            schema = {}
            for stream, streaminfo in cls.streams.items():
                if cls.is_oagnode(stream):
                    schema[stream] = streaminfo[0].dbpkname[1:]+'_'+stream
                else:
                    schema[stream] = stream
            setattr(cls, '_stream_db_mapping', schema)
        return cls._stream_db_mapping

    ##### User API
    @property
    def id(self):
        try:
            return self.propmgr._cframe[self.dbpkname]
        except:
            return None

    @property
    def infname(self):
        if len(self.propmgr._cframe)==0:
            raise OAError("Cannot calculate infname if OAG attributes have not set")

        hashstr = str()
        for stream in self.infname_fields:
            node = getattr(self, stream, None)
            hashstr += node.infname if self.is_oagnode(stream) else str(node)

        return hashlib.sha256(hashstr.encode('utf-8')).hexdigest()

    @property
    def infname_semantic(self):
        if None in [self.db.searchidx, self.db.searchprms]:
            raise OAError("Cannot calculate infname_semantic if search parameters are not initialized")

        hashstr = str()
        hashstr += self.context
        hashstr += self.__class__.__name__
        hashstr += self.db.searchidx
        for searchprm in self.db.searchprms:
            hashstr += str(searchprm)

        return hashlib.sha256(hashstr.encode('utf-8')).hexdigest()

    @property
    def logger(self): return self._logger

    def next(self):
        if self.is_unique:
            raise OAError("next: Unique OAGraph object is not iterable")
        else:
            if self._iteridx < self.size:

                # Clear all properties (use class clear for oagprops)
                self.propmgr.clear_all()

                # Clear oagcache
                self.cache.clear()

                # Set cframe according to rdf
                self.propmgr._cframe = self.rdf._rdf_window[self._iteridx]

                # Set attributes from cframe
                self.propmgr._set_attrs_from_cframe()

                # Set up next iteration
                self._iteridx += 1
                return self
            else:
                self._iteridx = 0
                raise StopIteration()

    @property
    def oagid(self): return self._oagid

    @property
    def oagurl(self): return self.rpc.router.addr

    def clone(self):
        oagcopy = self.__class__()

        oagcopy._iteridx        = 0

        # Clone proxies
        oagcopy.rdf.clone(self)
        oagcopy.db.clone(self)
        oagcopy._prop_proxy.clone(self)

        return oagcopy

    def reset(self, idxreset=True):
        self.rdf._rdf_window = self.rdf._rdf
        if idxreset:
            self._iteridx = 0
        self.propmgr._set_attrs_from_cframe()
        return self

    @property
    def size(self):
        if self.rdf._rdf_window is None:
            return 0
        else:
            return len(self.rdf._rdf_window)

    ##### Stream attributes

    ##### Internals
    def __del__(self):
        try:
            self._prop_proxy.profile_deregister(self)
        except Exception as e:
            print(e.message)
            print("This should never happen")
            import traceback
            traceback.print_exc()

    def __enter__(self):
        self.rpc.discoverable = True
        return self

    def __exit__(self, type, value, traceback):

        self.rpc.discoverable = False

    def __getattribute__(self, attr):
        try:
            logger  = object.__getattribute__(self, '_logger')
            rpc     = object.__getattribute__(self, '_rpc_proxy')
            if rpc.is_proxy:
                if attr in rpc.proxied_oags:
                    if logger.RPC:
                        print("[%s] proxying request for [%s] to [%s]" % (rpc.router.id, attr, rpc.proxied_url))
                    payload = reqcls(self).getstream(rpc.proxied_url, attr)['payload']
                    if payload['value']:
                        if payload['type'] == 'redirect':
                            for cls in OAG_RootNode.__subclasses__():
                                if cls.__name__==payload['class']:
                                    return cls(initurl=payload['value'], logger=logger)
                        else:
                            return payload['value']
                else:
                    raise AttributeError("[%s] does not exist at [%s]" % (attr, rpc.proxied_url))
        except AttributeError:
            pass

        try:
            propmgr = object.__getattribute__(self, '_prop_proxy')
            oagid   = object.__getattribute__(self, '_oagid')
            propmgr.profile_set(oagid)
        except AttributeError:
            pass

        return object.__getattribute__(self, attr)

    def __getitem__(self, indexinfo):
        self.rdf._rdf_window_index = indexinfo

        if self.is_unique:
            raise OAError("Cannot index OAG that is marked unique")

        self.cache.clear()

        if type(self.rdf._rdf_window_index)==int:
            self.propmgr._cframe = self.rdf._rdf_window[self.rdf._rdf_window_index]
        elif type(self.rdf._rdf_window_index)==slice:
            self.rdf._rdf_window = self.rdf._rdf_window[self.rdf._rdf_window_index]
            self.propmgr._cframe = self.rdf._rdf_window[0]

        self.propmgr._set_attrs_from_cframe()

        return self

    def __init__(self,
                 searchprms=[],
                 searchidx='id',
                 initprms={},
                 initurl=None,
                 exttxn=None,
                 logger=OALog(),
                 rpc=True,
                 rpc_acl=ACL.LOCAL_ALL,
                 rest=False,
                 heartbeat=True):

        # Initialize environment
        initenv()

        # Alphabetize
        self._iteridx        = None
        self._logger         = logger
        self._oagid          = hashlib.sha256(str(self).encode("utf-8")).hexdigest()

        #### Set up proxies

        # Database API
        self._db_proxy       = DbProxy(self, searchprms, searchidx, exttxn)

        # Relational Dataframe manipulation
        self._rdf_proxy      = RdfProxy(self)

        # Set attributes on OAG and keep them in sync with cframe
        self._prop_proxy     = PropProxy(self)

        # Manage oagprop state
        self._cache_proxy    = CacheProxy(self)

        # All RPC operations
        self._rpc_proxy      = RpcProxy(self, initurl=initurl, rpc_enabled=rpc, rpc_acl_policy=rpc_acl, heartbeat_enabled=heartbeat)

        # All REST operations
        self._rest_proxy     = RestProxy(self, rest_enabled=rest)

        if not self._rpc_proxy.is_proxy:
            self._prop_proxy.profile_set(self.oagid)
            self._prop_proxy._set_cframe_from_userprms(initprms, force_attr_refresh=True)
            if self.db.searchprms:
                self.db.search()
                if self.is_unique:
                    self.propmgr._set_attrs_from_cframe_uniq()

            self._rpc_proxy.register_with_surrounding_nodes()
        else:
            self._rpc_proxy.proxied_oags = reqcls(self).register_proxy(self._rpc_proxy.proxied_url, 'proxy')['payload']

    def __iter__(self):
        if self.is_unique:
            raise OAError("__iter__: Unique OAGraph object is not iterable")
        else:
            return self

    def __next__(self):
        if self.is_unique:
            raise OAError("__next__: Unique OAGraph object is not iterable")
        else:
            return self.next()

    def __setattr__(self, attr, newval):

        # Get RPC proyx
        rpc = getattr(self, '_rpc_proxy', None)

        # Setting values on a proxy OAG is nonsensical
        if rpc\
            and rpc.is_proxy\
            and attr in rpc.proxied_oags:
            raise OAError("Cannot set value on a proxy OAG")

        # Stash existing value
        currval = getattr(self, attr, None)

        # Set new value
        super(OAG_RootNode, self).__setattr__(attr, newval)

        # Tell the world
        if rpc\
            and rpc.is_init is True\
            and attr not in rpc.stoplist:
            rpc.distribute_stream_change(attr, currval, newval)

class OAG_RpcDiscoverable(OAG_RootNode):
    @property
    def is_unique(self): return False

    @property
    def context(self): return "openarc"

    @staticproperty
    def dbindices(cls):
        return {
        #Index Name------------Elements------Unique-------Partial
        'rpcinfname_idx' : [ ['rpcinfname'], True  ,      None  ]
    }

    @staticproperty
    def streams(cls): return {
        'rpcinfname' : [ 'text',      "", None ],
        'stripe'     : [ 'int',       0 , None ],
        'url'        : [ 'text',      "", None ],
        'type'       : [ 'text',      "", None ],
        'envid'      : [ 'text',      "", None ],
        'heartbeat'  : [ 'timestamp', "", None ],
    }

    def __cb_heartbeat(self):
        while True:
            # Did our underlying db control row evaporate? If so, holy shit.
            try:
                rpcdisc = OAG_RpcDiscoverable([self.id])[0]
            except OAGraphRetrieveError as e:
                if self.logger.RPC:
                    print("[%s] Underlying db controller row is missing for [%s]-[%d], exiting" % (self.id, self.rpcinfname, self.stripe))
                sys.exit(1)

            # Did environment change?
            if self.envid != rpcdisc.envid:
                if self.logger.RPC:
                    print("[%s] Environment changed from [%s] to [%s], exiting" % (self.id, self.envid, rpcdisc.envid))
                sys.exit(1)

            self.heartbeat = OATime().now
            if self.logger.RPC:
                print("[%s] heartbeat %s" % (self.id, self.heartbeat))
            self.db.update()
            sleep(getenv().rpctimeout)

    def start_heartbeat(self):
        if self.rpc.is_heartbeat:
            if self.logger.RPC:
                print("[%s] Starting heartbeat greenlet" % (self.id))
            self.rpc._glets.append(spawn(self.__cb_heartbeat))

class OAG_RootD(OAG_RootNode):
    @staticproperty
    def context(cls): return "openarc"

    @staticproperty
    def daemonname(cls): return cls.dbtable

    @staticproperty
    def dbindices(cls): return {
        'host' : [ ['host'], False, None ]
    }

    @staticproperty
    def streams(cls): return {
        'host'    : [ 'text', str, None ],
        'stripe'  : [ 'int',  int, None ],
    }

    def __enter__(self):

        self.db.create()
        return self

    def __exit__(self, *args):

        self.db.delete()
        sys.exit(0)

    def start(self):

        hostname = socket.gethostname()
        daemoncfg = getenv().cfg()[self.daemonname]
        stripe_info = [hosts for hosts in daemoncfg['hosts'] if hosts['host']==hostname]

        # Am I even allowed to run on this host?
        if len(stripe_info)==0:
            raise OAError("[%s] is not configured to run on [%s]." % (self.daemonname, hostname))

        # Are there too many stripes?
        try:
            _d = self.__class__(hostname, 'by_host')
            num_stripes = _d.size
        except OAGraphRetrieveError as e:
            num_stripes = 0
        if num_stripes>=stripe_info[0]['stripes']:
            raise OAError("All necessary stripes are already running")

        # set up and run this daemon
        self.host = hostname
        self.stripe = num_stripes
        with self as daemon:
            signal.signal(signal.SIGTERM, self.__exit__)
            signal.signal(signal.SIGINT, self.__exit__)
            daemon.REST.start(port=daemoncfg['startport']+self.stripe)
