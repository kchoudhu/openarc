__all__ = [
    'OAG_RootNode',
    'OAG_RootD',
    'OAG_RpcDiscoverable'
]

import datetime
import hashlib
import inflection
import inspect
import os
import signal
import socket
import sys
import toml

from ._db   import *
from ._env  import *
from ._rdf  import *
from ._rpc  import reqcls, RpcTransaction, RpcProxy, RestProxy, RpcACL
from ._util import oagprop, staticproperty

from openarc.exception import *
from openarc.time      import *

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
    def props(self):

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
    def restapi(cls): return {}

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
        ca_prop = getattr(cls, '_dbtable_name', ())
        if not ca_prop or (len(ca_prop)>0 and ca_prop[0]!=cls):
            db_table_name = inflection.underscore(cls.__name__)[4:]
            setattr(cls, '_dbtable_name', (cls, db_table_name))
            if not cls.is_reversible:
                raise OAError("This table name isn't reversible: [%s]" % cls.__name__)
        return cls._dbtable_name[1]

    @classmethod
    def is_oagnode(cls, stream):
        try:
            streaminfo = cls.streams[stream][0]
            if type(streaminfo).__name__=='type':
                return 'OAG_RootNode' in [x.__name__ for x in inspect.getmro(streaminfo)]
            else:
                return False
        except KeyError:
            return False

    @classmethod
    def is_scalar(cls, stream):
        try:
            return type(cls.streams[stream][0])==str
        except KeyError:
            # Stay with me here: if there's a key error, you've either fed this
            # function junk data, or an internal member beginning with '_'; return
            # True.
            return True

    @classmethod
    def is_enum(cls, stream):
        try:
            return cls.is_scalar(stream)==False and cls.is_oagnode(stream)==False
        except KeyError:
            return False

    @staticproperty
    def is_reversible(cls):
        ca_prop = getattr(cls, '_is_reversible', ())
        if not ca_prop or (len(ca_prop)>0 and ca_prop[0]!=cls):
            reverse_class_name = "OAG_"+inflection.camelize(cls.dbtable)
            setattr(cls, '_is_reversible', (cls, reverse_class_name == cls.__name__))
        return cls._is_reversible[1]

    @staticproperty
    def stream_db_mapping(cls):
        ca_prop = getattr(cls, '_stream_db_mapping', ())
        if not ca_prop or (len(ca_prop)>0 and ca_prop[0]!=cls):
            schema = {}
            for stream, streaminfo in cls.streams.items():
                if cls.is_oagnode(stream):
                    schema[stream] = streaminfo[0].dbpkname[1:]+'_'+stream
                else:
                    schema[stream] = stream
            setattr(cls, '_stream_db_mapping', (cls, schema))
        return cls._stream_db_mapping[1]

    @staticproperty
    def db_stream_mapping(cls):
        ca_prop = getattr(cls, '_db_stream_mapping', ())
        if not ca_prop or (len(ca_prop)>0 and ca_prop[0]!=cls):
            setattr(cls, '_db_stream_mapping', (cls, {cls.stream_db_mapping[k]:k for k in cls.stream_db_mapping}))
        return cls._db_stream_mapping[1]

    ##### User API
    @property
    def id(self):
        try:
            return self.props._cframe[self.dbpkname]
        except:
            return None

    @property
    def infname(self):
        if len(self.props._cframe)==0:
            raise OAError("Cannot calculate infname if OAG attributes have not set")

        hashstr = str()
        for stream in self.infname_fields:
            node = getattr(self, stream, None)
            hashstr += node.infname if self.is_oagnode(stream) and node else str(node)

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

    def clone(self):
        oagcopy = self.__class__()

        oagcopy._iteridx        = 0

        # Clone proxies
        oagcopy.rdf.clone(self)
        oagcopy.db.clone(self)
        oagcopy.props.clone(self)

        if oagcopy.is_unique:
            oagcopy.props._set_attrs_from_cframe_uniq()

        return oagcopy

    def reset(self, idxreset=True):
        self.rdf._rdf_window = self.rdf._rdf
        if idxreset:
            self._iteridx = 0
        self.props._set_attrs_from_cframe()
        return self

    @property
    def size(self):
        if self.rdf._rdf_window is None:
            return 0
        else:
            return len(self.rdf._rdf_window)

    @property
    def url(self):
        return self.rpc.url

    ##### Stream attributes

    ##### Internals
    def __del__(self):

        # If the table isn't reversible, OAG would never have been created
        if not self.is_reversible:
            return

        if self.logger.GC:
            print("GC=========>")
            print("Deleting %s %s, %s, proxy: %s" % (self,
                                                     self.rpc.id if self.rpc.is_enabled else str(),
                                                     self.rpc.url if self.rpc.is_enabled else str(),
                                                     self.rpc.is_proxy))

        if self.rpc.is_enabled:

            # Tell upstream proxies that we are going away
            if self.logger.GC:
                print("Delete: proxies")
            if self.rpc.is_proxy:
                if self.logger.GC:
                    print("--> %s" % self.rpc.proxied_url)
                oactx.rm_ka_via_rpc(self.rpc.url, self.rpc.proxied_url, 'proxy')

            # Tell upstream registrations that we are going away
            if self.logger.GC:
                print("Delete: registrations")
                print("--> %s" % self.rpc.registrations)

            # Tell subnodes we are going away
            if self.logger.GC:
                print("Delete: cache")
                print("--> %s" % self.cache.state)
            self.cache.clear()

            if self.logger.GC:
                print("Delete: queue size")
                print("--> %d" % oactx.rm_queue_size)

            # print("Delete: stop router")
            # self.rpc._glets[0].kill()

        if self.logger.GC:
            print("<=========GC")

    def __enter__(self):
        self.rpc.discoverable = True
        return self

    def __exit__(self, type, value, traceback):

        self.rpc.discoverable = False

    def __getattribute__(self, attr):
        """Cascade through the following lookups:

        1. Attempt a lookup via the prop proxy
        2. Attempt to retrieve via RPC if applicable.
        3. Attempt a regular attribute lookup.

        Failure at each step is denoted by the generation of an AttributeError"""
        try:
            props = object.__getattribute__(self, '_prop_proxy')
            return props.get(attr, internal_call=True)
        except AttributeError as e:
            pass

        try:
            if object.__getattribute__(self, 'is_proxy'):
                logger  = object.__getattribute__(self, '_logger')
                rpc     = object.__getattribute__(self, '_rpc_proxy')
                if attr in rpc.proxied_streams:
                    if logger.RPC:
                        print("[%s] proxying request for [%s] to [%s]" % (rpc.id, attr, rpc.proxied_url))
                    payload = reqcls(self).getstream(rpc.proxied_url, attr)['payload']
                    if payload['value']:
                        if payload['type'] == 'redirect':
                            for cls in OAG_RootNode.__subclasses__():
                                if cls.__name__==payload['class']:
                                    return cls(initurl=payload['value'])
                        else:
                            return payload['value']
                else:
                    raise AttributeError("[%s] does not exist at [%s]" % (attr, rpc.proxied_url))
        except AttributeError:
            pass

        return object.__getattribute__(self, attr)

    def __getitem__(self, indexinfo, preserve_cache=False):
        self.rdf._rdf_window_index = indexinfo

        if self.is_unique:
            raise OAError("Cannot index OAG that is marked unique")

        if not preserve_cache and self._iteridx != self.rdf._rdf_window_index:
            self.cache.clear()

        if type(self.rdf._rdf_window_index)==int:
            self.props._cframe = self.rdf._rdf_window[self.rdf._rdf_window_index]
        elif type(self.rdf._rdf_window_index)==slice:
            self.rdf._rdf_window = self.rdf._rdf_window[self.rdf._rdf_window_index]
            self.props._cframe = self.rdf._rdf_window[0]

        self.props._set_attrs_from_cframe()

        return self

    @classmethod
    def __graphsubclasses__(cls):
        subclasses = cls.__subclasses__()
        for subclass in cls.__subclasses__():
            subclasses += subclass.__graphsubclasses__()
        return subclasses

    def __init__(
                 self,
                 # Implied positional args
                 searchprms=[],
                 searchidx='id',
                 searchwin=None,
                 searchoffset=None,
                 searchdesc=False,
                 # Actual Named args
                 heartbeat=True,
                 initprms={},
                 initurl=None,
                 initschema=True,
                 logger=oactx.logger,
                 rest=False,
                 rpc=True,
                 rpc_acl=RpcACL.LOCAL_ALL,
                 rpc_dbupdate_listen=False,
                 rpc_discovery_timeout=0):

        # Initialize environment
        oainit(oag=self)

        # Alphabetize
        self._iteridx        = 0
        self._logger         = logger
        self.is_proxy        = not initurl is None

        #### Set up proxies

        # Database API
        self._db_proxy       = DbProxy(self, searchprms, searchidx, searchwin, searchoffset, searchdesc, initschema)

        # Relational Dataframe manipulation
        self._rdf_proxy      = RdfProxy(self)

        # Set attributes on OAG and keep them in sync with cframe
        self._prop_proxy     = PropProxy(self)

        # Manage oagprop state
        self._cache_proxy    = CacheProxy(self)

        # All RPC operations
        self._rpc_proxy      = RpcProxy(self,
                                        initurl=initurl,
                                        rpc_enabled=rpc,
                                        rpc_acl_policy=rpc_acl,
                                        rpc_dbupdate_listen=rpc_dbupdate_listen,
                                        rpc_discovery_timeout=rpc_discovery_timeout,
                                        heartbeat_enabled=heartbeat)

        # All REST operations
        self._rest_proxy     = RestProxy(self, rest_enabled=rest)

        if not self._rpc_proxy.is_proxy:
            self._prop_proxy._set_cframe_from_userprms(initprms, force_attr_refresh=True)
            if self.db.searchprms:
                self.db.search()
                if self.is_unique:
                    self.props._set_attrs_from_cframe_uniq()
        else:
            self._rpc_proxy.proxied_streams = reqcls(self).register_proxy(self._rpc_proxy.proxied_url, 'proxy')['payload']

        if self.logger.GC:
            print("Creating %s, %s, %s" % (self,
                                           self.rpc.id if self.rpc.is_enabled else str(),
                                          'listening on %s' % self.rpc.url if self.rpc.is_enabled else str()))

    def __iter__(self):
        if self.is_unique:
            raise OAError("__iter__: Unique OAGraph object is not iterable")
        else:
            return self

    def __next__(self):
        if self.is_unique:
            raise OAError("__next__: Unique OAGraph object is not iterable")
        else:
            if self._iteridx < self.size:

                # Clear propcache
                self.props.clear()

                # Clear oagcache
                self.cache.clear()

                # Set cframe according to rdf
                self.props._cframe = self.rdf._rdf_window[self._iteridx]

                # Set attributes from cframe
                self.props._set_attrs_from_cframe()

                # Set up next iteration
                self._iteridx += 1
                return self
            else:
                self._iteridx = 0
                self.cache.clear()
                raise StopIteration()

    def __setattr__(self, attr, newval, fastiter=False):
        try:
            # Sanity check
            if self.rpc.is_proxy and attr in self.rpc.proxied_streams:
                raise OAError("Cannot set value on a proxy OAG")

            # Set new value
            currval = self.props.add(attr, newval, None, None, False, False, fastiter)

        except (AttributeError, OAGraphIntegrityError):
            # Attribute errors means object has not been completely
            # initialized yet; graph integrity errors mean we used
            # property manager to manage property on the stoplist.
            #
            # In either case, default to using the default __setattr__

            super(OAG_RootNode, self).__setattr__(attr, newval)

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
        'envid'      : [ 'text',      "",   None ],
        'heartbeat'  : [ 'timestamp', "",   None ],
        'listen'     : [ 'boolean',   True, None ],
        'rpcinfname' : [ 'text',      "",   None ],
        'stripe'     : [ 'int',       0 ,   None ],
        'type'       : [ 'text',      "",   None ],
        'url'        : [ 'text',      "",   None ],
    }

    @property
    def is_valid(self):
        return OATime().now-self.heartbeat < datetime.timedelta(seconds=oaenv.rpctimeout)

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
        'port'    : [ 'int',  int, None ],
    }

    @staticproperty
    def streamable(cls): return False

    def __enter__(self):
        try:
            self.db.create()
            with open(self.pidfile, 'w') as f:
                f.write(str(os.getpid()))
            return self
        except Exception as e:
            print('[STARTUP ERROR]', e)
            self.__exit__(code=1)

    def __exit__(self, *args, code=0):
        try:
            self.db.delete()
        except Exception as e:
            print('[CLEANUP WARNING]', e)

        try:
            os.unlink(self.pidfile)
        except Exception as e:
            print('[CLEANUP WARNING]', e)

        sys.exit(code)

    def start(self, pidfile=None, cfgfile=None):

        def get_cfg_file_path():

            # If cfgfile has been specified, you are lucky. If not, do song
            # and dance to figure out where it is.
            if cfgfile:
                cfg_file_path = cfgfile
            else:
                cfgname = f'{self.daemonname}.conf'
                cfg_dir = os.environ.get("XDG_CONFIG_HOME")
                if not cfg_dir:
                    for l in [f'~/.config/{cfgname}', f'/usr/local/etc/{cfgname}' ]:
                        cfg_file_path = os.path.expanduser(l)
                        if os.path.exists(cfg_file_path):
                            break
                else:
                    cfg_file_path = os.path.join(cfg_dir, f'{cfgname}')

            return cfg_file_path

        cfg_file_path = get_cfg_file_path()
        print(f'Loading {self.daemonname.upper()} config: [{cfg_file_path}]')
        try:
            with open(cfg_file_path) as f:
                appcfg = toml.loads(f.read())
                oaenv.merge_app_cfg(self.daemonname, appcfg)
        except IOError:
            raise OAError(f'{cfg_file_path} does not exist')

        def get_pid_file_path():
            if pidfile:
                return pidfile
            else:
                pidname = f'{self.daemonname}.pid'
                xdg_rdir = os.environ.get("XDG_RUNTIME_DIR")
                rdir = xdg_rdir if xdg_rdir else '/var/run'
                return f'{rdir}/{pidname}'

        self.pidfile = get_pid_file_path()

        hostname = socket.gethostname()
        daemoncfg = getattr(oaenv, self.daemonname, {})

        # Are there too many stripes?
        allowed_ports = [daemoncfg['startport']+stripe for stripe in range(daemoncfg.stripes)]
        try:
            _d = self.__class__(hostname, 'by_host')
            occupied_ports = [dd.port for dd in _d]
        except OAGraphRetrieveError as e:
            occupied_ports = []
        if len(occupied_ports)==len(allowed_ports):
            raise OAError("All necessary stripes are already running")

        # set up and run this daemon
        self.host = hostname
        self.port = list(set(allowed_ports)-set(occupied_ports))[0]
        with self as daemon:
            signal.signal(signal.SIGTERM, self.__exit__)
            signal.signal(signal.SIGINT, self.__exit__)
            daemon.REST.start(port=self.port)
