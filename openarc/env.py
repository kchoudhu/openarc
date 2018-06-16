#!/usr/bin/env python3

from gevent import monkey
monkey.patch_all()

import atexit
import base64
import gevent
import gevent.queue
import inspect
import os
import sys
import toml
import traceback
import weakref

from openarc.exception import *

class ACL(object):
    # OAG can only be accessed from current process
    LOCAL_ALL  = 1
    # OAG is as open for business as your mom
    REMOTE_ALL = 2

class OALog(object):
    SQL   = False
    Graph = False
    RPC   = False
    GC    = False

    def log(loginfo=None, ignore_exceptions=False):
        # Information about run env: hostname, pid, time
        if loginfo:
            print(loginfo)
        # redirect to stdout, other logger as necessary
        logstr = traceback.format_exc()
        if logstr != "None\n" and ignore_exceptions is False:
            print(logstr)

def initoags():

    create_index = {}

    modules = sorted(sys.modules)
    for module in modules:
        fns = inspect.getmembers(sys.modules[module], inspect.isclass)

        # If classes are OAG_RootNodes, create their tables in the database
        for fn in fns:
            if 'OAG_RootNode' in [x.__name__ for x in inspect.getmro(fn[1])] and fn[1].__name__ != 'OAG_RootNode':
                if fn[1].__name__ not in create_index:
                    try:
                        create_index[fn[1].__name__] = fn[1]().db.schema.init()
                    except OAError:
                        pass

    # Once all tables are materialized, we need to create foreign key relationships
    for fn in create_index:
        try:
            create_index[fn].db.schema.init_fkeys()
        except OAError:
            pass

# Global context
#
# This section allows the storage of references to objects so
# that they are not garbage collected without our explicit say so.
# This is relevant in distributed scenarios

p_gctx = None
p_rootnode_cls = None

class OAGlobalContext(object):

    def __init__(self):
        # Maintain references to OAGs in order to prevent GC
        self._keepalive = {}

        # Maintain running greenlets here in set form:
        self._glets = []

        # Queue delayed deregistration messages to other nodes
        self._deferred_rm_queue = gevent.queue.Queue()

    def put(self, obj):
        try:
            self._keepalive[obj] += 1
        except KeyError:
            self._keepalive[obj] = 1

    def rm(self, removee, notifyee=None, stream=None):
        global p_rootnode_cls
        if p_rootnode_cls is None:
            from openarc.graph import OAG_RootNode
            p_rootnode_cls = OAG_RootNode
        if isinstance(removee, p_rootnode_cls):
            # We have been passed an OAG for direct removal
            try:
                self._keepalive[removee] -= 1
                if self._keepalive[removee] == 0:
                    del(self._keepalive[removee])
            except KeyError:
                OAError("I don't think this should ever happen")
        else:
            # We have been passed a URL for deferred removal
            self._deferred_rm_queue.put((removee, notifyee, stream))

    @property
    def rm_queue(self):

        return self._deferred_rm_queue

    @property
    def rm_queue_size(self):

        return self._deferred_rm_queue.qsize()

    # Greenlet put
    def put_glet(self, oag, greenlet):
        self._glets.append((weakref.ref(oag), greenlet))

        # Do a quick sweep of greenlets that need to die
        kill_glets = [g[1] for g in self._glets if g[0]() is None]
        gevent.killall(kill_glets, block=True)
        self._glets = [g for g in self._glets if g[0]() is not None]

    def kill_glet(self, oag):
        kill_glets = [g[1] for g in self._glets if g[0]()==oag]
        self._glets = [g for g in self._glets if g[0]() != oag]
        gevent.killall(kill_glets, block=True)
        return len(kill_glets)

    @property
    def state(self):

        return self._keepalive

    @property
    def glets(self):

        return self._glets

def gctx():
    global p_gctx
    return p_gctx

@atexit.register
def goodbye_world():

    global p_gctx

# Environment initialization
#
# We hold library initialization state here. Library state must
# be initialized using initenv() and accessed using getenv()

p_refcount_env = 0
p_env = None

class OAEnv(object):

    def __init__(self, on_demand_oags, cfgfile='openarc.toml'):
        self.envid = base64.b16encode(os.urandom(16)).decode('ascii')
        self.on_demand_oags = on_demand_oags
        self.rpctimeout = 5
        cfg_dir = os.environ.get("OPENARC_CFG_DIR") if os.environ.get("OPENARC_CFG_DIR") else '.'
        cfg_file_path = "%s/%s" % ( cfg_dir, cfgfile )
        try:
            with open( cfg_file_path ) as f:
                envcfg = toml.loads( f.read() )
                self._envcfg = envcfg

                # The highlights
                self.crypto     = envcfg['crypto']
                self.dbinfo     = envcfg['dbinfo']
                self.extcreds   = envcfg['extcreds']
                self.name       = envcfg['env']
                self.rpctimeout = envcfg['graph']['heartbeat']

        except IOError:
            raise OAError("%s does not exist" % cfg_file_path)

    def cfg(self):
        return self._envcfg

def getenv():
    """Accessor method for global state"""
    global p_env
    return p_env

def initenv(oag=None, on_demand_oags=False):
    """envstr: one of local, dev, qa, prod.
    Does not return OAEnv variable; for that, you
    must call getenv"""
    global p_env
    global p_refcount_env
    global p_gctx

    if p_refcount_env == 0:

        # Initialize environment
        p_env = OAEnv(on_demand_oags)
        p_refcount_env += 1

        # Intialize keepalive structure
        p_gctx = OAGlobalContext()

        # Create all OAGs if on demand oag creation is turned off
        if not p_env.on_demand_oags:

            # Force refresh of some class variables to ensure previous
            # runs of initoag didn't leave them corrupted
            if oag:
                setattr(oag.__class__, '_dbtable_name',      None)
                setattr(oag.__class__, '_stream_db_mapping', None)

            initoags()
