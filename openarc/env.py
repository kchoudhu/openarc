#!/usr/bin/env python3

from gevent import monkey
monkey.patch_all()

import atexit
import base64
import inspect
import os
import sys
import toml
import traceback

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

# Keepalive
#
# This section allows the storage of references to objects so
# that they are not garbage collected without our explicit say so.
# This is relevant in distributed scenarios

p_keepalive = None

class OAKeepAlive(object):

    def __init__(self):
        self._keepalive = {}

    def put(self, obj):
        try:
            self._keepalive[obj] += 1
        except KeyError:
            self._keepalive[obj] = 1

    def rm(self, obj):
        try:
            self._keepalive[obj] -= 1
            if self._keepalive[obj] == 0:
                del(self._keepalive[obj])
        except KeyError:
            OAError("I don't think this should ever happen")

    @property
    def state(self):

        return self._keepalive

def getkeepalive():
    global p_keepalive
    return p_keepalive

@atexit.register
def goodbye_world():
    # import objgraph
    # obj = objgraph.by_type('OAG_AutoNode1a')[0]
    # objgraph.show_backrefs([obj], filename='anode2.png', max_depth=100)
    pass

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
    global p_keepalive

    if p_refcount_env == 0:

        # Initialize environment
        p_env = OAEnv(on_demand_oags)
        p_refcount_env += 1

        # Intialize keepalive structure
        p_keepalive = OAKeepAlive()

        # Create all OAGs if on demand oag creation is turned off
        if not p_env.on_demand_oags:

            # Force refresh of some class variables to ensure previous
            # runs of initoag didn't leave them corrupted
            if oag:
                setattr(oag.__class__, '_dbtable_name',      None)
                setattr(oag.__class__, '_stream_db_mapping', None)

            initoags()
