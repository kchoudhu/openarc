#!/usr/bin/env python3

from gevent import monkey
monkey.patch_all()

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

class OALog(object):
    SQL   = False
    Graph = False
    RPC   = False

    def log(loginfo=None, ignore_exceptions=False):
        # Information about run env: hostname, pid, time
        if loginfo:
            print(loginfo)
        # redirect to stdout, other logger as necessary
        logstr = traceback.format_exc()
        if logstr != "None\n" and ignore_exceptions is False:
            print(logstr)

def initoags():
    modules = sorted(sys.modules)
    for module in modules:
        fns = inspect.getmembers(sys.modules[module], inspect.isclass)

        # If classes are OAG_RootNodes, create their tables in the database
        for fn in fns:
            if 'OAG_RootNode' in [x.__name__ for x in inspect.getmro(fn[1])] and fn[1].__name__ != 'OAG_RootNode':
                try:
                    fn[1]().db.schema.init()
                except OAError:
                    pass

        # Once all tables are materialized, we need to create foreign key relationships
        for fn in fns:
            if 'OAG_RootNode' in [x.__name__ for x in inspect.getmro(fn[1])] and fn[1].__name__ != 'OAG_RootNode':
                try:
                    fn[1]().db.schema.init_fkeys()
                except OAError:
                    pass

#This is where we hold library state.
#You will get cut if you don't manipulate the p_* variables
#via getenv() and initenv()

p_refcount_env = 0
p_env = None

def initenv(oag=None, on_demand_oags=False):
    """envstr: one of local, dev, qa, prod.
    Does not return OAEnv variable; for that, you
    must call getenv"""
    global p_env
    global p_refcount_env

    if p_refcount_env == 0:
        p_env = OAEnv(on_demand_oags)
        p_refcount_env += 1

        # Create all OAGs if on demand oag creation is turned off
        if not p_env.on_demand_oags:
            initoags()

            # Force refresh of some class variables to ensure initoag
            # didn't corrupt them
            if oag:
                setattr(oag.__class__, '_dbtable_name',      None)
                setattr(oag.__class__, '_stream_db_mapping', None)

def getenv():
    """Accessor method for global state"""
    global p_env
    return p_env
