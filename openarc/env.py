#!/usr/bin/env python3

from gevent import monkey
monkey.patch_all()

import atexit
import base64
import gevent
import gevent.queue
import inflection
import inspect
import locale
import os
import sys
import time
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
        try:
            fns = inspect.getmembers(sys.modules[module], inspect.isclass)
        except:
            continue

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

class OAGlobalContext(object):

    def __init__(self):
        # Maintain references to OAGs in order to prevent GC
        self._keepalive = {}

        # Maintain running greenlets here in set form:
        self._glets = []

        # Queue delayed deregistration messages to other nodes
        self._deferred_rm_queue = gevent.queue.Queue()

        # Global logger
        self._logger = OALog()

        # Cache database-to-class mappings
        self._db_class_mapping = {}

        # Database connection for this context
        self.__dbconn = None

        # Rpc Router
        self._rpcrtr = None

    def db_class_mapping(self, db_table_name):
        try:
            class_name = self._db_class_mapping[db_table_name]
        except KeyError:
            class_name = 'OAG_' + inflection.camelize(db_table_name)
            self._db_class_mapping[db_table_name] = class_name

        return class_name

    @property
    def db_conn(self):
        if self.__dbconn is None:
            import psycopg2
            dbinfo = getenv().dbinfo
            print("intializing database connection")
            self.__dbconn = psycopg2.connect(dbname=dbinfo['dbname'],
                                             user=dbinfo['user'],
                                             host=dbinfo['host'],
                                             port=dbinfo['port'])
        return self.__dbconn

    def put_ka(self, oag):
        try:
            self._keepalive[oag] += 1
        except KeyError:
            self._keepalive[oag] = 1

    def rm_ka(self, oag):
        try:
            self._keepalive[oag] -= 1
            if self._keepalive[oag] == 0:
                del(self._keepalive[oag])
        except KeyError:
            OAError("I don't think this should ever happen")

    def rm_ka_via_rpc(self, removee_oag_addr, notifyee_oag_addr, stream):
        self._deferred_rm_queue.put((removee_oag_addr, notifyee_oag_addr, stream))

    @property
    def rm_queue(self):

        return self._deferred_rm_queue

    @property
    def rm_queue_size(self):

        return self._deferred_rm_queue.qsize()

    @property
    def rpcrtr(self):
        if not self._rpcrtr:

            # Start router
            from ._rpc import OARpc_RTR_Requests
            self._rpcrtr = OARpc_RTR_Requests()

            # Force execution of newly spawned greenlets
            from gevent import spawn, sleep
            self.rpcrtr.procglet = spawn(self.rpcrtr.start)

            # Busy wait until router is up and running.
            while not self.rpcrtr.port:
                time.sleep(0.1)

            self.rpcrtr.procglet.name = "%s" % (self.rpcrtr)
            print(self.rpcrtr)
            sleep(0)

        return self._rpcrtr

    # Greenlet put
    def put_glet(self, oag, glet, glet_type=None):
        self._glets.append((weakref.ref(oag), glet, glet_type))

        # Do a quick sweep of greenlets that need to die
        kill_glets = [g for g in self._glets if g[0]() is None]
        self._glets = [g for g in self._glets if g not in kill_glets]
        gevent.killall([g[1] for g in kill_glets], block=True)

    def kill_glet(self, oag, glet_type=None):
        if glet_type:
            kill_glets = [g for g in self._glets if g[0]()==oag and g[2]==glet_type]
        else:
            kill_glets = [g for g in self._glets if g[0]()==oag]
        self._glets = [g for g in self._glets if g not in kill_glets]

        gevent.killall([g[1] for g in kill_glets], block=True)
        return len(kill_glets)

    @property
    def state(self):

        return self._keepalive

    @property
    def glets(self):

        return self._glets

    @property
    def logger(self):

        return self._logger

def gctx():
    global p_gctx
    if p_gctx is None:
        p_gctx = OAGlobalContext()
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
    locale.setlocale(locale.LC_ALL, "")

    global p_env
    global p_refcount_env

    if p_refcount_env == 0:

        # Initialize environment
        p_env = OAEnv(on_demand_oags)
        p_refcount_env += 1

        # Create all OAGs if on demand oag creation is turned off
        if not p_env.on_demand_oags:

            # Force refresh of some class variables to ensure previous
            # runs of initoag didn't leave them corrupted
            if oag:
                setattr(oag.__class__, '_dbtable_name',      None)
                setattr(oag.__class__, '_stream_db_mapping', None)

            initoags()
