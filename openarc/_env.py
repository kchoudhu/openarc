__all__ = [
    'oactx',
    'oaenv',
    'oainit',
    'oalog',
]

import atexit
import attrdict
import base64
import datetime
import gevent
import gevent.queue
import inflection
import inspect
import locale
import logging
import os
import sys
import time
import toml
import traceback
import weakref

from openarc.exception import *

class OASingleton(object):
    """There can only be one canonical source of information for anything
    deriving from this class. Proxy all requests to the canonical gvar if current
    instance is determined to be non-canonical"""
    def __init__(self, gvar):
        self.gvar = gvar

    def __getattribute__(self, attr):
        gvar = globals()[object.__getattribute__(self, 'gvar')]
        if not gvar or self==gvar:
            return object.__getattribute__(self, attr)
        else:
            return object.__getattribute__(gvar, attr)

# Logging
#
# Globally, there is one logger, initialized within the OAGlobalContext, and
# accessed via the oalog variable below.

# Exportable symbol
oalog = None

# Internals
class OALogFormatter(logging.Formatter):

    """See: https://gist.github.com/sloanlance/c8afc5da9847597bb54b52b9904994ba"""
    def __init__(self, logFormat=None, timeFormat=None, **kwargs):
        super(OALogFormatter, self).__init__(logFormat, timeFormat, **kwargs)

        from dateutil.tz import tzutc
        self._TIMEZONE_UTC = tzutc()

    def formatTime(self, record, timeFormat=None):
        if timeFormat is not None:
            return super(OALogFormatter, self).formatTime(record, timeFormat)

        return datetime.datetime.fromtimestamp(record.created, self._TIMEZONE_UTC).isoformat()

    def format(self, record):
        global oalog
        record.corrid = oalog.correlation_id
        return super().format(record)

class OALogManager(OASingleton):

    def __init__(self, gvar, name='openarc.core'):
        super(OALogManager, self).__init__(gvar)

        # In debug mode log turn off all logging first. Reset these nodes from
        # config as necessary
        global oaenv
        for family in ['GC', 'GRAPH', 'RPC', 'SQL', 'TRANSPORT']:
            setattr(self, family, False)
        try:
            for family, value in oaenv.logging.debug.items():
                setattr(self, family.upper(), value)
        except AttributeError:
            pass

        self._correlation_id = {}

        # Stow logger objects here
        self._logger = self.set_logger(name)

    def set_logger(self, name):

        # Set up core logger
        self._logger = logging.getLogger(name)
        if self._logger.handlers:
            return

        # Set loglevel from config
        self._logger.setLevel(getattr(logging, oaenv.logging.level.upper(), 0))

        # Set format from config
        handler = logging.StreamHandler()
        handler.setFormatter(OALogFormatter(oaenv.logging.format, style='{'))
        self.handler = handler

        self._logger.addHandler(self.handler)

    def __call__(self, corrid=None):
        if not corrid:
            # Todo: make sure a correlation ID is generated
            pass

        self._correlation_id[gevent.greenlet.getcurrent()] = corrid
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            del(self._correlation_id[gevent.greenlet.getcurrent()])
        except KeyError:
            pass

    @property
    def correlation_id(self):
        try:
            return self._correlation_id[gevent.greenlet.getcurrent()]
        except KeyError:
            return None

    # Minimal pseudo logger interface with debug sugar thrown in
    def critical(self, msg, *args, **kwargs):
        self._logger.critical(msg, *args, **kwargs)

    def debug(self, msg, *args, f=None, **kwargs):
        if f is None or getattr(self, f.upper(), None):
            self._logger.debug(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self._logger.error(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self._logger.info(msg, *args, **kwargs)

    @property
    def logger(self):
        return self._logger

    def warning(self, msg, *args, **kwargs):
        self._logger.warning(msg, *args, **kwargs)

# Global context
#
# This section allows the storage of references to objects so
# that they are not garbage collected without our explicit say so.
# This is relevant in distributed scenarios

# Exportable symbol
oactx = None

# Internals
class OAGlobalContext(object):

    def __init__(self):
        # Maintain references to OAGs in order to prevent GC
        self._keepalive = {}

        # Maintain running greenlets here in set form:
        self._glets = []

        # Queue delayed deregistration messages to other nodes
        self._deferred_rm_queue = gevent.queue.Queue()

        # Cache database-to-class mappings
        self._db_class_mapping = {}

        # Database connection for this context
        self._db_conn = None

        # Global transaction
        self._db_txn = None

        # Rpc Router
        self._rpcrtr = None

        # Make accessible globally
        global oactx
        oactx = self

    def db_class_mapping(self, db_table_name):
        try:
            class_name = self._db_class_mapping[db_table_name]
        except KeyError:
            class_name = 'OAG_' + inflection.camelize(db_table_name)
            self._db_class_mapping[db_table_name] = class_name

        return class_name

    @property
    def db_conn(self):
        if self._db_conn is None:
            import psycopg2
            dbinfo = oaenv.dbinfo
            print("intializing database connection")
            self._db_conn = psycopg2.connect(dbname=dbinfo['dbname'],
                                             user=dbinfo['user'],
                                             password=dbinfo['password'],
                                             host=dbinfo['host'],
                                             port=dbinfo['port'])
        return self._db_conn

    @property
    def db_txndao(self):

        return self._db_txn

    @db_txndao.setter
    def db_txndao(self, newtxn):

        self._db_txn = newtxn

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
        global oalog
        return oalog

# Environment initialization
#
# We hold library initialization state here. Library state must
# be initialized using oainit() and accessed using oaenv

# Exportable symbol
oaenv = None

# Internals
p_refcount_env = 0
p_testmode = False

class OAEnv(OASingleton):

    def __init__(self, gvar, cfgfile=None):
        super(OAEnv, self).__init__(gvar)

        self.envid = base64.b16encode(os.urandom(16)).decode('ascii')

        def get_cfg_file_path():

            # If cfgfile has been specified, you are lucky. If not, do song
            # and dance to figure out where it is.
            if cfgfile:
                cfg_file_path = cfgfile
            else:
                cfgname = 'openarc.conf'
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
        print(f'Loading OPENARC config: [{cfg_file_path}]')

        try:
            with open( cfg_file_path ) as f:
                envcfg = attrdict.AttrDict(toml.loads( f.read() ))
                self._envcfg = envcfg

                # The highlights
                self.crypto     = attrdict.AttrDict(envcfg['crypto'])
                self.dbinfo     = attrdict.AttrDict(envcfg['dbinfo'])
                self.logging    = attrdict.AttrDict(envcfg['logging'])
                self.rpctimeout = self._envcfg.graph.heartbeat
        except IOError:
            raise OAError(f'{cfg_file_path} does not exist')

        # Set external symbol
        global oaenv
        oaenv = self

    def __call__(self, node):
        return getattr(self, node, None) if node else oaenv

    def init_db(self, oag=None):

        # Create all OAGs if on demand oag creation is turned off
        if not oaenv.dbinfo['on_demand_schema']:

            # Force refresh of some class variables to ensure previous
            # runs of initoag didn't leave them corrupted
            if oag:
                setattr(oag.__class__, '_dbtable_name',      None)
                setattr(oag.__class__, '_stream_db_mapping', None)

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
                                if fn[1].streamable:
                                    create_index[fn[1].__name__] = fn[1]().db.schema.init()
                            except OAError:
                                pass

            # Once all tables are materialized, we need to create foreign key relationships
            for fn in create_index:
                try:
                    create_index[fn].db.schema.init_fkeys()
                except OAError:
                    pass

    def init_logging(self, name, reset=False):
        # Global logger
        global oalog
        if not oalog or reset:
            oalog = OALogManager('oalog', name)
        oalog.set_logger(name)

    def merge_app_cfg(self, app, appcfg):

        self._envcfg = attrdict.AttrDict({**self._envcfg, **attrdict.AttrDict({ app : appcfg })})

        if getattr(self, app, None) or getattr(self, 'app', None):
            raise OAError(f'Configuration for {app} already exists')

        setattr(self,  app,  attrdict.AttrDict(appcfg))
        setattr(self, 'app', getattr(self, app, attrdict.AttrDict()).app)

        # Refresh database to create entries from new application
        self.init_db()

        # And renew logger with the appname
        self.init_logging(self.app.name)

    @property
    def cfg(self):
        return self._envcfg

def oainit(reset=False,          # Reset the environment barring one special env prop...
           oag=None,             # Reference to the OAG calling this function
           cfgfile=None):        # Load environment from this cfgfile

    locale.setlocale(locale.LC_ALL, "")

    global oaenv
    global oactx
    global p_refcount_env

    if p_refcount_env==1 and reset:
        p_refcount_env -= 1

    if p_refcount_env == 0:

        # Initialize environment
        OAEnv('oaenv', cfgfile=cfgfile)

        # Initialize global context to get logging+object sink set up
        OAGlobalContext()

        # Make sure we can't reinit environment
        p_refcount_env += 1

        # Now that configs have been read in, do some other stuff
        # Initialize database schema
        oaenv.init_db()

        # Initialize logging
        oaenv.init_logging('openarc.core', reset=reset)
