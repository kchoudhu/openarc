#!/usr/bin/env python2.7

import base64
import datetime
import hashlib
import gevent
import inflection
import inspect
import msgpack
import os
import socket
import sys
import zmq.green as zmq

from gevent            import spawn
from gevent            import monkey
monkey.patch_all()
from gevent.lock       import BoundedSemaphore
from textwrap          import dedent as td

from openarc.dao       import *
from openarc.env       import OALog
from openarc.exception import *
from openarc.oatime    import *

class oagprop(object):
    """Responsible for maitaining _oagcache on decorated properties"""
    def __init__(self, fget=None, fset=None, fdel=None, doc=None):
        self.fget = fget
        self.fset = fset
        self.fdel = fdel
        if doc is None and fget is not None:
            doc = fget.__doc__
        self.__doc__ = doc

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        if self.fget is None:
            raise AttributeError("unreadable attribute")
        try:
            return obj._oagcache[self.fget.func_name]
        except:
            obj._oagcache[self.fget.func_name] = self.fget(obj)
            return obj._oagcache[self.fget.func_name]

    def __set__(self, obj, value):
        pass

class staticproperty(property):
    def __get__(self, cls, owner):
        return classmethod(self.fget).__get__(None, owner)()

class OAGRPC(object):

    @property
    def addr(self): return "tcp://%s:%s" % (self.runhost, self.port)

    @property
    def id(self): return self._hash

    @property
    def port(self): return self._ctxsoc.LAST_ENDPOINT.split(":")[-1]

    @staticmethod
    def rpcfn(fn):
        def wrapfn(self, target, *args, **kwargs):
            if isinstance(target, OAGRPC):
                addr = target.addr
            else:
                addr = target

            self._ctxsoc.connect(addr)

            payload = fn(self, args, kwargs)
            payload['action']    = fn.__name__

            # This should eventually derive from the Auth mgmt object
            # used to initialize the OAGRPC
            payload['authtoken'] = getenv().envid

            if self._oag.logger.RPC:
                print "========>"
                if addr==target:
                    toaddr = addr
                else:
                    toaddr = target.id
                print "[%s:req] Sending RPC request with payload [%s] to [%s]" % (self.id, payload, toaddr)

            self._ctxsoc.send(msgpack.dumps(payload))
            reply = self._ctxsoc.recv()

            rpcret = msgpack.loads(reply)
            if self._oag.logger.RPC:
                print "[%s:req] Received reply [%s]" % (self.id, rpcret)
                print "<======== "

            if rpcret['status'] != 'OK':
                raise OAError("[%s:req] Failed with status [%s] and message [%s]" % (self.id,
                                                                                     rpcret['status'],
                                                                                     rpcret['message']))

            return rpcret

        return wrapfn

    @staticmethod
    def rpcprocfn(fn):
        def wrapfn(self, *args, **kwargs):
            ret = {
                'status'  : 'OK',
                'message' : None,
                'payload' : {},
            }

            try:
                if args[0]['authtoken'] != getenv().envid:
                    raise OAError("Client unauthorized")
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
        self._ctx     = zmq.Context()
        self._ctxsoc  = self._ctx.socket(zmqtype)
        self._oag     = oag
        self._hash    = base64.b16encode(os.urandom(5))

class OAGRPC_RTR_Requests(OAGRPC):
    """Process all RPC calls from other OAGRPC_REQ_Requests"""
    def __init__(self, oag):
        super(OAGRPC_RTR_Requests, self).__init__(zmq.ROUTER, oag)

    def start(self):
        self._ctxsoc.bind("tcp://*:0")

    def _send(self, sender, payload):
        self._ctxsoc.send(sender, zmq.SNDMORE)
        self._ctxsoc.send(str(), zmq.SNDMORE)
        self._ctxsoc.send(msgpack.dumps(payload))

    def _recv(self):
        sender  = self._ctxsoc.recv()
        empty   = self._ctxsoc.recv()
        payload = msgpack.loads(self._ctxsoc.recv())

        if self._oag.logger.RPC:
            print "[%s:rtr] Received message [%s]" % (self.id, payload)

        return (sender, payload)

    @OAGRPC.rpcprocfn
    def proc_deregister(self, ret, args):
        self._oag._rpcreqs = {rpcreq:self._oag._rpcreqs[rpcreq] for rpcreq in self._oag._rpcreqs if rpcreq != args['addr']}

    @OAGRPC.rpcprocfn
    def proc_getstream(self, ret, args):
        attr = getattr(self._oag, args['stream'], None)
        if isinstance(attr, OAG_RootNode):
            ret['payload']['type']  = 'redirect'
            ret['payload']['value'] = attr.rpcrtr.addr
            ret['payload']['class'] = attr.__class__.__name__
        else:
            ret['payload']['type']  = 'value'
            ret['payload']['value'] = attr

    @OAGRPC.rpcprocfn
    def proc_invalidate(self, ret, args):

        invstream = args['stream']

        if self._oag.logger.RPC:
            print '[%s:rtr] invalidation signal received' % self._oag._rpcrtr.id

        # Selectively clear cache
        # - filter out all non-dbstream items
        tmpoagcache = {oag:self._oag._oagcache[oag] for oag in self._oag._oagcache if oag in self._oag.dbstreams.keys()}
        # - filter out invalidated downstream node
        tmpoagcache = {oag:tmpoagcache[oag] for oag in tmpoagcache if oag != invstream}

        # Reset fget
        for stream, streaminfo in self._oag._cframe.items():
            self._oag._set_oagprop(stream, streaminfo)

        # Hack: fix oagcache corruption from setting fgets
        self._oag._oagcache = tmpoagcache

        # Inform upstream
        for addr, stream in self._oag._rpcreqs.items():
            OAGRPC_REQ_Requests(self._oag).invalidate(addr, stream)

        # Execute any event handlers
        try:
            if invstream in self._oag.dbstreams.keys():
                evhdlr = self._oag.dbstreams[invstream][2]
                if evhdlr:
                    getattr(self._oag, evhdlr, None)()
        except KeyError as e:
            pass

    @OAGRPC.rpcprocfn
    def proc_register(self, ret, args):
        self._oag._rpcreqs[args['addr']] = args['stream']

    @OAGRPC.rpcprocfn
    def proc_register_proxy(self, ret, args):
        self._oag._rpcreqs[args['addr']] = args['stream']

        rawprops = self._oag.dbstreams.keys()\
                   + [p for p in dir(self._oag.__class__) if isinstance(getattr(self._oag.__class__, p), property)]\
                   + [p for p in dir(self._oag.__class__) if isinstance(getattr(self._oag.__class__, p), oagprop)]\
                   + getattr(self._oag.__class__, 'oagproplist', [])

        ret['payload'] = [p for p in list(set(rawprops)) if p not in self._oag._rpc_stop_list]

rtrcls = OAGRPC_RTR_Requests

class OAGRPC_REQ_Requests(OAGRPC):
    """Make RPC calls to another node's OAGRPC_RTR_Requests"""
    def __init__(self, oag):
        super(OAGRPC_REQ_Requests, self).__init__(zmq.REQ, oag)

    @OAGRPC.rpcfn
    def deregister(self, *args, **kwargs):
        return  {
            'args'      : {
                'stream' : args[0][0],
                'addr'   : self._oag.rpcrtr.addr
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
                'addr'   : self._oag.rpcrtr.addr
            }
        }

    @OAGRPC.rpcfn
    def register_proxy(self, *args, **kwargs):
        return {
            'args'      : {
                'stream' : args[0][0],
                'addr'   : self._oag.rpcrtr.addr
            }
        }

reqcls = OAGRPC_REQ_Requests

class OAGraphRootNode(object):

    def create(self, initprms={}):

        attrs = self._set_attrs_from_userprms(initprms) if len(initprms)>0 else []
        self._set_cframe_from_attrs(attrs, fullhouse=True)

        if self._rawdata is not None:
            raise OAError("Cannot create item that has already been initiated")

        filtered_cframe = {k:self._cframe[k] for k in self._cframe if k[0] != '_'}
        attrstr    = ', '.join([k for k in filtered_cframe])
        vals       = [filtered_cframe[k] for k in filtered_cframe]
        formatstrs = ', '.join(['%s' for v in vals])
        insert_sql = self.SQL['insert']['id'] % (attrstr, formatstrs)

        if self._extcur is None:
            with OADao(self.dbcontext) as dao:
                with dao.cur as cur:
                    self.SQLexec(cur, insert_sql, vals)
                    index_val = cur.fetchall()
                    self._clauseprms = index_val[0].values()
                    self._refresh_from_cursor(cur)
                    dao.commit()
        else:
            self.SQLexec(self._extcur, insert_sql, vals)
            index_val = self._extcur.fetchall()
            self._clauseprms = index_val[0].values()
            self._refresh_from_cursor(self._extcur)

        # Refresh to set iteridx
        self.refresh()

        # Set attrs if this is a unique oag
        if self.is_unique:
            self._set_attrs_from_cframe_uniq()

        return self

    @property
    def dbcontext(self):

        raise NotImplementedError("Must be implemented in deriving OAGraph class")

    @property
    def infname(self):
        if len(self.infname_fields)==0:
            raise OAError("Cannot calculate infname if infname_fields not set")
        return hashlib.sha256(str().join([str(getattr(self, k, ""))
                                          for k in self.infname_fields
                                          if k[0] != '_'])).hexdigest()

    @property
    def infname_fields(self):
        """Override in deriving classes as necessary"""
        return [k for k, v in self._cframe.items()]

    def init_state_cls(self, clauseprms, indexprm, initprms, extcur, logger):

        self._cframe         = {}
        self._fkframe        = {}
        self._rawdata        = None
        self._rawdata_window = None
        self._oagcache       = {}
        self._clauseprms     = clauseprms
        self._indexparm      = indexprm
        self._extcur         = extcur
        self._logger         = logger

        attrs = self._set_attrs_from_userprms(initprms)
        self._set_cframe_from_attrs(attrs)

    def init_state_dbschema(self):

        return

    def init_state_oag(self):
        if self._clauseprms is not None:
            self.refresh(gotodb=True)

            if len(self._rawdata_window) == 0:
                raise OAGraphRetrieveError("No results found in database")

            if self.is_unique:
                self._set_attrs_from_cframe_uniq()

    @property
    def is_unique(self):

        raise NotImplementedError("Must be implemented in deriving OAGraph class")

    @property
    def logger(self): return self._logger

    def next(self):
        if self.is_unique:
            raise OAError("next: Unique OAGraph object is not iterable")
        else:
            if self.__iteridx < self.size:
                self._oagcache = {}
                self._cframe = self._rawdata_window[self.__iteridx]
                self.__iteridx += 1
                self._set_attrs_from_cframe()
                return self
            else:
                self.__iteridx = 0
                raise StopIteration()

    def refresh(self, gotodb=False):
        """Generally we want to simply reset the iterator; set gotodb=True to also
        refresh instreams from the database"""
        if gotodb is True:
            if self._extcur is None:
                with OADao(self.dbcontext) as dao:
                    with dao.cur as cur:
                        self._refresh_from_cursor(cur)
            else:
                self._refresh_from_cursor(self._extcur)
            self._oagcache = {}

        self._rawdata_window = self._rawdata
        self.__iteridx = 0
        self._set_attrs_from_cframe()
        return self

    @property
    def rpcrtr(self): return self._rpcrtr

    @property
    def size(self):
        if self._rawdata_window is None:
            return 0
        else:
            return len(self._rawdata_window)

    def update(self, updparms={}):

        attrs = self._set_attrs_from_userprms(updparms) if len(updparms)>0 else []
        self._set_cframe_from_attrs(attrs)

        member_attrs  = [k for k in self._cframe if k[0] != '_']
        index_key     = [k for k in self._cframe if k[0] == '_'][0]
        update_clause = ', '.join(["%s=" % attr + "%s"
                                    for attr in member_attrs])
        update_sql    = self.SQL['update']['id']\
                        % (update_clause, getattr(self, index_key, ""))
        update_values = [self._cframe[attr] for attr in member_attrs]
        if self._extcur is None:
            with OADao(self.dbcontext) as dao:
                with dao.cur as cur:
                    self.SQLexec(cur, update_sql, update_values)
                    dao.commit()
        else:
            self.SQLexec(self._extcur, update_sql, update_values)

        return self

    @property
    def SQL(self):

        raise NotImplementedError("Must be implemneted in deriving OAGraph class")

    def SQLexec(self, cur, query, parms=[]):
        if self.logger.SQL:
            print cur.mogrify(query, parms)
        cur.execute(query, parms)

    def __getitem__(self, indexinfo):
        if self.is_unique:
            raise OAError("Cannot index OAG that is marked unique")
        self._oagcache = {}
        if type(indexinfo)==int:
            self._cframe = self._rawdata_window[indexinfo]
        elif type(indexinfo)==slice:
            self._rawdata_window = self._rawdata[indexinfo]
            self._cframe = self._rawdata_window[0]

        self._set_attrs_from_cframe()

        return self

    def __init__(self, clauseprms=None, indexprm='id', initprms={}, extcur=None, logger=OALog(), rpc=True, heartbeat=True):
        self.init_state_cls(clauseprms, indexprm, initprms, extcur, logger)
        self.init_state_dbschema()
        self.init_state_oag()

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

    def _refresh_from_cursor(self, cur):
        if type(self.SQL).__name__ == "str":
            self.SQLexec(cur, self.SQL, self._clauseprms)
        elif type(self.SQL).__name__ == "dict":
            self.SQLexec(cur, self.SQL['read'][self._indexparm], self._clauseprms)
        self._rawdata = cur.fetchall()
        self._rawdata_window = self._rawdata

    def _set_attrs_from_cframe(self):
        for k, v in self._cframe.items():
            setattr(self, k, v)

    def _set_attrs_from_cframe_uniq(self):
        if len(self._rawdata_window) > 1:
            raise OAGraphIntegrityError("Graph object indicated unique, but returns more than one row from database")

        if len(self._rawdata_window) == 1:
            self._cframe = self._rawdata_window[0]
        else:
            self._cframe = []

        self._set_attrs_from_cframe()

    def _set_attrs_from_userprms(self, userprms):
        """Set attributes corresponding to params in userprms, return list of
        attrs created"""
        for k, v in userprms.items():
            setattr(self, k, v)
        return userprms.keys()

    def _set_cframe_from_attrs(self, keys, fullhouse=False):
        if len(keys)==0:
            dbstreams = self._cframe.keys()
        else:
            dbstreams = keys
        for stream in dbstreams:
            self._cframe[stream] = getattr(self, stream, "")

class OAG_RootNode(OAGraphRootNode):

    @staticproperty
    def db_oag_mapping(cls):
        schema = {}
        for stream, streaminfo in cls.dbstreams.items():
            if cls.is_oagnode(stream):
                schema[stream] = streaminfo[0].dbpkname[1:]
            else:
                schema[stream] = stream
        return schema

    @staticproperty
    def dbindices(cls): return {}

    @staticproperty
    def dbpkname(cls): return "_%s_id" % cls.dbtable

    @staticproperty
    def dbtable(cls): return inflection.underscore(cls.__name__)[4:]

    @staticproperty
    def dbstreams(cls):

        raise NotImplementedError("Must be implemented in deriving OAGraph class")

    def delete(self):
        delete_sql    = self.SQL['delete']['id']

        if self._extcur is None:
            with OADao(self.dbcontext) as dao:
                with dao.cur as cur:
                    self.SQLexec(cur, delete_sql, [self.id])
                    dao.commit()
        else:
            self.SQLexec(cur, delete_sql, [self.id])

        self.refresh(gotodb=True)

        if self.is_unique:
            self._set_attrs_from_cframe_uniq()

        return self

    def discover(self):
        remote_oag =\
            OAG_RpcDiscoverable({
                'rpcinfname' : self.infname
            }, 'by_rpcinfname_idx', rpc=False)
        return self.__class__(initurl=remote_oag[0].url)

    @property
    def discoverable(self): return self._rpc_discovery is not None

    @discoverable.setter
    def discoverable(self, value):
        if self._rpc_discovery==value:
            return

        if value is False:
            if self.logger.RPC:
                print "[%s] Killing rpcdisc greenlets [%d]" % (self._rpc_discovery.id, len(self._rpc_discovery._glets))
            [glet.kill() for glet in self._rpc_discovery._glets]
            if self.logger.RPC:
                print "[%s] Killing OAG greenlets [%d]" % (self._rpc_discovery.id, len(self._glets))
            [glet.kill() for glet in self._glets]
            gevent.joinall(self._glets+self._rpc_discovery._glets)
            self._rpc_discovery.delete()
            self._rpc_discovery = None
        else:
            # Cleanup previous messes
            try:
                currtime = OATime().now
                prevrpcs = OAG_RpcDiscoverable([self.infname], 'by_rpcinfname_idx')
                number_active = 0
                for rpc in prevrpcs:
                    delta = currtime - rpc.heartbeat
                    if delta < datetime.timedelta(seconds=getenv().rpctimeout):
                        number_active += 1
                    else:
                        if self.logger.RPC:
                            print "[%s] Removing stale discoverable [%s]-[%d], last HA at [%s], %s seconds ago"\
                                   % (rpc.id, rpc.type, rpc.stripe, rpc.heartbeat, delta)
                        rpc.delete()

                # Is there already an active subscription there?
                if number_active > 0:
                    if not self.fanout:
                        message = "[%s] Active OAG already on inferred name [%s], last HA at [%s], %s seconds ago"\
                                   % (rpc.id, rpc.rpcinfname, rpc.heartbeat, delta)
                        if self.logger.RPC:
                            print message
                        raise OAError(message)
            except OAGraphRetrieveError as e:
                pass

            # Create new database entry
            self._rpc_discovery =\
                OAG_RpcDiscoverable(logger=self.logger,
                                    rpc=False,
                                    heartbeat=self._rpc_heartbeat)\
                .create({
                    'rpcinfname' : self.infname,
                    'stripe'     : 0,
                    'url'        : self.oagurl,
                    'type'       : self.__class__.__name__,
                    'envid'      : getenv().envid,
                    'heartbeat'  : currtime
                }).next()

            self._rpc_discovery.start_heartbeat()

    @property
    def fanout(self): return False

    @property
    def id(self):
        try:
            return self._cframe[self.dbpkname]
        except:
            return None

    def init_state_dbschema(self):
        with OADao(self.dbcontext, cdict=False) as dao:
            with dao.cur as cur:
                # Check that dbcontext schema exists
                self.SQLexec(cur, self.SQL['admin']['schema'], parms=[self.dbcontext])
                check = cur.fetchall()
                if len(check)==0:
                    if self.logger.SQL:
                        print "Creating missing schema [%s]" % self.dbcontext
                    self.SQLexec(cur, self.SQL['admin']['mkschema'])

                # Check for presence of table
                try:
                    self.SQLexec(cur, self.SQL['admin']['table'])
                except psycopg2.ProgrammingError as e:
                    dao.commit()
                    if ('relation "%s.%s" does not exist' % (self.dbcontext, self.dbtable)) in str(e):
                        if self.logger.SQL:
                            print "Creating missing table [%s]" % self.dbtable
                        self.SQLexec(cur, self.SQL['admin']['mktable'])
                        self.SQLexec(cur, self.SQL['admin']['table'])

                # Check for table schema integrity
                oag_columns     = sorted(self.dbstreams.keys())
                db_columns_ext  = [desc[0] for desc in cur.description if desc[0][0] != '_']
                db_columns_reqd = [self.db_oag_mapping[k] for k in sorted(self.db_oag_mapping.keys())]

                dropped_cols = [ dbc for dbc in db_columns_ext if dbc not in db_columns_reqd ]
                if len(dropped_cols)>0:
                    raise OAGraphIntegrityError("Dropped columns %s detected, cannot initialize" % dropped_cols)

                add_cols = [rdb for rdb in db_columns_reqd if rdb not in db_columns_ext]
                if len(add_cols)>0:
                    if self.logger.SQL:
                        print "Adding new columns %s to [%s]" % (add_cols, self.dbtable)
                    add_col_clauses = []
                    for i, col in enumerate(oag_columns):
                        if db_columns_reqd[i] in add_cols:
                            if oag_columns[i] != db_columns_reqd[i]:
                                subnode = self.dbstreams[col][0](logger=self.logger, rpc=False)
                                add_clause = "ADD COLUMN %s int NOT NULL references %s.%s(%s)"\
                                             % (subnode.dbpkname[1:],
                                                subnode.dbcontext,
                                                subnode.dbtable,
                                                subnode.dbpkname)
                            else:
                                add_clause = "ADD COLUMN %s %s NOT NULL" % (col, self.dbstreams[col][0])
                            add_col_clauses.append(add_clause)

                    addcol_sql = self.SQLpp("ALTER TABLE {0}.{1} %s") % ",".join(add_col_clauses)
                    self.SQLexec(cur, addcol_sql)

                for idx, idxinfo in self.dbindices.items():
                    col_sql     = ','.join(map(lambda x: self.db_oag_mapping[x], idxinfo[0]))
                    unique_sql  = 'UNIQUE' if idxinfo[1] else str()
                    partial_sql = 'WHERE %s' % idxinfo[2] if idxinfo[2] else str()
                    exec_sql    = self.SQL['admin']['mkindex'] % (unique_sql, idx, col_sql, partial_sql)
                    self.SQLexec(cur, exec_sql)

            dao.commit()

    def init_state_rpc(self):
        # Intiailize reqs
        if not self._rpc_init_done:
            self._rpcsem = BoundedSemaphore(1)
            with self._rpcsem:
                self._rpcrtr = OAGRPC_RTR_Requests(self)
                self.rpcrtr.start()
                self._glets.append(spawn(self.__cb_init_state_rpc))
                gevent.sleep(0)

                # Initialize REQ array
                self._rpcreqs = {}

                # Avoid double RPC initialization
                self._rpc_init_done = True

                # RPC discovery isn't actually on yet
                self._rpc_discovery = None

                # Some things probably shouldn't be sent over rpc
                self._rpc_stop_list = [
                    'logger',
                    'rpcrtr',
                    'discoverable'
                ] + [attr for attr in dir(self) if attr[0]=='_']

    @classmethod
    def is_oagnode(cls, stream):
        streaminfo = cls.dbstreams[stream][0]
        if type(streaminfo).__name__=='type':
            return 'OAGraphRootNode' in [x.__name__ for x in inspect.getmro(streaminfo)]
        else:
            return False

    @property
    def oagurl(self): return self.rpcrtr.addr

    @property
    def proxyurl(self): return self._proxy_url

    @property
    def rpcreqs(self):
        rpcreq = self._rpcreqs

    def signal_surrounding_nodes(self, stream, currval, newval=None, initmode=False):

        if initmode and currval:
            if self.is_oagnode(stream):
                if self.logger.RPC:
                    print "[%s] Connecting to new stream [%s] in initmode" % (stream, currval.rpcrtr.id)
                reqcls(self).register(currval.rpcrtr, stream)
            return

        if stream[0] != '_':
            invalidate_upstream = False

            # Handle oagprops
            if self.is_oagnode(stream):
                if newval:
                    # Update oagcache
                    self._oagcache[stream] = newval
                    # Regenerate connections to surrounding nodes
                    if currval is None:
                        if self.logger.RPC:
                            print "[%s] Connecting to new stream [%s] in non-initmode" % (stream, newval.rpcrtr.id)
                        reqcls(self).register(newval.rpcrtr, stream)
                    else:
                        if currval != newval:
                            if self.logger.RPC:
                                print "[%s] Detected changed stream [%s]->[%s]" % (stream,
                                                                                   currval.rpcrtr.id,
                                                                                   newval.rpcrtr.id)
                            if currval:
                                reqcls(self).deregister(currval.rpcrtr, stream)
                            reqcls(self).register(newval.rpcrtr, stream)
                            try:
                                self._cframe[self.db_oag_mapping[stream]]=newval.id
                            except KeyError:
                                pass
                            invalidate_upstream = True
            else:
                if currval and currval != newval:
                    invalidate_upstream  = True

            if invalidate_upstream:
                if len(self._rpcreqs)>0:
                    if self.logger.RPC:
                        print "[%s] Informing upstream of invalidation [%s]->[%s]" % (stream, currval, newval)
                    for addr, stream_to_invalidate in self._rpcreqs.items():
                        reqcls(self).invalidate(addr, stream_to_invalidate)

    @property
    def sql_local(self): return {}

    @property
    def SQL(self):

        # Default SQL defined for all tables
        default_sql = {
            "read" : {
              "id"       : self.SQLpp("""
                  SELECT *
                    FROM {0}.{1}
                   WHERE {2}=%s
                ORDER BY {2}"""),

            },
            "update" : {
              "id"       : self.SQLpp("""
                  UPDATE {0}.{1}
                     SET %s
                   WHERE {2}=%s""")
            },
            "insert" : {
              "id"       : self.SQLpp("""
             INSERT INTO {0}.{1}(%s)
                  VALUES (%s)
               RETURNING {2}""")
            },
            "delete" : {
              "id"       : self.SQLpp("""
             DELETE FROM {0}.{1}
                   WHERE {2}=%s""")
            },
            "admin"  : {
              "fkeys"    : self.SQLpp("""
                  SELECT tc.constraint_name,
                         kcu.column_name as id,
                         kcu.constraint_schema as schema,
                         tc.table_name as table,
                         ccu.table_schema as points_to_schema,
                         ccu.table_name as points_to_table_name,
                         ccu.column_name as points_to_id
                    FROM information_schema.table_constraints as tc
                         INNER JOIN information_schema.key_column_usage as kcu
                             ON tc.constraint_name=kcu.constraint_name
                         INNER JOIN information_schema.constraint_column_usage as ccu
                             ON ccu.constraint_name = tc.constraint_name
                   WHERE constraint_type = 'FOREIGN KEY'
                         AND ccu.table_schema='{0}'
                         AND ccu.table_name='{1}'"""),
              "mkindex"  : self.SQLpp("""
                 CREATE %s INDEX IF NOT EXISTS {1}_%s ON {0}.{1} (%s) %s"""),
              "mkschema" : self.SQLpp("""
                 CREATE SCHEMA {0}"""),
              "mktable"  : self.SQLpp("""
                  CREATE table {0}.{1}({2} serial primary key)"""),
              "schema"   : self.SQLpp("""
                  SELECT 1
                    FROM information_schema.schemata
                   WHERE schema_name=%s"""),
              "table"    : self.SQLpp("""
                  SELECT *
                    FROM {0}.{1}
                   WHERE 1=0""")
            }
        }

        # Add in id retrieval for oagprops
        for stream, streaminfo in self.dbstreams.items():
            if self.is_oagnode(stream):
                stream_sql_key = 'by_'+stream
                stream_sql     = td("""
                  SELECT *
                    FROM {0}.{1}
                   WHERE {2}=%s
                ORDER BY {3}""").format(self.dbcontext, self.dbtable, streaminfo[0].dbpkname[1:], self.dbpkname)
                default_sql['read'][stream_sql_key] = stream_sql

        # Add in other indices
        for index, idxinfo in self.dbindices.items():
            index_sql = td("""
                  SELECT *
                    FROM {0}.{1}
                   WHERE %s
                ORDER BY {2}""").format(self.dbcontext, self.dbtable, self.dbpkname)
            default_sql['read']['by_'+index] = index_sql % ' AND '.join(["{0}=%s".format(f) for f in idxinfo[0]])

        # Add in user defined SQL
        for action, sqlinfo in self.sql_local.items():
            for index, sql in sqlinfo.items():
                default_sql[action][index] = sql

        return default_sql

    def SQLpp(self, SQL):
        """Pretty prints SQL and populates schema{0}.table{1} and its primary
        key{2} in given SQL string"""
        return td(SQL.format(self.dbcontext, self.dbtable, self.dbpkname))

    def __cb_init_state_rpc(self):

        rpc_dispatch = {
            'deregister'     : self.rpcrtr.proc_deregister,
            'invalidate'     : self.rpcrtr.proc_invalidate,
            'register'       : self.rpcrtr.proc_register,
            'register_proxy' : self.rpcrtr.proc_register_proxy,
            'getstream'      : self.rpcrtr.proc_getstream,
        }

        if self.logger.RPC:
            print "[%s:rtr] Listening for RPC requests [%s]" % (self.rpcrtr.id, self.__class__.__name__)

        while True:
            (sender, payload) = self.rpcrtr._recv()
            self.rpcrtr._send(sender, rpc_dispatch[payload['action']](payload))

    def __enter__(self):
        self.discoverable = True
        return self

    def __exit__(self, type, value, traceback):
        self.discoverable = False

    def __getattribute__(self, attr):
        def objattr(stream):
            """returns local-accessible attributes"""
            return object.__getattribute__(self, stream)

        try:
            if objattr('_proxy_mode'):
                if attr in objattr('_proxy_oags'):
                    if objattr('logger').RPC:
                        print "[%s] proxying request for [%s] to [%s]" % (attr, attr, objattr('_proxy_url'))
                    payload = reqcls(self).getstream(objattr('_proxy_url'), attr)['payload']
                    if payload['value']:
                        if payload['type'] == 'redirect':
                            for cls in OAGraphRootNode.__subclasses__()+OAG_RootNode.__subclasses__():
                                if cls.__name__==payload['class']:
                                    return cls(initurl=payload['value'], logger=objattr('logger'))
                        else:
                            return payload['value']
                    else:
                        raise AttributeError("[%s] does not exist" % attr)
                else:
                    raise AttributeError("[%s] is not an allowed proxy attribute" % attr)
        except AttributeError:
            pass

        return object.__getattribute__(self, attr)

    def __init__(self,
                 clauseprms=None,
                 indexprm='id',
                 initprms={},
                 initurl=None,
                 extcur=None,
                 logger=OALog(),
                 rpc=True,
                 heartbeat=True):

        # Alphabetize
        if type(clauseprms).__name__=='dict':
            tmpcprms = clauseprms.keys()
            tmpcprms.sort()
            clauseprms = [clauseprms[prm] for prm in tmpcprms]

        self._proxy_mode     = False

        self._rpc_init_done  = False
        self._rpc_heartbeat  = heartbeat

        self._cframe         = {}
        self._fkframe        = {}
        self._rawdata        = None
        self._rawdata_window = None
        self._oagcache       = {}
        self._clauseprms     = clauseprms
        self._indexparm      = indexprm
        self._extcur         = extcur
        self._logger         = logger
        self._glets          = []

        if initurl:
            self.init_state_rpc()
            self._proxy_oags = reqcls(self).register_proxy(initurl, 'proxy')['payload']
            self._proxy_mode = True
            self._proxy_url  = initurl
        else:
            self._reset_oagprops()
            attrs = self._set_attrs_from_userprms(initprms)
            self._set_cframe_from_attrs(attrs)

            self.init_state_dbschema()
            self.init_state_oag()

            if rpc:
                self.init_state_rpc()
                for stream in self.dbstreams:
                    currval = getattr(self, stream, None)
                    self.signal_surrounding_nodes(stream, currval, initmode=True)

    def __setattr__(self, attr, newval):

        # Setting values on a proxy OAG is nonsensical
        if getattr(self, '_proxy_mode', None) == True and attr in getattr(self, '_proxy_oags', []):
            raise OAError("Cannot set value on a proxy OAG")

        # Stash existing value
        currval = getattr(self, attr, None)

        # Set new value
        super(OAG_RootNode, self).__setattr__(attr, newval)

        # Tell the world
        if getattr(self, '_rpc_init_done', None) and attr not in getattr(self, '_rpc_stop_list', []):
            self.signal_surrounding_nodes(attr, currval, newval)

    def _refresh_from_cursor(self, cur):
        self.SQLexec(cur, self.SQL['admin']['fkeys'])
        self._fkframe = cur.fetchall()
        super(OAG_RootNode, self)._refresh_from_cursor(cur)

    def _reset_oagprops(self):
        """Maintain list of oagprops that have been set"""
        curr_proplist = getattr(self.__class__, "oagproplist", [])
        for prop in curr_proplist:
            if getattr(self.__class__, prop, None) is not None:
                delattr(self.__class__, prop)
        new_proplist = [stream for stream in self.dbstreams if self.is_oagnode(stream)]
        for prop in new_proplist:
            self._set_oagproplist(prop)

    def _set_attrs_from_cframe(self):
        oag_db_mapping = {self.db_oag_mapping[k]:k for k in self.db_oag_mapping}

        # Blank everything if _cframe isn't set
        if len(self._cframe)==0:
            for stream in self.dbstreams:
                setattr(self, stream, None)
            return

        # Set dbstream attributes
        for stream, streaminfo in self._cframe.items():
            self._set_oagprop(stream, streaminfo)

        # Set forward lookup attributes
        for fk in self._fkframe:
            classname = "OAG_"+inflection.camelize(fk['table'])
            for cls in OAGraphRootNode.__subclasses__()+OAG_RootNode.__subclasses__():
                if cls.__name__==classname:
                    stream = fk['table']
                    def fget(obj,
                             cls=cls,
                             clauseprms=[getattr(self, fk['points_to_id'], None)],
                             indexprm='by_'+{cls.db_oag_mapping[k]:k for k in cls.db_oag_mapping}[fk['id']],
                             logger=self.logger):
                        return cls(clauseprms, indexprm, logger=self.logger)
                    fget.__name__ = stream
                    self._set_oagproplist(stream)
                    setattr(self.__class__, stream, oagprop(fget))

    def _set_attrs_from_userprms(self, userprms):
        missing_streams = []
        invalid_streams = []
        processed_streams = {}

        # blank everything
        for oagkey in self.dbstreams.keys():
            setattr(self, oagkey, None)

        if len(userprms)==0:
            return []

        invalid_streams = [ s for s in userprms.keys() if s not in self.dbstreams.keys() ]
        if len(invalid_streams)>0:
            raise OAGraphIntegrityError("Invalid update stream(s) detected %s" % invalid_streams)

        processed_streams = { s:userprms[s] for s in userprms.keys() if s not in invalid_streams }
        for stream, streaminfo in processed_streams.items():
            setattr(self, stream, streaminfo)
            self._set_oagprop(stream, streaminfo, streamform='oag')

        return processed_streams.keys()

    def _set_cframe_from_attrs(self, attrs, fullhouse=False):
        cframe_tmp = {}
        missing_streams = []

        all_streams = self.dbstreams.keys()
        if len(self._cframe) > 0:
            all_streams.append(self.dbpkname)

        for oagkey in all_streams:
            cfkey = oagkey
            cfval = getattr(self, oagkey, None)

            # Special handling for indices
            if cfkey[0] == '_':
                cframe_tmp[cfkey] = cfval
                continue

            # Is a value missing for this stream?
            if cfval is None:
                missing_streams.append(oagkey)
                continue

            # Ok, actualy set cframe
            if self.is_oagnode(oagkey):
                cfkey = self.db_oag_mapping[oagkey]
                # this only works if we're in dbpersist mode
                # if there's a key error, we're working in-memory
                try:
                    cfval = cfval.id
                except KeyError:
                    pass
            cframe_tmp[cfkey] = cfval

        if fullhouse:
            if len(missing_streams)>0:
                raise OAGraphIntegrityError("Missing streams detected %s" % missing_streams)

        self._cframe = cframe_tmp

    def _set_oagprop(self, stream, cfval, indexprm='id', streamform='cframe'):

        # primary key: set directly
        if stream[0] == '_':
            setattr(self, stream, self._cframe[stream])
            return

        # Normalize stream name to OAG form
        if streamform == 'cframe':
            db_oag_mapping = {self.db_oag_mapping[k]:k for k in self.db_oag_mapping}
            stream = db_oag_mapping[stream]

        if self.is_oagnode(stream):

            # oagprop: update cache if necessary
            currattr = getattr(self, stream, None)
            if currattr:
                self._oagcache[stream] = currattr

            # oagprop: actually set it
            def oagpropfn(obj,
                     cls=self.dbstreams[stream][0],
                     clauseprms=[cfval],
                     indexprm=indexprm,
                     logger=self.logger,
                     currattr=currattr):
                # Do not instantiate objects unnecessarily
                if currattr:
                    try:
                        if currattr.id == clauseprms[0]:
                            return currattr
                    except KeyError:
                        # We're dealing with in-memory OAGs, just return
                        return currattr
                # All else has failed, instantiate a new object
                return cls(clauseprms, indexprm, logger=logger)
            oagpropfn.__name__ = stream

            setattr(self.__class__, stream, oagprop(oagpropfn))
        else:
            setattr(self, stream, cfval)

    def _set_oagproplist(self, stream):
        """Maintain list of oagprops that have been set"""
        oagproplist = getattr(self.__class__, "oagproplist", None)
        if oagproplist is None:
            oagproplist = []
        if stream not in oagproplist:
            oagproplist.append(stream)
            setattr(self.__class__, 'oagproplist', oagproplist)

class OAG_RpcDiscoverable(OAG_RootNode):
    @property
    def is_unique(self): return False

    @property
    def dbcontext(self): return "openarc"

    @staticproperty
    def dbindices(cls):
        return {
        #Index Name------------Elements------Unique-------Partial
        'rpcinfname_idx' : [ ['rpcinfname'], True  ,      None  ]
    }

    @staticproperty
    def dbstreams(cls): return {
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
                    print "[%s] Underlying db controller row is missing for [%s]-[%d], exiting" % (self.id, self.rpcinfname, self.stripe)
                sys.exit(1)

            # Did environment change?
            if self.envid != rpcdisc.envid:
                if self.logger.RPC:
                    print "[%s] Environment changed from [%s] to [%s], exiting" % (self.id, self.envid, rpcdisc.envid)
                sys.exit(1)

            self.heartbeat = OATime().now
            if self.logger.RPC:
                print "[%s] heartbeat %s" % (self.id, self.heartbeat)
            self.update()
            gevent.sleep(getenv().rpctimeout)

    def start_heartbeat(self):
        if self._rpc_heartbeat:
            if self.logger.RPC:
                print "[%s] Starting heartbeat greenlet" % (self.id)
            self._glets.append(spawn(self.__cb_heartbeat))
