#!/usr/bin/env python2.7

import base64
import collections
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
from zmq.utils.garbage import gc
_zmqctx = zmq.Context()
_zmqctx.max_sockets = 32768
gc.context = _zmqctx

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
            return obj.cache.match(self.fget.func_name)
        except:
            oagprop = self.fget(obj)
            if oagprop is not None:
                obj.cache.put(self.fget.func_name, oagprop)
            return oagprop

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
                print "[%s:req] Sending RPC request with payload [%s] to [%s]" % (self._oag.rpc.router.id, payload, toaddr)

            self._ctxsoc.send(msgpack.dumps(payload))
            reply = self._ctxsoc.recv()

            rpcret = msgpack.loads(reply)
            if self._oag.logger.RPC:
                print "[%s:req] Received reply [%s]" % (self._oag.rpc.router.id, rpcret)
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
        self._ctxsoc  = _zmqctx.socket(zmqtype)
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
        self._oag.rpc.registration_invalidate(args['addr'])

    @OAGRPC.rpcprocfn
    def proc_getstream(self, ret, args):
        attr = getattr(self._oag, args['stream'], None)
        if isinstance(attr, OAG_RootNode):
            ret['payload']['type']  = 'redirect'
            ret['payload']['value'] = attr.rpc.router.addr
            ret['payload']['class'] = attr.__class__.__name__
        else:
            ret['payload']['type']  = 'value'
            ret['payload']['value'] = attr

    @OAGRPC.rpcprocfn
    def proc_invalidate(self, ret, args):

        invstream = args['stream']

        if self._oag.logger.RPC:
            print '[%s:rtr] invalidation signal received' % self._oag.rpc.router.id

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

        rawprops = self._oag.streams.keys()\
                   + [p for p in dir(self._oag.__class__) if isinstance(getattr(self._oag.__class__, p), property)]\
                   + [p for p in dir(self._oag.__class__) if isinstance(getattr(self._oag.__class__, p), oagprop)]\
                   + getattr(self._oag.__class__, 'oagproplist', [])

        ret['payload'] = [p for p in list(set(rawprops)) if p not in self._oag.rpc.stoplist]

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

reqcls = OAGRPC_REQ_Requests

class OAG_CacheProxy(object):
    """Responsible for manipulation of relational data frame"""
    def __init__(self, oag):
        self._oag = oag

        # Cache storage object.
        self._oagcache ={}

    def clear(self):
        self._oagcache = {}

    def clone(self, src):
        self._oagcache   = list(src.oagache._oagcache)

    def invalidate(self, invstream):
        # - filter out all non-dbstream items: calcs can no longer be trusted as node as been invalidated
        tmpoagcache = {oag:self._oagcache[oag] for oag in self._oagcache if oag in self._oag.streams.keys()}
        # - filter out invalidated downstream node
        tmpoagcache = {oag:tmpoagcache[oag] for oag in tmpoagcache if oag != invstream}

        self._oagcache = tmpoagcache

    def match(self, stream):
        return self._oagcache[stream]

    def put(self, stream, new_value):
        self._oagcache[stream] = new_value

    @property
    def state(self):

        return self._oagcache

class OAG_DbSchemaProxy(object):
    def __init__(self, dbproxy):
        self._dbproxy = dbproxy

    def init(self):
        dbp = self._dbproxy
        oag = dbp._oag

        with OADao(oag.context, cdict=False) as dao:
            with dao.cur as cur:
                # Check that context schema exists
                dbp.SQLexec(cur, dbp.SQL['admin']['schema'])
                check = cur.fetchall()
                if len(check)==0:
                    if oag.logger.SQL:
                        print "Creating missing schema [%s]" % oag.context
                    dbp.SQLexec(cur, dbp.SQL['admin']['mkschema'])
                    dao.commit()

                # Check for presence of table
                try:
                    dbp.SQLexec(cur, dbp.SQL['admin']['table'])
                except psycopg2.ProgrammingError as e:
                    dao.commit()
                    if ('relation "%s.%s" does not exist' % (oag.context, oag.dbtable)) in str(e):
                        if oag.logger.SQL:
                            print "Creating missing table [%s]" % oag.dbtable
                        dbp.SQLexec(cur, dbp.SQL['admin']['mktable'])
                        dbp.SQLexec(cur, dbp.SQL['admin']['table'])

                # Check for table schema integrity
                oag_columns     = sorted(oag.streams.keys())
                db_columns_ext  = [desc[0] for desc in cur.description if desc[0][0] != '_']
                db_columns_reqd = [oag.stream_db_mapping[k] for k in sorted(oag.stream_db_mapping.keys())]

                dropped_cols = [ dbc for dbc in db_columns_ext if dbc not in db_columns_reqd ]
                if len(dropped_cols)>0:
                    raise OAGraphIntegrityError("Dropped columns %s detected, cannot initialize" % dropped_cols)

                add_cols = [rdb for rdb in db_columns_reqd if rdb not in db_columns_ext]
                if len(add_cols)>0:
                    if oag.logger.SQL:
                        print "Adding new columns %s to [%s]" % (add_cols, oag.dbtable)
                    add_col_clauses = []
                    for i, col in enumerate(oag_columns):
                        if db_columns_reqd[i] in add_cols:
                            if oag_columns[i] != db_columns_reqd[i]:
                                subnode = oag.streams[col][0](logger=oag.logger, rpc=False).db.schema.init()
                                add_clause = "ADD COLUMN %s int %s references %s.%s(%s)"\
                                             % (subnode.dbpkname[1:],
                                               'NOT NULL' if oag.streams[col][1] else str(),
                                                subnode.context,
                                                subnode.dbtable,
                                                subnode.dbpkname)
                            else:
                                add_clause = "ADD COLUMN %s %s" % (col, oag.streams[col][0])
                                if oag.streams[col][1] is not None:
                                    add_clause = "%s NOT NULL" % add_clause
                            add_col_clauses.append(add_clause)

                    addcol_sql = dbp.SQLpp("ALTER TABLE {0}.{1} %s") % ",".join(add_col_clauses)
                    dbp.SQLexec(cur, addcol_sql)

                for idx, idxinfo in oag.dbindices.items():
                    col_sql     = ','.join(map(lambda x: oag.stream_db_mapping[x], idxinfo[0]))

                    unique_sql  = str()
                    if idxinfo[1]:
                        unique_sql = 'UNIQUE'

                    partial_sql = str()
                    if idxinfo[2]:
                        partial_sql =\
                            'WHERE %s' % ' AND '.join('%s=%s' % (oag.stream_db_mapping[k], idxinfo[2][k]) for k in idxinfo[2].keys())

                    exec_sql    = dbp.SQL['admin']['mkindex'] % (unique_sql, idx, col_sql, partial_sql)
                    dbp.SQLexec(cur, exec_sql)

            dao.commit()
            return oag

    def init_fkeys(self):
        dbp = self._dbproxy
        oag = dbp._oag

        # Don't set props for proxied OAGs, they are passthrough entities
        if oag.rpc.is_proxy:
            return

        with OADao(oag.context) as dao:
            with dao.cur as cur:
                for stream in oag.streams:
                    if oag.is_oagnode(stream):
                        currattr = getattr(oag, stream, None)
                        if currattr:
                            currattr.db.schema.init_fkeys()

                dbp.SQLexec(cur, dbp.SQL['admin']['fkeys'])
                setattr(oag.__class__, '_fkframe', cur.fetchall())
                dbp.refresh(idxreset=False)

class OAG_DbProxy(object):
    """Responsible for manipulation of database"""
    def __init__(self, oag, clauseprms, indexprm):

        # Store reference to outer object
        self._oag = oag

        # Schema is unknown right now
        self._schema = None

        # Intialize search parameters, if any
        if clauseprms:
            if type(clauseprms).__name__ in ['dict']:
                rawprms = [clauseprms[prm] for prm in sorted(clauseprms.keys())]
            elif type(clauseprms).__name__ in ['list', 'tuple']:
                rawprms = clauseprms
            else:
                rawprms = [clauseprms]

            clauseprms = map(lambda x: x.id if isinstance(x, OAG_RootNode) else x, rawprms)

        self._clauseprms     = clauseprms
        self._indexparm      = indexprm

    def clone(self, src):
        self._schema     = src.db.schema
        self._clauseprms = src.db.searchprms
        self._indexparm  = src.db.searchidx

    def create(self, initprms={}):

        self.schema.init()

        attrs = self._oag.propmgr._set_attrs_from_userprms(initprms) if len(initprms)>0 else []
        self._oag.propmgr._set_cframe_from_attrs(attrs, fullhouse=True)

        if self._oag.rdf._rdf is not None:
            raise OAError("Cannot create item that has already been initiated")

        filtered_cframe = {k:self._oag.propmgr._cframe[k] for k in self._oag.propmgr._cframe if k[0] != '_'}
        attrstr    = ', '.join([k for k in filtered_cframe])
        vals       = [filtered_cframe[k] for k in filtered_cframe]
        formatstrs = ', '.join(['%s' for v in vals])
        insert_sql = self.SQL['insert']['id'] % (attrstr, formatstrs)

        if self._oag._extcur is None:
            with OADao(self._oag.context) as dao:
                with dao.cur as cur:
                    self.SQLexec(cur, insert_sql, vals)
                    if self._indexparm == 'id':
                        index_val = cur.fetchall()
                        self._clauseprms = index_val[0].values()
                    self.__refresh_from_cursor(cur)
                    dao.commit()
        else:
            self.SQLexec(self._oag._extcur, insert_sql, vals)
            if self._indexparm == 'id':
                index_val = self._oag._extcur.fetchall()
                self._clauseprms = index_val[0].values()
            self.__refresh_from_cursor(self._oag._extcur)

        # Refresh to set iteridx
        self.refresh()

        self.schema.init_fkeys()

        # Set attrs if this is a unique oag
        if self._oag.is_unique:
            self._oag.propmgr._set_attrs_from_cframe_uniq()

        return self._oag

    def delete(self):

        delete_sql = self.SQL['delete']['id']

        if self._oag._extcur is None:
            with OADao(self._oag.context) as dao:
                with dao.cur as cur:
                    self.SQLexec(cur, delete_sql, [self._oag.id])
                    dao.commit()
        else:
            self.SQLexec(cur, delete_sql, [self._oag.id])

        self.refresh(gotodb=True)

        if self._oag.is_unique:
            self._oag.propmgr._set_attrs_from_cframe_uniq()

        return self

    @property
    def searchidx(self):
        return self._indexparm

    @property
    def searchprms(self):
        return self._clauseprms

    def update(self, updparms={}, norefresh=False):

        attrs = self._oag.propmgr._set_attrs_from_userprms(updparms) if len(updparms)>0 else []
        self._oag.propmgr._set_cframe_from_attrs(attrs)

        self.update_clauseprms()

        member_attrs  = [k for k in self._oag.propmgr._cframe if k[0] != '_']
        index_key     = [k for k in self._oag.propmgr._cframe if k[0] == '_'][0]
        update_clause = ', '.join(["%s=" % attr + "%s"
                                    for attr in member_attrs])
        update_sql    = self.SQL['update']['id']\
                        % (update_clause, getattr(self._oag, index_key, ""))
        update_values = [self._oag.propmgr._cframe[attr] for attr in member_attrs]
        if self._oag._extcur is None:
            with OADao(self._oag.context) as dao:
                with dao.cur as cur:
                    self.SQLexec(cur, update_sql, update_values)
                    if not norefresh:
                        self.__refresh_from_cursor(cur)
                    dao.commit()
        else:
            self.SQLexec(self._oag._extcur, update_sql, update_values)
            if not norefresh:
                self.__refresh_from_cursor(self._oag._extcur)

        if not self._oag.is_unique and len(self._oag.rdf._rdf_window)>0:
            self._oag[self._oag.rdf._rdf_window_index]

        return self._oag

    def update_clauseprms(self):
        # Update search parameteres from prop manager
        index = self._indexparm[3:]
        if index != str():
            try:
                keys = self._oag.dbindices[index][0]
            except KeyError:
                keys = [index]

            new_clauseprms = []
            for i, key in enumerate(keys):
                key = self._oag.stream_db_mapping[key]
                try:
                    new_clauseprms.append(self._oag.propmgr._cframe[key])
                except KeyError:
                    new_clauseprms.append(self._clauseprms[i])

            self._clauseprms = new_clauseprms

    def refresh(self, gotodb=False, idxreset=True):
        """Generally we want to simply reset the iterator; set gotodb=True to also
        refresh instreams from the database"""
        if gotodb is True:
            if self._oag._extcur is None:
                with OADao(self._oag.context) as dao:
                    with dao.cur as cur:
                        self.__refresh_from_cursor(cur)
            else:
                self.__refresh_from_cursor(self._oag._extcur)
            self._oag.cache.clear()

        self._oag.rdf._rdf_window = self._oag.rdf._rdf
        if idxreset:
            self._oag._iteridx = 0
        self._oag.propmgr._set_attrs_from_cframe()
        return self._oag

    def __refresh_from_cursor(self, cur):
        try:
            if type(self.SQL).__name__ == "str":
                self.SQLexec(cur, self.SQL, self._clauseprms)
            elif type(self.SQL).__name__ == "dict":
                self.SQLexec(cur, self.SQL['read'][self._indexparm], self._clauseprms)

            self._oag.rdf._rdf = cur.fetchall()
            self._oag.rdf._rdf_window = self._oag.rdf._rdf

            for predicate in self._oag.rdf._rdf_filter_cache:
                self._oag.filter(predicate, rerun=True)
        except psycopg2.ProgrammingError:
            raise OAGraphRetrieveError("Missing database table")

    @property
    def schema(self):
        if not self._schema:
            self._schema = OAG_DbSchemaProxy(self)
        return self._schema

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
                   WHERE schema_name='{0}'"""),
              "table"    : self.SQLpp("""
                  SELECT *
                    FROM {0}.{1}
                   WHERE 1=0""")
            }
        }

        # Add in id retrieval for oagprops
        for stream, streaminfo in self._oag.streams.items():
            if self._oag.is_oagnode(stream):
                stream_sql_key = 'by_'+stream
                stream_sql     = td("""
                  SELECT *
                    FROM {0}.{1}
                   WHERE {2}=%s
                ORDER BY {3}""").format(self._oag.context, self._oag.dbtable, streaminfo[0].dbpkname[1:], self._oag.dbpkname)
                default_sql['read'][stream_sql_key] = stream_sql

        # Add in other indices
        for index, idxinfo in self._oag.dbindices.items():
            index_sql = td("""
                  SELECT *
                    FROM {0}.{1}
                   WHERE %s
                ORDER BY {2}""").format(self._oag.context, self._oag.dbtable, self._oag.dbpkname)
            where_clauses = []
            for f in idxinfo[0]:
                where_clauses.append("{0}=%s".format(self._oag.stream_db_mapping[f] if self._oag.is_oagnode(f) else f))
            default_sql['read']['by_'+index] = index_sql % ' AND '.join(where_clauses)

        # Add in user defined SQL
        for action, sqlinfo in self._oag.dblocalsql.items():
            for index, sql in sqlinfo.items():
                default_sql[action][index] = sql

        return default_sql

    def SQLexec(self, cur, query, parms=[]):
        if self._oag.logger.SQL:
            print cur.mogrify(query, parms)
        cur.execute(query, parms)

    def SQLpp(self, SQL):
        """Pretty prints SQL and populates schema{0}.table{1} and its primary
        key{2} in given SQL string"""
        return td(SQL.format(self._oag.context, self._oag.dbtable, self._oag.dbpkname))

class OAG_RdfProxy(object):
    """Responsible for manipulation of relational data frame"""
    def __init__(self, oag):
        self._oag = oag

        # All data. Currently can only be set from database.
        self._rdf = None

        # An array of lambdas sequentially used to filter the rdf
        self._rdf_filter_cache = []

        # If this is a slice, it can be used to further filter the rdf
        self._rdf_window_index = 0

        # After
        self._rdf_window = None

    def clone(self, src):
        self._rdf        = list(src.rdf._rdf)
        self._rdf_window = list(src.rdf._rdf_window)
        self._rdf_window_index =\
                                src.rdf._rdf_window_index
        self._rdf_filter_cache =\
                                list(src.rdf._rdf_filter_cache)

    def filter(self, predicate, rerun=False):
        if self._oag.is_unique:
            raise OAError("Cannot filter OAG that is marked unique")

        self._oag.cache.clear()

        self._rdf_window = self._rdf

        if rerun is False:
            self._rdf_filter_cache.append(predicate)

        self._rdf = []
        for i, frame in enumerate(self._rdf):
            self._oag.propmgr._cframe = frame
            self._oag.propmgr._set_attrs_from_cframe()
            if predicate(self._oag):
                self._rdf_window.append(self._rdf[i])

        if len(self._rdf_window)>0:
            self._oag.propmgr._cframe = self._rdf_window[0]
        else:
            self._rdf_window = []
            self._oag.propmgr._cframe = {}

        self._oag.propmgr._set_attrs_from_cframe()

        return self.oag

    def sort(self, key):

        self._rdf.sort(key=lambda x: x[key])
        self._rdf_window = self._rdf
        self._rdf_window_index = None
        self._oag.propmgr._cframe = {}

        return self

class OAG_RpcProxy(object):
    """Manipulates rpc functionality for OAG"""

    def __init__(self, oag, initurl=None, rpc_enabled=True, heartbeat_enabled=True):

        ### Store reference to OAG
        self._oag = oag

        # Greenlets spawned by RPC
        self._glets = []

        ### Spin up rpc infrastructure

        # Imports
        from gevent import spawn, monkey
        from gevent.lock import BoundedSemaphore

        # Is RPC initialization complete?
        self._rpc_init_done = False

        # A very basic question...
        self._rpc_enabled = rpc_enabled

        # Serialize access to RPC
        self._rpcsem = BoundedSemaphore(1)

        # Routes all incoming RPC requests (dead to start with)
        self._rpcrtr = None

        # Registrations received from other OAGs
        self._rpcreqs = {}

        # Holding spot for RPC discoverability - default off
        self._rpc_discovery = None

        # Should you heartbeat?
        self._rpc_heartbeat = heartbeat_enabled

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
                self._glets.append(spawn(self.__cb_init))

                # Force execution of newly spawned greenlets
                gevent.sleep(0)

                # Avoid double RPC initialization
                self._rpc_init_done = True

        # Proxying
        if self._proxy_url:
            self._proxy_mode = True

    def clone(self, src):

        pass

    def discover(self):
        remote_oag =\
            OAG_RpcDiscoverable({
                'rpcinfname' : self._oag.infname
            }, 'by_rpcinfname_idx', rpc=False, logger=self._oag.logger)
        return self._oag.__class__(initurl=remote_oag[0].url)

    @property
    def discoverable(self): return self._rpc_discovery is not None

    @discoverable.setter
    def discoverable(self, value):
        if self._rpc_discovery==value:
            return

        if value is False:
            if self._oag.logger.RPC:
                print "[%s] Killing rpcdisc greenlets [%d]" % (self.router.id, len(self._rpc_discovery.rpc._glets))
            [glet.kill() for glet in self._rpc_discovery.rpc._glets]
            if self._oag.logger.RPC:
                print "[%s] Killing OAG greenlets [%d]" % (self.router.id, len(self._rpc_discovery.rpc._glets))
            [glet.kill() for glet in self._glets]
            gevent.joinall(self._glets+self._rpc_discovery.rpc._glets)
            self._rpc_discovery.db.delete()
            self._rpc_discovery = None
        else:
            # Cleanup previous messes
            try:
                currtime = OATime().now
                prevrpcs = OAG_RpcDiscoverable(self._oag.infname, 'by_rpcinfname_idx', logger=self._oag.logger, heartbeat=False)
                number_active = 0
                for rpc in prevrpcs:
                    delta = currtime - rpc.heartbeat
                    if delta < datetime.timedelta(seconds=getenv().rpctimeout):
                        number_active += 1
                    else:
                        if self._oag.logger.RPC:
                            print "[%s] Removing stale discoverable [%s]-[%d], last HA at [%s], %s seconds ago"\
                                   % (self.router.id, rpc.type, rpc.stripe, rpc.heartbeat, delta)
                        rpc.db.delete()

                # Is there already an active subscription there?
                if number_active > 0:
                    if not self.fanout:
                        message = "[%s] Active OAG already on inferred name [%s], last HA at [%s], %s seconds ago"\
                                   % (self.router.id, rpc.rpcinfname, rpc.heartbeat, delta)
                        if self._oag.logger.RPC:
                            print message
                        raise OAError(message)
                    else:
                        raise OAError("Fanout not implemented yet")
            except OAGraphRetrieveError as e:
                pass

            # Create new database entry
            self._rpc_discovery =\
                OAG_RpcDiscoverable(logger=self._oag.logger,
                                    rpc=False,
                                    heartbeat=self._rpc_heartbeat)\
                .db.create({
                    'rpcinfname' : self._oag.infname,
                    'stripe'     : 0,
                    'url'        : self._oag.oagurl,
                    'type'       : self._oag.__class__.__name__,
                    'envid'      : getenv().envid,
                    'heartbeat'  : currtime
                }).next()

            self._rpc_discovery.start_heartbeat()

    @property
    def fanout(self): return False

    def distribute_stream_change(self, stream, currval, newval=None, initmode=False):

        if not self._rpc_enabled:
            return

        ###### Don't distribute if...
        # Attribute is internal or protected
        if stream[0] == '_':
            return

        # RPC isn't fully initialized
        if not self._rpc_init_done:
            return

        ###### RPC eligibility has been established

        # Flag driving whether or not to invalidate upstream nodes
        invalidate_upstream = False

        # Handle oagprops
        if self._oag.is_oagnode(stream):
            if newval:
                # Update oagcache
                self._oag.cache.put(stream, newval)
                # Update the oagprop
                self._oag.propmgr._set_oagprop(stream, newval.id, streamform='oag')
                # Regenerate connections to surrounding nodes
                if currval is None:
                    if self._oag.logger.RPC:
                        print "[%s] Connecting to new stream [%s] in non-initmode" % (stream, newval.rpc.router.id)
                    reqcls(self._oag).register(newval.rpc.router, stream)
                else:
                    if currval != newval:
                        if self._oag.logger.RPC:
                            print "[%s] Detected changed stream [%s]->[%s]" % (stream,
                                                                               currval.rpc.router.id,
                                                                               newval.rpc.router.id)
                        if currval:
                            reqcls(self._oag).deregister(currval.rpc.router, stream)
                        reqcls(self._oag).register(newval.rpc.router, stream)
                        try:
                            self._oag.propmgr._cframe[self._oag.stream_db_mapping[stream]]=newval.id
                        except KeyError:
                            pass
                        invalidate_upstream = True
        else:
            if currval and currval != newval:
                invalidate_upstream  = True

        if invalidate_upstream:
            if len(self._rpcreqs)>0:
                if self._oag.logger.RPC:
                    print "[%s] Informing upstream of invalidation [%s]->[%s]" % (stream, currval, newval)
                for addr, stream_to_invalidate in self._rpcreqs.items():
                    reqcls(self._oag).invalidate(addr, stream_to_invalidate)

    @property
    def is_init(self):

        return getattr(self, '_rpc_init_done', False)

    @property
    def is_heartbeat(self):

        return self._rpc_heartbeat

    @property
    def is_proxy(self):

        return self._proxy_mode

    @property
    def proxied_url(self):

        return self._proxy_url

    @property
    def proxied_oags(self):

        return self._proxy_oags

    @proxied_oags.setter
    def proxied_oags(self, oag_array):

        self._proxy_oags = oag_array

    @property
    def router(self):
        if not self._rpc_enabled:
            raise OAError("This OAG is not RPC enabled")
        return self._rpcrtr

    def register_with_surrounding_nodes(self):
        if not self._rpc_enabled:
            return

        for stream in self._oag.streams:
            node = getattr(self._oag, stream, None)
            if node and self._oag.is_oagnode(stream):
                if self._oag.logger.RPC:
                    print "[%s] Connecting to new stream [%s] in initmode" % (stream, node.rpc.router.id)
                reqcls(self._oag).register(node.rpc.router, stream)

    @property
    def registrations(self):
        return self._rpcreqs

    def registration_add(self, registering_oag_addr, registering_stream):
        self._rpcreqs[registering_oag_addr] = registering_stream

    def registration_invalidate(self, deregistering_oag_addr):
        self._rpcreqs = {rpcreq:self._rpcreqs[rpcreq] for rpcreq in self._rpcreqs if rpcreq != deregistering_oag_addr}

    @property
    def stoplist(self):
        return self._rpc_stop_list

    #### Callbacks
    def __cb_init(self):

        rpc_dispatch = {
            'deregister'     : self._rpcrtr.proc_deregister,
            'invalidate'     : self._rpcrtr.proc_invalidate,
            'register'       : self._rpcrtr.proc_register,
            'register_proxy' : self._rpcrtr.proc_register_proxy,
            'getstream'      : self._rpcrtr.proc_getstream,
        }

        if self._oag.logger.RPC:
            print "[%s:rtr] Listening for RPC requests [%s]" % (self._rpcrtr.id, self._oag.__class__.__name__)

        while True:
            (sender, payload) = self._rpcrtr._recv()
            self._rpcrtr._send(sender, rpc_dispatch[payload['action']](payload))

class OAG_PropProxy(object):
    """Manipulates properties"""
    def __init__(self, oag):

        # OAG whose properties are being managed
        self._oag           = oag

        # Mutuable pointer to a row in OAG's RDF
        self._cframe        = {}

        # Other convenience functions
        self.cls            = self._oag.__class__
        self.cls.current_id = getattr(self.cls, 'current_id', str())
        self.oagid          = self._oag.oagid

        oagprofiles = getattr(self.cls, 'oagprofiles', collections.OrderedDict())
        try:
            profile = oagprofiles[self.oagid]
        except KeyError:
            oagprofiles[self.oagid] = collections.OrderedDict()

        setattr(self.cls, 'oagprofiles', oagprofiles)
        self.profile_set(self.oagid)

    def add_oagprop(self, stream, oagprop):
        self.profile_set(self.oagid)
        setattr(self.cls, stream, oagprop)
        self.cls.oagprofiles[self.oagid][stream] = oagprop

    def clear_all(self):
        for stream in self._oag.streams:
            if self._oag.is_oagnode(stream):
                setattr(self._oag.__class__, stream, None)
            else:
                setattr(self._oag, stream, None)

    def clone(self, src):

        self._cframe = dict(src.propmgr._cframe)

    def profile_deregister(self, obj):
        del(self.cls.oagprofiles[obj.oagid])
        self.cls.current_id = str()

    def profile_set(self, obj_id):
        if obj_id == self.cls.current_id:
            # redundant call, don't do anything
            return
        else:
            # change detected, store and blank old oagprops, hydrate set new current_id
            try:
                current_profile = self.cls.oagprofiles[self.cls.current_id]
                for stream, streaminfo in current_profile.items():
                    current_profile[stream] = getattr(self.cls, stream, None)
                    delattr(self.cls, stream)
            except KeyError as e:
                pass

            try:
                new_profile = self.cls.oagprofiles[obj_id]
                for stream, streaminfo in new_profile.items():
                    setattr(self.cls, stream, streaminfo)
            except KeyError as e:
                print e.message
                print "This should never, ever happen"

            # Set up next invocation
            setattr(self.cls, 'current_id', obj_id)

    def _set_attrs_from_cframe(self):

        # Blank everything if _cframe isn't set
        if len(self._cframe)==0:
            for stream in self._oag.streams:
                setattr(self._oag, stream, None)
            return

        # Set dbstream attributes
        for stream, streaminfo in self._cframe.items():
            self._set_oagprop(stream, streaminfo)

        # Set forward lookup attributes
        for fk in self._oag.__class__._fkframe:
            classname = "OAG_"+inflection.camelize(fk['table'])
            for cls in OAG_RootNode.__subclasses__():
                if cls.__name__==classname:
                    stream = fk['table']
                    def fget(obj,
                             cls=cls,
                             clauseprms=[getattr(self._oag, fk['points_to_id'], None)],
                             indexprm='by_'+{cls.stream_db_mapping[k]:k for k in cls.stream_db_mapping}[fk['id']],
                             logger=self._oag.logger):
                        return cls(clauseprms, indexprm, logger=self._oag.logger)
                    fget.__name__ = stream
                    self.add_oagprop(stream, oagprop(fget))

    def _set_attrs_from_cframe_uniq(self):
        if len(self._oag.rdf._rdf_window) > 1:
            raise OAGraphIntegrityError("Graph object indicated unique, but returns more than one row from database")

        if len(self._oag.rdf._rdf_window) == 1:
            self._cframe = self._oag.rdf._rdf_window[0]
        else:
            self._cframe = []

        self._set_attrs_from_cframe()

    def _set_attrs_from_userprms(self, userprms):
        missing_streams = []
        invalid_streams = []
        processed_streams = {}

        # blank everything
        for oagkey in self._oag.streams.keys():
            setattr(self._oag, oagkey, None)

        if len(userprms)==0:
            return []

        invalid_streams = [ s for s in userprms.keys() if s not in self._oag.streams.keys() ]
        if len(invalid_streams)>0:
            raise OAGraphIntegrityError("Invalid update stream(s) detected %s" % invalid_streams)

        processed_streams = { s:userprms[s] for s in userprms.keys() if s not in invalid_streams }
        for stream, streaminfo in processed_streams.items():
            setattr(self._oag, stream, streaminfo)
            self._set_oagprop(stream, streaminfo, streamform='oag')

        return processed_streams.keys()

    def _set_cframe_from_attrs(self, attrs, fullhouse=False):
        cframe_tmp = {}
        raw_missing_streams = []

        all_streams = self._oag.streams.keys()
        if len(self._cframe) > 0:
            all_streams.append(self._oag.dbpkname)

        for oagkey in all_streams:

            # Special handling for indices
            if oagkey[0] == '_':
                cframe_tmp[oagkey] = getattr(self._oag, oagkey, None)
                continue

            cfkey = oagkey
            if self._oag.is_oagnode(oagkey):
                cfkey = self._oag.stream_db_mapping[oagkey]
            cfval = getattr(self._oag, oagkey, None)

            # Special handling for nullable items
            if type(self._oag.streams[oagkey][0])!=str\
               and self._oag.streams[oagkey][1] is False:
                cframe_tmp[cfkey] = cfval.id if cfval else None
                continue

            # Is a value missing for this stream?
            if cfval is None:
                raw_missing_streams.append(oagkey)
                continue

            # Ok, actualy set cframe
            if self._oag.is_oagnode(oagkey):
                # this only works if we're in dbpersist mode
                # if there's a key error, we're working in-memory
                try:
                    cfval = cfval.id
                except KeyError:
                    pass
            cframe_tmp[cfkey] = cfval

        if fullhouse:
            missing_streams = []
            for rms in raw_missing_streams:
                if self._oag.is_oagnode(rms):
                    missing_streams.append(rms)
                else:
                    if self._oag.streams[rms][1] is not None:
                        missing_streams.append(rms)
            if len(missing_streams)>0:
                raise OAGraphIntegrityError("Missing streams detected %s" % missing_streams)

        self._cframe = cframe_tmp

    def _set_oagprop(self, stream, cfval, indexprm='id', streamform='cframe'):

        # primary key: set directly
        if stream[0] == '_':
            setattr(self._oag, stream, self._cframe[stream])
            return

        # Normalize stream name to OAG form
        if streamform == 'cframe':
            db_stream_mapping = {self._oag.stream_db_mapping[k]:k for k in self._oag.stream_db_mapping}
            stream = db_stream_mapping[stream]

        if self._oag.is_oagnode(stream):

            # oagprop: update cache if necessary
            try:
                currattr = getattr(self._oag, stream, None)
                if currattr is None:
                    if self._oag.streams[stream][1] is False and cfval:
                        currattr = self._oag.streams[stream][0](cfval, indexprm, logger=self._oag.logger)
                        if not currattr.is_unique:
                            currattr = currattr[-1]
            except OAGraphRetrieveError:
                currattr = None

            if currattr:
                self._oag.cache.put(stream, currattr)

            # oagprop: actually set it
            def oagpropfn(obj,
                          stream=stream,
                          streaminfo=self._oag.streams[stream],
                          clauseprms=[cfval],
                          indexprm=indexprm,
                          logger=self._oag.logger,
                          currattr=currattr):
                # Do not instantiate objects unnecessarily
                if currattr:
                    try:
                        if currattr == clauseprms[0]:
                            return currattr
                        if currattr.id == clauseprms[0]:
                            return currattr
                    except KeyError:
                        # We're dealing with in-memory OAGs, just return
                        return currattr
                elif streaminfo[1] is False:
                    return currattr
                else:
                    newattr = streaminfo[0](clauseprms, indexprm, logger=logger)
                    if not newattr.is_unique:
                        newattr = newattr[-1]
                    return newattr
            oagpropfn.__name__ = stream

            self.add_oagprop(stream, oagprop(oagpropfn))
        else:
            setattr(self._oag, stream, cfval)

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
        return [k for k, v in cls.streams.items()]

    @staticproperty
    def is_unique(cls): return False

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
                    schema[stream] = streaminfo[0].dbpkname[1:]
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
        return hashlib.sha256(str().join([str(getattr(self, k, ""))
                                          for k in self.infname_fields
                                          if k[0] != '_'])).hexdigest()

    def is_init_oag(self, clauseprms, indexprm, initprms={}):
        attrs = self.propmgr._set_attrs_from_userprms(initprms)
        self.propmgr._set_cframe_from_attrs(attrs)

        if self.db.searchprms is not None:
            self.db.refresh(gotodb=True)

            if len(self.rdf._rdf_window) == 0:
                raise OAGraphRetrieveError("No results found in database")

            if self.is_unique:
                self.propmgr._set_attrs_from_cframe_uniq()

        return self

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

    def reset(self):
        self.cache.clear()

        self.propmgr._cframe = {}
        # add in rdf functionality

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
            print e.message
            print "This should never happen"
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
                        print "[%s] proxying request for [%s] to [%s]" % (rpc.router.id, attr, rpc.proxied_url)
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
                 clauseprms=None,
                 indexprm='id',
                 initprms={},
                 initurl=None,
                 extcur=None,
                 logger=OALog(),
                 rpc=True,
                 heartbeat=True):

        # Alphabetize
        self._extcur         = extcur
        self._iteridx        = None
        self._logger         = logger
        self._oagid          = hashlib.sha256(str(self)).hexdigest()

        #### Set up proxies

        # Database API
        self._db_proxy       = OAG_DbProxy(self, clauseprms, indexprm)

        # Relational Dataframe manipulation
        self._rdf_proxy      = OAG_RdfProxy(self)

        # Set attributes on OAG and keep them in sync with cframe
        self._prop_proxy     = OAG_PropProxy(self)

        # Manage oagprop state
        self._cache_proxy    = OAG_CacheProxy(self)

        # All RPC operations
        self._rpc_proxy      = OAG_RpcProxy(self, initurl=initurl, rpc_enabled=rpc, heartbeat_enabled=heartbeat)

        if not self._rpc_proxy.is_proxy:
            self._prop_proxy.profile_set(self.oagid)
            self.is_init_oag(clauseprms, indexprm, initprms)
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
            self.db.update()
            gevent.sleep(getenv().rpctimeout)

    def start_heartbeat(self):
        from gevent import spawn
        if self.rpc.is_heartbeat:
            if self.logger.RPC:
                print "[%s] Starting heartbeat greenlet" % (self.id)
            self.rpc._glets.append(spawn(self.__cb_heartbeat))
