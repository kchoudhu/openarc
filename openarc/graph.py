#!/usr/bin/env python2.7

import hashlib
import inflection
import inspect

from textwrap          import dedent as td

from openarc.dao       import *
from openarc.exception import *

class oagprop(object):
    """Responsible for maitaining _oagcache on decorated properties"""
    def __init__(self, fget=None, fset=None, fdel=None, doc=None, extkey=None):
        self.fget = fget
        self.fset = fset
        self.fdel = fdel
        if doc is None and fget is not None:
            doc = fget.__doc__
        self.__doc__ = doc
        self.extkey=extkey

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
        obj._oagcache[self.extkey] = value

class staticproperty(property):
    def __get__(self, cls, owner):
        return classmethod(self.fget).__get__(None, owner)()

class OAGraphRootNode(object):

    def create(self, initprms={}):

        attrs = self._set_attrs_from_userprms(initprms) if len(initprms)>0 else []
        self._set_cframe_from_attrs(attrs, fullhouse=True)

        if self._rawdata is not None:
            raise OAError("Cannot create item that has already been initiated")

        attrstr    = ', '.join([k for k in self._cframe])
        vals       = [self._cframe[k] for k in self._cframe]
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

    def init_state_cls(self, clauseprms, indexprm, initprms, extcur, debug):
        self._clear_oagprops()

        self._cframe         = {}
        self._fkframe        = {}
        self._rawdata        = None
        self._rawdata_window = None
        self._oagcache       = {}
        self._clauseprms     = clauseprms
        self._indexparm      = indexprm
        self._extcur         = extcur
        self._debug          = debug

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
        self._rawdata_window = self._rawdata
        self.__iteridx = 0
        self._oagcache = {}
        self._set_attrs_from_cframe()
        return self

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
        if self._debug:
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

    def __init__(self, clauseprms=None, indexprm='id', initprms={}, extcur=None, debug=False):
        self.init_state_cls(clauseprms, indexprm, initprms, extcur, debug)
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

    def _clear_oagprops(self):
        oagproplist = getattr(self.__class__, "oagproplist", [])
        for op in oagproplist:
            if getattr(self.__class__, op, None) is not None:
                delattr(self.__class__, op)
        setattr(self.__class__, 'oagproplist', [])

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
        if len(self._rawdata_window) != 1:
            raise OAGraphIntegrityError("Graph object indicated unique, but returns more than one row from database")
        self._cframe = self._rawdata_window[0]
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

    def _set_oagprop(self, stream, payload):
        # Maintain list of oagprops that have been set
        oagproplist = getattr(self.__class__, "oagproplist", None)
        if oagproplist is None:
            oagproplist = []
        oagproplist.append(stream)
        setattr(self.__class__, 'oagproplist', oagproplist)

        # And actually set oagprop
        setattr(self.__class__, stream, payload)

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
    def dbpkname(cls): return "_%s_id" % cls.dbtable

    @staticproperty
    def dbtable(cls): return inflection.underscore(cls.__name__)[4:]

    @staticproperty
    def dbstreams(cls):

        raise NotImplementedError("Must be implemented in deriving OAGraph class")

    @property
    def id(self): return self._cframe[self.dbpkname]

    def init_state_dbschema(self):
        with OADao(self.dbcontext, cdict=False) as dao:
            with dao.cur as cur:
                # Check that dbcontext schema exists
                self.SQLexec(cur, self.SQL['admin']['schema'], parms=[self.dbcontext])
                check = cur.fetchall()
                if len(check)==0:
                    if self._debug:
                        print "Creating missing schema [%s]" % self.dbcontext
                    self.SQLexec(cur, self.SQL['admin']['mkschema'])

                # Check for presence of table
                try:
                    self.SQLexec(cur, self.SQL['admin']['table'])
                except psycopg2.ProgrammingError as e:
                    dao.commit()
                    if ('relation "%s.%s" does not exist' % (self.dbcontext, self.dbtable)) in str(e):
                        if self._debug:
                            print "Creating missing table [%s]" % self.dbtable
                        self.SQLexec(cur, self.SQL['admin']['mktable'])
                        self.SQLexec(cur, self.SQL['admin']['table'])

                # Check for table schema integrity
                oag_columns     = self.dbstreams.keys()
                db_columns_ext  = [desc[0] for desc in cur.description if desc[0][0] != '_']
                db_columns_reqd = self.db_oag_mapping.values()

                dropped_cols = [ dbc for dbc in db_columns_ext if dbc not in db_columns_reqd ]
                if len(dropped_cols)>0:
                    raise OAGraphIntegrityError("Dropped columns %s detected, cannot initialize" % dropped_cols)

                add_cols = [rdb for rdb in db_columns_reqd if rdb not in db_columns_ext]
                if len(add_cols)>0:
                    if self._debug:
                        print "Adding new columns %s to [%s]" % (add_cols, self.dbtable)
                    add_col_clauses = []
                    for i, col in enumerate(oag_columns):
                        if db_columns_reqd[i] in add_cols:
                            if oag_columns[i] != db_columns_reqd[i]:
                                subnode = self.dbstreams[col][0](debug=self._debug)
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

            dao.commit()

    @classmethod
    def is_oagnode(cls, stream):
        streaminfo = cls.dbstreams[stream][0]
        if type(streaminfo).__name__=='type':
            return 'OAGraphRootNode' in [x.__name__ for x in inspect.getmro(streaminfo)]
        else:
            return False

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

        # Add in user defined SQL
        for action, sqlinfo in self.sql_local.items():
            for index, sql in sqlinfo.items():
                default_sql[action][index] = sql

        return default_sql

    def SQLpp(self, SQL):
        """Pretty prints SQL and populates schema{0}.table{1} and its primary
        key{2} in given SQL string"""
        return td(SQL.format(self.dbcontext, self.dbtable, self.dbpkname))

    @property
    def sql_local(self): return {}

    def _refresh_from_cursor(self, cur):
        self.SQLexec(cur, self.SQL['admin']['fkeys'])
        self._fkframe = cur.fetchall()
        super(OAG_RootNode, self)._refresh_from_cursor(cur)

    def _set_attrs_from_cframe(self):
        oag_db_mapping = {self.db_oag_mapping[k]:k for k in self.db_oag_mapping}

        # Set dbstream attributes
        for stream, streaminfo in self._cframe.items():
            try:
                stream = oag_db_mapping[stream]
                if self.is_oagnode(stream):
                    def fget(obj, streaminfo=streaminfo, stream=stream):
                        return self.dbstreams[stream][0](clauseprms=[streaminfo], debug=self._debug)
                    fget.__name__ = stream
                    self._set_oagprop(stream, oagprop(fget, extkey=stream))
                else:
                    setattr(self, stream, self._cframe[stream])
            except KeyError as e:
                setattr(self, stream, self._cframe[stream])

        # Set forward lookup attributes
        for fk in self._fkframe:
            classname = "OAG_"+inflection.camelize(fk['table'])
            for cls in OAGraphRootNode.__subclasses__()+OAG_RootNode.__subclasses__():
                if cls.__name__==classname:
                    stream = fk['table']
                    def fget(obj,
                              cls=cls,
                              clauseprms=[getattr(self, fk['points_to_id'], None)],
                              indexprm='by_'+{cls.db_oag_mapping[k]:k for k in cls.db_oag_mapping}[fk['id']]):
                        return cls(clauseprms, indexprm, debug=self._debug)
                    fget.__name__ = stream
                    self._set_oagprop(stream, oagprop(fget))

    def _set_attrs_from_cframe_uniq(self):

        if len(self._rawdata_window) != 1:
            raise OAGraphIntegrityError("Graph object indicated unique, but returns more than one row from database")
        self._cframe = self._rawdata_window[0]
        self._set_attrs_from_cframe()

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
            if self.is_oagnode(stream):
                def fget(obj):
                    return streaminfo
                fget.__name__ = stream
                self._set_oagprop(stream, oagprop(fget, extkey=stream))
            setattr(self, stream, streaminfo)
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
                cfval = cfval.id
            cframe_tmp[cfkey] = cfval

        if fullhouse:
            if len(missing_streams)>0:
                raise OAGraphIntegrityError("Missing streams detected %s" % missing_streams)

        self._cframe = cframe_tmp
