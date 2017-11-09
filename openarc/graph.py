#!/usr/bin/env python2.7

import hashlib

from openarc.dao       import *
from openarc.exception import *

class oagprop(object):

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

    def getter(self, fget):
        return type(self)(fget, self.fset, self.fdel, self.__doc__)

class OAGraphRootNode(object):

    def create(self, initprms):
        attrs = self._set_attrs_from_userprms(initprms, fullhouse=True)
        self._set_cframe_from_attrs(attrs)

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
        self._cframe         = {}
        self._rawdata        = None
        self._oagcache       = {}
        self._clauseprms     = clauseprms
        self._indexparm      = indexprm
        self._extcur         = extcur
        self._debug          = debug

        if len(initprms)>0:
            attrs = self._set_attrs_from_userprms(initprms)
            self._set_cframe_from_attrs(attrs)

    def init_state_dbschema(self):

        return

    def init_state_oag(self):
        if self._clauseprms is not None:
            self.refresh(gotodb=True)

            if len(self._rawdata) == 0:
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
                self._cframe = self._rawdata[self.__iteridx]
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
        self.__iteridx = 0
        self._oagcache = {}
        return self

    @property
    def size(self):
        if self._rawdata is None:
            return 0
        else:
            return len(self._rawdata)

    def update(self, updparms={}):

        if len(updparms)>0:
            attrs = self._set_attrs_from_userprms(updparms)
            self._set_cframe_from_attrs(attrs)

        member_attrs  = [k for k in self._cframe if k[0] != '_']
        index_key     = [k for k in self._cframe if k[0] == '_'][0]
        update_clause = ', '.join(["%s=" % attr + "%s"
                                    for attr in member_attrs])
        update_sql    = self.SQL['update']['id']\
                        % (update_clause, getattr(self, index_key, ""))
        update_values = [getattr(self, attr, "") for attr in member_attrs]
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

    def _refresh_from_cursor(self, cur):
        if type(self.SQL).__name__ == "str":
            self.SQLexec(cur, self.SQL, self._clauseprms)
        elif type(self.SQL).__name__ == "dict":
            self.SQLexec(cur, self.SQL['read'][self._indexparm], self._clauseprms)
        self._rawdata = cur.fetchall()

    def _set_attrs_from_cframe(self):
        for k, v in self._cframe.items():
            setattr(self, k, v)

    def _set_attrs_from_cframe_uniq(self):
        if len(self._rawdata) != 1:
            raise OAGraphIntegrityError("Graph object indicated unique, but returns more than one row from database")
        self._cframe = self._rawdata[0]
        self._set_attrs_from_cframe()

    def _set_attrs_from_userprms(self, userprms, fullhouse=False):
        """Set attributes corresponding to params in userprms, return list of
        attrs created"""
        for k, v in userprms.items():
            setattr(self, k, v)
        return userprms.keys()

    def _set_cframe_from_attrs(self, keys):
        for k in keys:
            self._cframe[k] = getattr(self, k, "")
