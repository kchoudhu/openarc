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

    @property
    def size(self):
        if self._rawdata is None:
            return 0
        else:
            return len(self._rawdata)

    @property
    def infname(self):
        if len(self.infname_fields)==0:
            raise OAError("Cannot calculate infname if infname_fields not set")
        return hashlib.sha256(str().join([str(getattr(self, k, ""))
                                          for k in self.infname_fields
                                          if k[0] != '_'])).hexdigest()

    def next(self):
        if self.is_unique:
            raise OAError("next: Unique OAGraph object is not iterable")
        else:
            if self.__iteridx < self.size:
                self._oagcache = {}
                self._cframe = self._rawdata[self.__iteridx]
                self.__iteridx += 1
                self.__set_attrs_from_cframe()
                return self
            else:
                self.__iteridx = 0
                raise StopIteration()

    def __init__(self, clauseprms=None, indexparm='id', initparms={}, extcur=None, debug=False):

        self._cframe         = initparms
        self._rawdata        = None
        self._oagcache       = {}
        self._clauseprms     = clauseprms
        self._indexparm      = indexparm
        self._extcur         = extcur

        # Flip to True to see SQL being executed
        self._debug          = debug

        if self._clauseprms is not None:
            self.refresh(gotodb=True)

            if len(self._rawdata) == 0:
                raise OAGraphRetrieveError("No results found in database")

            if self.is_unique:
                self.__set_uniq_attrs()

        self.__set_attrs_from_cframe()

    def __set_attrs_from_cframe(self):
        for k, v in self._cframe.items():
            setattr(self, k, v)

    def __set_uniq_attrs(self):
        if len(self._rawdata) != 1:
            raise OAGraphIntegrityError("Graph object indicated unique, but returns more than one row from database")
        self._cframe = self._rawdata[0]
        self.__set_attrs_from_cframe()

    def create(self, initparm):
        self._cframe = initparm

        if self._rawdata is not None:
            raise OAError("Cannot create item that has already been initiated")

        ### TODO: scehma validation

        attrstr    = ', '.join([k for k in self._cframe])
        vals       = [self._cframe[k] for k in self._cframe]
        formatstrs = ', '.join(['%s' for v in vals])
        insert_sql = self.SQL['insert']['id'] % (attrstr, formatstrs)

        if self._extcur is None:
            with OADao(self.dbcontext) as dao:
                with dao.cur as cur:
                    if self._debug:
                        print cur.mogrify(insert_sql, vals)
                    cur.execute(insert_sql, vals)
                    index_val = cur.fetchall()
                    self._clauseprms = index_val[0].values()
                    self.__refresh_from_cursor(cur)
                    dao.commit()
        else:
            if self._debug:
                print self._extcur.mogrify(insert_sql, vals)
            self._extcur.execute(insert_sql, vals)
            index_val = self._extcur.fetchall()
            self._clauseprms = index_val[0].values()
            self.__refresh_from_cursor(self._extcur)

        # Refresh to set iteridx
        self.refresh()

        # Set attrs if this is a unique oag
        if self.is_unique:
            self.__set_uniq_attrs()

        return self

    def __refresh_from_cursor(self, cur):
        if type(self.SQL).__name__ == "str":
            if self._debug:
                print cur.mogrify(self.SQL, self._claseprms)
            cur.execute(self.SQL, self._clauseprms)
        elif type(self.SQL).__name__ == "dict":
            if self._debug:
                print cur.mogrify(self.SQL['read'][self._indexparm], self._clauseprms)
            cur.execute(self.SQL['read'][self._indexparm], self._clauseprms)
        else:
            raise OAError("Cannot find correct SQL to execute")
        self._rawdata = cur.fetchall()

    def refresh(self, gotodb=False):
        """Generally we want to simply reset the iterator; set gotodb=True to also
        refresh instreams from the database"""
        if gotodb is True:
            if self._extcur is None:
                with OADao(self.dbcontext) as dao:
                    with dao.cur as cur:
                        self.__refresh_from_cursor(cur)
            else:
                self.__refresh_from_cursor(self._extcur)
        self.__iteridx = 0
        self._oagcache = {}
        return self

    def update(self, updparms={}):

        if len(updparms)>0:
            for k, v in updparms.items():
                setattr(self, k, v)

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
                    if self._debug:
                        print cur.mogrify(update_sql, update_values)
                    cur.execute(update_sql, update_values)
                    dao.commit()
        else:
            if self._debug:
                print self._extcur.mogrify(update_sql, update_values)
            self._extcur.execute(update_sql, update_values)
        return self

    @property
    def dbcontext(self):
        raise NotImplementedError("Must be implemented in deriving OAGraph class")

    @property
    def infname_fields(self):
        """Override in deriving classes as necessary"""
        return [k for k, v in self._cframe.items()]

    @property
    def is_unique(self):
        raise NotImplementedError("Must be implemented in deriving OAGraph class")

    @property
    def SQL(self):
        raise NotImplementedError("Must be implemneted in deriving OAGraph class")
