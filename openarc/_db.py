#!/usr/bin/env python3

from textwrap    import dedent as td

from openarc.dao import *

class DbSchemaProxy(object):
    def __init__(self, dbproxy):
        self._dbproxy = dbproxy

    def init(self, crstack=[]):
        dbp = self._dbproxy
        oag = dbp._oag

        if not oag.streamable:
            if oag.logger.SQL:
                print("[%s] is not streamable, not creating" % oag.dbtable)
            return oag

        if oag.__class__ in crstack:
            if oag.logger.SQL:
                print("Class %s is already in the process of being creating, not recursing" % oag.__class__)
            return oag
        else:
            crstack.append(oag.__class__)


        with OADbTransaction("schema init") as tran: #, cdict=False) as dao:
            # Check that context schema exists
            check = tran.dao.execute(dbp.SQL['admin']['schema'])
            if len(check)==0:
                if oag.logger.SQL:
                    print("Creating missing schema [%s]" % oag.context)
                tran.dao.execute(dbp.SQL['admin']['mkschema'])

            # Check for presence of table
            extcur = []
            try:
                tran.dao.execute(dbp.SQL['admin']['table'], cdict=False, extcur=extcur, savepoint=True)
                db_columns = [desc[0] for desc in extcur[0].description]
            except psycopg2.ProgrammingError as e:
                if ('relation "%s.%s" does not exist' % (oag.context, oag.dbtable)) in str(e):
                    if oag.logger.SQL:
                        print("Creating missing table [%s]" % oag.dbtable)
                    tran.dao.execute(dbp.SQL['admin']['mktable'])
                    tran.dao.execute(dbp.SQL['admin']['table'], cdict=False, extcur=extcur)
                    db_columns = [desc[0] for desc in extcur[0].description]

            # Check for table schema integrity
            oag_columns     = sorted(oag.streams.keys())
            db_columns_ext  = [desc for desc in db_columns if desc[0] != '_']
            db_columns_reqd = [oag.stream_db_mapping[k] for k in sorted(oag.stream_db_mapping.keys())]

            dropped_cols = [ dbc for dbc in db_columns_ext if dbc not in db_columns_reqd ]
            if len(dropped_cols)>0:
                raise OAGraphIntegrityError("Dropped columns %s detected, cannot initialize" % dropped_cols)

            add_cols = [rdb for rdb in db_columns_reqd if rdb not in db_columns_ext]
            if len(add_cols)>0:
                if oag.logger.SQL:
                    print("Adding new columns %s to [%s]" % (add_cols, oag.dbtable))
                add_col_clauses = []
                for i, col in enumerate(oag_columns):
                    if db_columns_reqd[i] in add_cols:
                        if oag_columns[i] != db_columns_reqd[i]:
                            subnode = oag.streams[col][0](rpc=False).db.schema.init(crstack=crstack)
                            add_clause = "ADD COLUMN %s int %s references %s.%s(%s)"\
                                         % (self._dbproxy._oag.stream_db_mapping[col],
                                           'NOT NULL' if oag.streams[col][1] else str(),
                                            subnode.context,
                                            subnode.dbtable,
                                            subnode.dbpkname)
                        else:
                            # Could be straight definition of database type, or enum. If enum,
                            # force column type to int. Optionality is determined by true/false
                            # on the second field of the declaration for enums.
                            if type(oag.streams[col][0])==str:
                                add_clause = "ADD COLUMN %s %s" % (col, oag.streams[col][0])
                                if oag.streams[col][1] is not None:
                                    add_clause = "%s NOT NULL" % add_clause
                            else:
                                add_clause = "ADD COLUMN %s int" % (col)
                                if oag.streams[col][1]:
                                    add_clause = "%s NOT NULL" % add_clause

                        add_col_clauses.append(add_clause)

                addcol_sql = dbp.SQLpp("ALTER TABLE {0}.{1} %s") % ",".join(add_col_clauses)
                tran.dao.execute(addcol_sql)

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
                    tran.dao.execute(exec_sql)

        crstack.pop()

        return oag

    def init_fkeys(self):
        dbp = self._dbproxy
        oag = dbp._oag

        # Don't set props for proxied OAGs, they are passthrough entities
        if oag.rpc.is_proxy:
            return

        with OADbTransaction("init fkey") as tran:
            for stream in oag.streams:
                if oag.is_oagnode(stream):
                    currattr = getattr(oag, stream, None)
                    if currattr:
                        currattr.db.schema.init_fkeys()

            results = tran.dao.execute(dbp.SQL['admin']['fkeys'])
            setattr(oag.__class__, '_fkframe', results)
            dbp._oag.reset(idxreset=False)

class DbProxy(object):
    """Responsible for manipulation of database"""
    def __init__(self, oag, searchprms, searchidx, searchwin, searchoffset, searchdesc):
        from .graph import OAG_RootNode

        # Store reference to outer object
        self._oag = oag

        # Schema is unknown right now
        self._schema = None

        # Intialize search parameters, if any
        if searchprms:
            if type(searchprms).__name__ in ['dict']:
                rawprms = [searchprms[prm] for prm in sorted(searchprms.keys())]
            elif type(searchprms).__name__ in ['list', 'tuple']:
                rawprms = searchprms
            else:
                rawprms = [searchprms]

            searchprms = map(lambda x: x.id if isinstance(x, OAG_RootNode) else x, rawprms)

        self._searchprms     = list(searchprms) if searchprms else list()
        self._searchidx      = searchidx
        self._searchwin      = searchwin
        self._searchoffset   = searchoffset
        self._searchdesc     = searchdesc

    @property
    def _dao(self):
        return OADao(self._oag.context) if not gctx().db_txndao else gctx().db_txndao

    def clone(self, src):
        self._schema     = src.db.schema
        self._searchprms = src.db.searchprms
        self._searchidx  = src.db.searchidx

    def create(self, initprms={}):

        if getenv().on_demand_oags:
            self.schema.init()

        self._oag.props._set_cframe_from_userprms(initprms, fullhouse=True)

        if self._oag.rdf._rdf is not None:
            raise OAError("Cannot create item that has already been initiated")

        filtered_cframe = {k:self._oag.props._cframe[k] for k in self._oag.props._cframe if k[0] != '_'}
        attrstr    = ', '.join([k for k in filtered_cframe])
        vals       = [filtered_cframe[k] for k in filtered_cframe]
        formatstrs = ', '.join(['%s' for v in vals])
        insert_sql = self.SQL['insert']['id'] % (attrstr, formatstrs)

        results = self._dao.execute(insert_sql, vals)
        if self._searchidx=='id':
            index_val = results
            self._searchprms = list(index_val[0].values())
        self.__refresh_from_cursor()

        # Refresh to set iteridx
        self._oag.reset()

        if getenv().on_demand_oags:
            self.schema.init_fkeys()

        # Autosetting a multinode is ok here, because it is technically
        # only restoring properties that were originally set before the
        # call to the database.
        #
        # DO preserve cache though, we don't want to kill OAGs that were
        # set as part of the initialization process.
        if not self._oag.is_unique and len(self._oag.rdf._rdf_window)>0:
            self._oag.__getitem__(self._oag.rdf._rdf_window_index, preserve_cache=True)
        else:
            self._oag.props._set_attrs_from_cframe_uniq()

        return self._oag

    def delete(self, broadcast=False):

        delete_sql = self.SQL['delete']['id']

        self._dao.execute(delete_sql, [self._oag.id])
        self.search(throw_on_empty=False, broadcast=broadcast)

        if self._oag.is_unique:
            self._oag.props._set_attrs_from_cframe_uniq()

        return self

    def search(self, throw_on_empty=True, broadcast=False):
        """Generally we want to simply reset the iterator; set gotodb=True to also
        refresh instreams from the database"""
        self.__refresh_from_cursor(broadcast=broadcast)

        # Is the new rdf empty?
        if throw_on_empty and len(self._oag.rdf._rdf) == 0:
            raise OAGraphRetrieveError("No results found in database")

        # Reset cache
        self._oag.cache.clear()

        # Refresh
        self._oag.reset()

        return self._oag

    @property
    def searchidx(self):
        return self._searchidx

    @property
    def searchprms(self):
        return self._searchprms

    def update(self, updparms={}, norefresh=False, broadcast=False):

        self._oag.props._set_cframe_from_userprms(updparms)

        self.update_searchprms()

        member_attrs  = [k for k in self._oag.props._cframe if k[0] != '_']
        index_key     = [k for k in self._oag.props._cframe if k[0] == '_'][0]
        update_clause = ', '.join(["%s=" % attr + "%s"
                                    for attr in member_attrs])
        update_sql    = self.SQL['update']['id']\
                        % (update_clause, getattr(self._oag, index_key, ""))
        update_values = [self._oag.props._cframe[attr] for attr in member_attrs]

        self._dao.execute(update_sql, update_values)
        if not norefresh:
            self.__refresh_from_cursor(broadcast=broadcast)

        if not self._oag.is_unique and len(self._oag.rdf._rdf_window)>0:
            self._oag.__getitem__(self._oag.rdf._rdf_window_index, preserve_cache=True)

        return self._oag

    def update_many(self, updparms, norefresh=False, broadcast=False):
        update_clause = ', '.join(["%s=%%s" % attr
                                   for attr in updparms.keys()])
        update_sql    = self.SQL['update'][self._searchidx] % (update_clause, '%s')

        self._dao.execute(update_sql, list(updparms.values())+self._searchprms)

        if not norefresh:
            self.__refresh_from_cursor(broadcast=broadcast)

        if not self._oag.is_unique and len(self._oag.rdf._rdf_window)>0:
            self._oag[self._oag.rdf._rdf_window_index]

        return self._oag

    def update_searchprms(self):
        # Update search parameteres from prop manager
        index = self._searchidx[3:]
        if index != str():
            try:
                keys = self._oag.dbindices[index][0]
            except KeyError:
                keys = [index]

            new_searchprms = []
            for i, key in enumerate(keys):
                key = self._oag.stream_db_mapping[key]
                try:
                    new_searchprms.append(self._oag.props._cframe[key])
                except KeyError:
                    new_searchprms.append(self._searchprms[i])

            self._searchprms = new_searchprms

    def __refresh_from_cursor(self, broadcast=False):
        try:
            select_sql = self.SQL['read'][self._searchidx]

            modified_searchprms = list(self._searchprms)

            if self._searchwin:
                select_sql += ' LIMIT %s'
                modified_searchprms = modified_searchprms + [self._searchwin]

            if self._searchoffset:
                select_sql += ' OFFSET %s'
                modified_searchprms = modified_searchprms + [self._searchoffset]

            self._oag.rdf._rdf = self._dao.execute(select_sql, modified_searchprms, savepoint=True)
            self._oag.rdf._rdf_window = self._oag.rdf._rdf

            for predicate in self._oag.rdf._rdf_filter_cache:
                self._oag.rdf.filter(predicate, rerun=True)

            if broadcast:
                from .graph import OAG_RpcDiscoverable
                remote_oags =\
                    OAG_RpcDiscoverable({
                        'rpcinfname' : self._oag.infname_semantic
                    }, 'by_rpcinfname_idx', rpc=False)

                listeners = [r.url for r in remote_oags if (r.listen is True and r.is_valid)]

                print('sending mesages to: %s' % listeners)
                from openarc._rpc import OARpc_REQ_Request
                for listener in listeners:
                    OARpc_REQ_Request(self._oag).update_broadcast(listener)

        except psycopg2.ProgrammingError:
            raise OAGraphRetrieveError("Missing database table")

    @property
    def schema(self):
        if not self._schema:
            self._schema = DbSchemaProxy(self)
        return self._schema

    @property
    def SQLorderdir(self):
        return 'DESC' if self._searchdesc else 'ASC'

    @property
    def SQL(self):

        # Default SQL defined for all tables
        default_sql = {
            "read" : {
              "id"       : self.SQLpp("""
                  SELECT *
                    FROM {0}.{1}
                   WHERE {2}=%s
                ORDER BY {2} {3}"""),
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
                ORDER BY {3} {4}""").format(self._oag.context, self._oag.dbtable, streaminfo[0].dbpkname[1:]+'_'+stream, self._oag.dbpkname, self.SQLorderdir)
                default_sql['read'][stream_sql_key] = stream_sql

        # Add in update/delegate by indices
        for index, idxinfo in self._oag.dbindices.items():
            select_sql = self.SQLpp("""
                  SELECT *
                    FROM {0}.{1}
                   WHERE %s
                ORDER BY {2} {3}""")

            update_sql = self.SQLpp("""
                  UPDATE {0}.{1}
                     SET %%s
                   WHERE %s""")

            # Genrate where clauses
            where_clauses = []
            for f in idxinfo[0]:
                where_clauses.append("{0}=%s".format(self._oag.stream_db_mapping[f] if self._oag.is_oagnode(f) else f))

            default_sql['read']['by_'+index] = select_sql % ' AND '.join(where_clauses)
            default_sql['update']['by_'+index] = update_sql % ' AND '.join(where_clauses)

        # Add in "all" search
        default_sql['read']['by_all'] = self.SQLpp("""
                  SELECT *
                    FROM {0}.{1}
                   WHERE 1=1
                ORDER BY {2}""")

        # Add in user defined SQL
        for action, sqlinfo in self._oag.dblocalsql.items():
            for index, sql in sqlinfo.items():
                default_sql[action]['by_'+index] = sql.format(self._oag.context, self._oag.dbtable, self._oag.dbpkname)

        return default_sql

    def SQLpp(self, SQL):
        """Pretty prints SQL and populates schema{0}.table{1} and its primary
        key{2} in given SQL string"""
        return SQL.format(self._oag.context, self._oag.dbtable, self._oag.dbpkname, self.SQLorderdir)
