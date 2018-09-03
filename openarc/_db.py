#!/usr/bin/env python3

from textwrap    import dedent as td

from openarc.dao import *

class DbSchemaProxy(object):
    def __init__(self, dbproxy):
        self._dbproxy = dbproxy

    def init(self):
        dbp = self._dbproxy
        oag = dbp._oag

        if not oag.streamable:
            if oag.logger.SQL:
                print("[%s] is not streamable, not creating" % oag.dbtable)
            return oag

        with OADao(oag.context, cdict=False) as dao:
            with dao.cur as cur:
                # Check that context schema exists
                dbp.SQLexec(dbp.SQL['admin']['schema'], cur=cur)
                check = cur.fetchall()
                if len(check)==0:
                    if oag.logger.SQL:
                        print("Creating missing schema [%s]" % oag.context)
                    dbp.SQLexec(dbp.SQL['admin']['mkschema'], cur=cur)
                    dao.commit()

                # Check for presence of table
                try:
                    dbp.SQLexec(dbp.SQL['admin']['table'], cur=cur)
                except psycopg2.ProgrammingError as e:
                    dao.commit()
                    if ('relation "%s.%s" does not exist' % (oag.context, oag.dbtable)) in str(e):
                        if oag.logger.SQL:
                            print("Creating missing table [%s]" % oag.dbtable)
                        dbp.SQLexec(dbp.SQL['admin']['mktable'], cur=cur)
                        dbp.SQLexec(dbp.SQL['admin']['table'], cur=cur)

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
                        print("Adding new columns %s to [%s]" % (add_cols, oag.dbtable))
                    add_col_clauses = []
                    for i, col in enumerate(oag_columns):
                        if db_columns_reqd[i] in add_cols:
                            if oag_columns[i] != db_columns_reqd[i]:
                                subnode = oag.streams[col][0](rpc=False).db.schema.init()
                                add_clause = "ADD COLUMN %s int %s references %s.%s(%s)"\
                                             % (self._dbproxy._oag.stream_db_mapping[col],
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
                    dbp.SQLexec(addcol_sql, cur=cur)

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
                        dbp.SQLexec(exec_sql, cur=cur)

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

                dbp.SQLexec(dbp.SQL['admin']['fkeys'], cur=cur)
                setattr(oag.__class__, '_fkframe', cur.fetchall())
                dbp._oag.reset(idxreset=False)

class DbTransaction(object):
    def __init__(self, db_proxy, exttxn):

        self._commit_on_exit = True
        self._connection = None
        self._cursor = None
        self._db_proxy = db_proxy
        self.depth = 0
        self.external_registrations = []

        if exttxn:
            exttxn.external_registrations.append(self)
            self._commit_on_exit = False
            self._connection = exttxn.connection
            self.depth = exttxn.depth

    def __enter__(self):

        if self._connection is None:
            self._connection = OADao(self._db_proxy._oag.context)

        self.depth += 1
        return self

    def __exit__(self, type, value, traceback):

        # short circuit transaction commit if flag is not set
        if not self._commit_on_exit:
            return

        # Clean some stuff up
        if self.depth==1:
            self._cursor = None
            self._connection.commit()
            self._connection.close()
            self._connection = None
            while len(self.external_registrations)>0:
                extreg = self.external_registrations.pop()
                extreg._commit_on_exit = True
                extreg._connection = None
                extreg._cursor = None
                extreg.depth = 0

        # You are definitely not using this transaction again
        self.depth -= 1

    @property
    def connection(self):
        return self._connection

    @property
    def cur(self):
        if not self._cursor:
            self._cursor = self.connection.cur
        return self._cursor

class DbProxy(object):
    """Responsible for manipulation of database"""
    def __init__(self, oag, searchprms, searchidx, exttxn):
        from .graph import OAG_RootNode

        # Store reference to outer object
        self._oag = oag

        # Schema is unknown right now
        self._schema = None

        # Transaction management
        self._db_transaction = DbTransaction(self, exttxn)

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

    def clone(self, src):
        self._schema     = src.db.schema
        self._searchprms = src.db.searchprms
        self._searchidx  = src.db.searchidx

    def create(self, initprms={}):

        if getenv().on_demand_oags:
            self.schema.init()

        self._oag.propmgr._set_cframe_from_userprms(initprms, fullhouse=True)

        if self._oag.rdf._rdf is not None:
            raise OAError("Cannot create item that has already been initiated")

        filtered_cframe = {k:self._oag.propmgr._cframe[k] for k in self._oag.propmgr._cframe if k[0] != '_'}
        attrstr    = ', '.join([k for k in filtered_cframe])
        vals       = [filtered_cframe[k] for k in filtered_cframe]
        formatstrs = ', '.join(['%s' for v in vals])
        insert_sql = self.SQL['insert']['id'] % (attrstr, formatstrs)

        with self.transaction:
            results = self.SQLexec(insert_sql, vals)
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
            self._oag.propmgr._set_attrs_from_cframe_uniq()

        return self._oag

    def delete(self, broadcast=False):

        delete_sql = self.SQL['delete']['id']

        with self.transaction:
            self.SQLexec(delete_sql, [self._oag.id])
            self.search(throw_on_empty=False, broadcast=broadcast)

        if self._oag.is_unique:
            self._oag.propmgr._set_attrs_from_cframe_uniq()

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

        self._oag.propmgr._set_cframe_from_userprms(updparms)

        self.update_searchprms()

        member_attrs  = [k for k in self._oag.propmgr._cframe if k[0] != '_']
        index_key     = [k for k in self._oag.propmgr._cframe if k[0] == '_'][0]
        update_clause = ', '.join(["%s=" % attr + "%s"
                                    for attr in member_attrs])
        update_sql    = self.SQL['update']['id']\
                        % (update_clause, getattr(self._oag, index_key, ""))
        update_values = [self._oag.propmgr._cframe[attr] for attr in member_attrs]

        with self.transaction:
            self.SQLexec(update_sql, update_values)
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
                    new_searchprms.append(self._oag.propmgr._cframe[key])
                except KeyError:
                    new_searchprms.append(self._searchprms[i])

            self._searchprms = new_searchprms

    def __refresh_from_cursor(self, broadcast=False):
        try:
            # if type(self.SQL).__name__ == "str":
            #     self._oag.rdf._rdf = self.SQLexec(self.SQL, self._searchprms)
            # elif type(self.SQL).__name__ == "dict":
            self._oag.rdf._rdf = self.SQLexec(self.SQL['read'][self._searchidx], self._searchprms)
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
                from openarc._rpc import OAGRPC_REQ_Requests
                for listener in listeners:
                    OAGRPC_REQ_Requests(self._oag).update_broadcast(listener)

        except psycopg2.ProgrammingError:
            raise OAGraphRetrieveError("Missing database table")

    @property
    def schema(self):
        if not self._schema:
            self._schema = DbSchemaProxy(self)
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
                ORDER BY {3}""").format(self._oag.context, self._oag.dbtable, streaminfo[0].dbpkname[1:]+'_'+stream, self._oag.dbpkname)
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

        # Add in "all" search
        all_sql = td("""
                  SELECT *
                    FROM {0}.{1}
                   WHERE 1=1
                ORDER BY {2}""")
        default_sql['read']['by_all'] = all_sql.format(self._oag.context, self._oag.dbtable, self._oag.dbpkname)

        # Add in user defined SQL
        for action, sqlinfo in self._oag.dblocalsql.items():
            for index, sql in sqlinfo.items():
                default_sql[action]['by_'+index] = sql.format(self._oag.context, self._oag.dbtable, self._oag.dbpkname)

        return default_sql

    def SQLexec(self, query, parms=[], cur=None):

        def curexec():
            if self._oag.logger.SQL:
                print(cur.mogrify(query, parms))
            cur.execute(query, parms)
            if return_results:
                try:
                    return cur.fetchall()
                except:
                    return None
            else:
                return None

        results = None
        return_results = True
        # Figure out which cursor to use
        if cur is None:
            if self.transaction.depth>0:
                cur = self.transaction.cur
        else:
            return_results = False

        if cur is None:
            with OADao(self._oag.context) as dao:
                with dao.cur as cur:
                    results = curexec()
                    dao.commit()
        else:
            results = curexec()

        return results

    def SQLpp(self, SQL):
        """Pretty prints SQL and populates schema{0}.table{1} and its primary
        key{2} in given SQL string"""
        return SQL.format(self._oag.context, self._oag.dbtable, self._oag.dbpkname)

    @property
    def transaction(self):
        return self._db_transaction
