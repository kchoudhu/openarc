import psycopg2
import psycopg2.extras

class TestOABase(object):
    """Mixin class to assist with database testing"""
    def setUp_db(self):
        """Create scratch "test" schema in database"""
        from openarc._env import oaenv, oainit

        oainit()
        dbinfo = oaenv.dbinfo
        self.dbconn = psycopg2.connect(dbname='openarc',
                                       user=dbinfo['user'],
                                       password=dbinfo['password'],
                                       host=dbinfo['host'],
                                       port=dbinfo['port'])
        with self.dbconn.cursor() as cur:
            cur.execute(self.SQL.drop_test_schema)
            cur.execute(self.SQL.create_test_schema)
            self.dbconn.commit()

    def tearDown_db(self):
        with self.dbconn.cursor() as cur:
            cur.execute(self.SQL.drop_test_schema)
            self.dbconn.commit()

    def nuke_database(self):
        self.clear_openarc_schema()
        self.dbconn.commit()

    def clear_openarc_schema(self):
        with self.dbconn.cursor() as setupcur:
            setupcur.execute(self.SQL.delete_openarc_rpc)

    class SQL(object):
        ## Test schema helper SQL
        drop_test_schema =\
            "DROP SCHEMA IF EXISTS test CASCADE"
        create_test_schema =\
            "CREATE SCHEMA test"
        ## Common schema helper SQL
        delete_openarc_rpc =\
            "DELETE FROM openarc.rpc_registry"
