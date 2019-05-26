
# Initialize library
def bootstrap():
    from ._env import oainit
    from gevent import monkey

    monkey.patch_all()
    oainit()

bootstrap()

# Hoist symbols from submodules
from ._dao   import OADbTransaction
from ._env   import *
from ._graph import *
from ._util  import *

oaenv.init_db()

def cleanup(symbol):
    if symbol in globals():
        del(globals()[symbol])

cleanup('bootstrap')
cleanup('_dao')
cleanup('_db')
cleanup('_env')
cleanup('_graph')
cleanup('_rdf')
cleanup('_rpc')
cleanup('_util')
