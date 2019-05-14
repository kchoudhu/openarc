
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

# Clean up internal symbols
del(bootstrap)
del(_dao)
del(_db)
del(_env)
del(_graph)
del(_rdf)
del(_rpc)
del(_util)
