from gevent import monkey;

import src.redis.server as rdis

monkey.patch_all()

server = rdis.Server(address="127.0.0.1", port=31337)
server.run()