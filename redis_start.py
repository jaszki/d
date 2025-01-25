import src.redis.server as rdis

server = rdis.Server(address="127.0.0.1", port=31337)
server.run()