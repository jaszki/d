from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer

from collections import namedtuple
from io import BytesIO
from socket import error as socket_error


class CommandError(Exception): pass
class Disconnect(Exception): pass


class Server(object):

    def __init__(self, address="127.0.0.1", port=7777):
        self._pool = Pool(size=64)
        self._server = StreamServer(
            listener = [
                address,
                port
            ],
            handle = self.connection_handler,
            spawn=self._pool
        )
        self._protocol = ProtocolHandler()
        self._kv = {}
        self._commands = self.get_commands()

    def connection_handler(self, conn: socket.socket, address):
        socket_file = conn.makefile('rwb')
        while True:
            try:
                data = self._protocol.handle_request(socket_file)
            except Disconnect as e:
                break

            try:
                resp = self.get_response(data)
            except CommandError as e:
                # TODO what do we do with error?
                # SOme ideas: log it, print it
                raise e
            
            self._protocol.write_response(socket_file, resp)
        
    def get_response(self, data: str):
        # We assume that the data is either of:
        # 1. A string command
        # 2. An array of string commands
        pass
    
    def get_commands(self):
        return {
            "GET": 1,
            "SET": 2,
            "DEL": 3,
            "FLUSH": 4,
            "MGET": 4,
            "MSET": 5
        }

    def get(self, key):
        return self._kv[key]

    def set(self, key, value):
        self._kv[key] = value
        return 1

    def delete(self, key):
        if key in self._kv:
            del self._kv[key]
            return 1
        return 0

    def flush(self):
        self._kv = {}
        return 0

    def mget(self, )

    def run(self):
        self._server.serve_forever()


class ProtocolHandler(object):

    def __init__(self):
        self._handlers = {
            '+': self.handle_simple_string,
            '-': self.handle_error,
            ':': self.handle_integer,
            '$': self.handle_string,
            '*': self.handle_array,
            '%': self.handle_dict
        }
    
    def handle_request(self, socket_file: BytesIO):
        first_byte = socket_file.read(1)
        if first_byte is None:
            raise Disconnect()
        return self._handlers[first_byte](socket_file) 

    def handle_simple_string(self, socket_file: BytesIO):
        return socket_file.readline().rstrip('\r\n')

    def handle_error(self, socket_file: BytesIO):
        return socket_file.readline().rstrip('\r\n')

    def handle_integer(self, socket_file: BytesIO):
        return int(socket_file.readline().rstrip('\r\n'))

    def handle_string(self, socket_file: BytesIO):
        byte_count = int(socket_file.readline().split('\r\n'))
        if byte_count > 0:
            return socket_file.read(byte_count+2)[:-2]
    
    def handle_array(self, socket_file: BytesIO):
        num_elems = int(socket_file.readline().rstrip('\r\n'))
        res = []
        for i in range(num_elems):
            res.append(self.handle_request(socket_file))
        return res

    def handle_dict(self, socket_file: BytesIO):
        num_keys = int(socket_file.readline().rstrip('\r\n'))
        res = {}
        for i in range(num_keys):
            key = self.handle_request(socket_file)
            value = self.handle_request(socket_file)
            res[key] = value
        return res
    
    def write_response(self, socket_file: BytesIO, data):
        buf = BytesIO()

    def _write(self, buf: BytesIO, data):
        if isinstance(data, str):
            buf.write(f"+{str}\r\n")
        elif isinstance(data, Exception):
            buf.write(f"-{data}\r\n")
        elif isinstance(data, int):
            buf.write(f":{int}\r\n")
        elif isinstance(data, bytes):
            buf.write(f"${len(data)}\r\n{[i + '\r\n' for i in data]}")
        elif isinstance(data, list):
            buf.write(f"*{len(data)}\r\n")
            for i in data:
                self._write(buf, i)
        elif isinstance(data, dict):
            buf.write(f"%{len(data)}\r\n")
            for k, v in dict:
                self._write(buf, k)
                self._write(buf, v)
