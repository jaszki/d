from collections import namedtuple
from io import BytesIO
from socket import error as socket_error
import logging

from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer

class CommandError(Exception): pass
class Disconnect(Exception): pass

Error = namedtuple('Error', ('message',))

# Setup the logger
log = logging.getLogger(__name__)
log.setLevel("DEBUG")
streamHandler = logging.StreamHandler()
log.addHandler(streamHandler)
streamHandler.setFormatter(
    logging.Formatter(
        fmt="{asctime} - {levelname} - {message}",
        style="{",
        datefmt="%Y-%m-%d %H:%M",
    )
)

class Server(object):

    def __init__(self, address="127.0.0.1", port=31337):
        self._pool = Pool(size=64)
        self._server = StreamServer(
            (
                address,
                port
            ),
            self.connection_handler,
            spawn=self._pool
        )
        self._protocol = ProtocolHandler()
        self._kv = {}
        self._commands = self.get_commands()

    def connection_handler(self, conn, address):
        print(conn.getblocking())
        socket_file = conn.makefile('rwb')
        log.info("connection received")
        while True:
            log.info("in loop")
            try:
                log.info("trying to decode")
                data = self._protocol.handle_request(socket_file)
                log.info("command decoded")
            except Disconnect as e:
                log.exception("received disconnect")
                break

            try:
                resp = self.get_response(data)
                log.info("response generated")
            except CommandError as e:
                resp = Error(message=e.args[0])
            
            self._protocol.write_response(socket_file, resp)
            log.info("written response")

    def get_response(self, data):
        # We assume that the data is either of:
        # 1. A string command
        # 2. An array of string commands
        if not isinstance(data, list):
            try:
                data.split()
            except:
                raise CommandError("Request must be list or simple string")
        
        if not data:
            raise CommandError("Missing command")
        
        command = data[0].upper()

        if command in self._commands:
            return self._commands[command](*data[1:])
        else:
            raise CommandError(f"Unrecognised command {command}")

    def get_commands(self):
        return {
            "GET": self.get,
            "SET": self.set,
            "DEL": self.delete,
            "FLUSH": self.flush,
            "MGET": self.mget,
            "MSET": self.mset
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
        kvlen = len(self._kv)
        self._kv.clear()
        return kvlen

    def mget(self, keys):
        # TODO how did the zip function come into this?
        return [self._kv[k] for k in keys]

    def mset(self, *kv):
        data = zip(kv[::2], kv[1::2])
        for k, v in data:
            self._kv[k] = v
        return len(data)

    def run(self):
        self._server.serve_forever()


class ProtocolHandler(object):

    def __init__(self):
        self._handlers = {
            b'+': self.handle_simple_string,
            b'-': self.handle_error,
            b':': self.handle_integer,
            b'$': self.handle_string,
            b'*': self.handle_array,
            b'%': self.handle_dict
        }
    
    def handle_request(self, socket_file: BytesIO):
        log.info("BEFORE reading first byte")
        first_byte = socket_file.read(1)
        log.info('AFTER reading first byte')
        log.info(f"First byte length = {len(first_byte)}")
        if not first_byte:
            raise Disconnect()
        try:
            return self._handlers[first_byte](socket_file)
        except KeyError:
            raise CommandError("bad request")

    def handle_simple_string(self, socket_file):
        return socket_file.readline().decode("utf-8").rstrip('\r\n')

    def handle_error(self, socket_file):
        return Error(socket_file.readline().decode("utf-8").rstrip('\r\n'))

    def handle_integer(self, socket_file):
        return int(socket_file.readline().decode("utf-8").rstrip('\r\n'))

    def handle_string(self, socket_file):
        # First read the length ($<length>\r\n).
        length = int(socket_file.readline().decode("utf-8").rstrip('\r\n'))
        if length == -1:
            return None  # Special-case for NULLs.
        length += 2  # Include the trailing \r\n in count.
        return socket_file.read(length).decode("utf-8")[:-2]
    
    def handle_array(self, socket_file: BytesIO):
        num_elems = int(socket_file.readline().decode("utf-8").rstrip('\r\n'))
        return [self.handle_request(socket_file) for _ in range(num_elems)]

    def handle_dict(self, socket_file: BytesIO):
        num_keys = int(socket_file.readline().decode("utf-8").rstrip('\r\n'))
        res = {}
        for _ in range(num_keys):
            key = self.handle_request(socket_file)
            value = self.handle_request(socket_file)
            res[key] = value
        return res
    
    def write_response(self, socket_file: BytesIO, data):
        buf = BytesIO()
        self._write(buf, data)
        buf.seek(0)
        socket_file.write(buf.getvalue())
        socket_file.flush()

    def _write(self, buf, data):
        if isinstance(data, str):
            data = data.encode('utf-8')
        if isinstance(data, bytes):
            buf.write(b'$%d\r\n%s\r\n' % (len(data), data))
        elif isinstance(data, int):
            buf.write(b':%d\r\n' % data)
        elif isinstance(data, Error):
            buf.write(b'-%s\r\n' % data.message)
        elif isinstance(data, (list, tuple)):
            to_write = b'*%d\r\n' % len(data)
            buf.write(to_write)
            for item in data:
                self._write(buf, item)
        elif isinstance(data, dict):
            buf.write(b'%%%d\r\n' % len(data))
            for key in data:
                self._write(buf, key)
                self._write(buf, data[key])
        elif data is None:
            buf.write(b'$-1\r\n')
        else:
            raise CommandError('unrecognized type: %s' % type(data))


class Client(object):
    def __init__(self, host="127.0.0.1", port = 31337):
        self._protocol = ProtocolHandler()
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((host, port))
        self._fh = self._socket.makefile('rwb')
    
    def execute(self, *args):
        self._protocol.write_response(self._fh, args)
        resp = self._protocol.handle_request(self._fh)
        if isinstance(resp, Error):
            raise CommandError("Invalid response")
        return resp
    
    def get(self, key):
        return self.execute('GET', key)

    def set(self, key, val):
        return self.execute('SET', key, val)

    def delete(self, key):
        return self.execute('DEL', key)

    def flush(self):
        return self.execute('FLUSH')

    def mget(self, *args):
        return self.execute('MGET', args)
    
    def mset(self, *args):
        return self.execute('MSET', args)