import socket
import selectors
import resource
import struct
import random
import os
import sys
import time
import hashlib


def log(*args, brief=True):
    for arg in args:
        if brief and isinstance(arg, str) and len(arg) > 100:
            arg = arg[:100].encode() + b'...'
        print(arg)


class SocketBrokenException(Exception):
    pass


class WrongChecksumException(Exception):
    pass


COMMAND_NEW_REQUEST = 1
COMMAND_NEW_ACCEPT = 2
COMMAND_NEW_FAILED = 3
COMMAND_PROXY = 4
COMMAND_USER_GONE = 5
COMMAND_APP_GONE = 6
COMMAND_HEARTBEAT_PING = 7

HEADER_LENGTH = 9
HEARTBEAT_INTERVAL = 40
CHECKSUM_SALT = b'hello world'


# BEFORE_HANDSHAKE = 0
# AFTER_HANDSHAKE = 1

# base connection used from client connection and user connection
class BaseConnection:
    def __init__(self, sock):
        self.sock = sock
        self.readbuffer = b''

    def fileno(self):
        return self.sock.fileno()

    def close(self):
        self.sock.close()

    def recv(self, new_data):
        pass


class ParsableMixin:
    def _parse(self):
        log("parsing protocol")
        if len(self.readbuffer) >= HEADER_LENGTH:
            command, checksum, user_fd, data_length = struct.unpack('!BHHL', self.readbuffer[:HEADER_LENGTH])
            header = {
                'command': command,
                'checksum': checksum,
                'user_fd': user_fd,
                'data_length': data_length
            }

            packet_length = HEADER_LENGTH + data_length
            if len(self.readbuffer) >= packet_length:
                packet = self.readbuffer[:packet_length]
                if self._check_checksum(packet):
                    data = self.readbuffer[HEADER_LENGTH: packet_length]
                    self.readbuffer = self.readbuffer[packet_length:]
                    return header, data
                else:
                    raise WrongChecksumException()
        # else:
        #         log("not whole package")
        # else:
        #     log("not whole header")
        return None

    @classmethod
    def _check_checksum(cls, packet):
        log("checking checksum")
        return cls._cal_checksum(packet) == packet[1:3]

    @classmethod
    def _add_checksum(cls, packet):
        check_sum = cls._cal_checksum(packet)
        return packet[:1] + check_sum + packet[3:]

    @staticmethod
    def _cal_checksum(packet):
        whole = packet[:1] + b'\x00\x00' + packet[3:] + CHECKSUM_SALT
        md5 = hashlib.md5()
        md5.update(whole)
        return md5.digest()[:2]

    @classmethod
    def _package(cls, command, user_fd, data=b''):
        packet = struct.pack('!BHHL', command, 0, user_fd, len(data)) + data
        return cls._add_checksum(packet)


class OSReadWriteMixin:
    def __init__(self):
        self.send_buffers = {}

    @staticmethod
    def _read_chunk(fd):
        chunk = b''
        try:
            while True:  # read all data currently available
                # tmp = fd.recv(3)
                tmp = os.read(fd, 1024)
                if tmp == b'':
                    raise SocketBrokenException()
                # log("tmp is: {}".format(tmp))
                chunk += tmp
                if len(tmp) < 1024:
                    break
        except ConnectionResetError:
            log('connectionReset!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            raise SocketBrokenException()
        except BlockingIOError:  # if no data available, python throw this error if non-blocking socket is used
            log("blockio error!!!")
            pass
        log("{} get new chunk: {}".format(fd, chunk))
        return chunk

    def _write_chunk(self, fd):
        l = os.write(fd, self.send_buffers[fd])
        self.send_buffers[fd] = self.send_buffers[fd][l:]


# connection from proxy client
class ClientConnection(BaseConnection, ParsableMixin):
    def __init__(self, sock, server):
        super().__init__(sock)
        self.server = server
        self.user_server = None
        self.last_heartbeat_time = time.time()

    def recv(self, new_data):
        self.readbuffer += new_data
        try:
            while True:
                packet = self._parse()
                if packet:
                    self.process(packet)
                else:
                    break
        except WrongChecksumException:
            log("wrong checksum, close client")
            self.server.wrong_client(self.fileno())

    def process(self, packet):
        log("processing packet: {}".format(packet))
        header = packet[0]
        data = packet[1]
        command = header['command']

        if command == COMMAND_NEW_REQUEST:  # new proxy request
            log("should add a new user port")
            user_server, port = self.server.add_user_server(self.fileno())
            if user_server:
                self.user_server = user_server
                log("new port is {}".format(port))
                response = self._package(COMMAND_NEW_ACCEPT, 0, struct.pack('!H', port))
            else:
                response = self._package(COMMAND_NEW_FAILED, 0, b'')
            self.server.send_data(self.fileno(), response)
        elif command == COMMAND_PROXY:
            log("should proxy the data: {}".format(data))
            self.server.send_data(header['user_fd'], data)
        elif command == COMMAND_APP_GONE:
            log("app close socket for user {}".format(header['user_fd']))
            self.server.app_gone(header['user_fd'])
        elif command == COMMAND_HEARTBEAT_PING:
            log("client send heartbeat")
            self.last_heartbeat_time = time.time()

    def user_gone(self, fd):
        log("user {} closed, notify the client: {}".format(fd, self.fileno()))
        response = self._package(COMMAND_USER_GONE, fd, b'')
        self.server.send_data(self.fileno(), response)


# connection from end user
class UserConnection(BaseConnection, ParsableMixin):
    def __init__(self, sock, client_fd, server_fd):
        super().__init__(sock)
        self.client_fd = client_fd
        self.server_fd = server_fd
        self.proxy_server = None

    def set_proxy_server(self, ps):
        self.proxy_server = ps

    def recv(self, new_data):
        self.readbuffer += new_data
        self.process()

    def process(self):
        log("got new data: {}".format(self.readbuffer))
        response = self._package(COMMAND_PROXY, self.fileno(), self.readbuffer)
        self.readbuffer = b''
        self.proxy_server.send_data(self.client_fd, response)


# listening socket for end user
class UserServer:
    def __init__(self, sock, client_fd):
        self.sock = sock
        self.client_fd = client_fd
        self.user_conns = {}

    def fileno(self):
        return self.sock.fileno()

    def accept(self):
        new_conn, addr = self.sock.accept()
        new_conn.setblocking(False)
        log("new connection from {}:{}".format(addr[0], addr[1]))
        user_conn = UserConnection(new_conn, self.client_fd, self.sock.fileno())
        self.user_conns[user_conn.fileno()] = user_conn
        log("user server {} has user conns: {}".format(self.fileno(), self.user_conns), brief=False)
        return user_conn, addr

    def user_gone(self, fd):
        self.user_conns.pop(fd, None)
        log("user server has user conns: {}".format(self.user_conns), brief=False)

    # close self and rerun all user connection fds to proxy server to unregister them
    def close(self):
        self.sock.close()
        fd_list = list(self.user_conns.keys())
        self.user_conns.clear()
        return fd_list


# main proxy server
class ProxyServer(OSReadWriteMixin):
    def __init__(self, ip, port):
        OSReadWriteMixin.__init__(self)
        self.ip = ip
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.selectors = selectors.DefaultSelector()
        self.client_conns = {}
        self.user_conns = {}
        self.user_servers = {}
        self.user_port_range = (10000, 20000)
        # self.send_buffers = {}
        self.user_fd_close_list = set()
        self.last_heartbeat_check_time = time.time()

    def add_user_server(self, fd):
        log("{} wants to add a new user server".format(fd))
        s = self._new_user_server_socket()
        if not s:
            return None, None
        user_server = UserServer(s, fd)
        self.user_servers[s.fileno()] = user_server
        log("new user server socket: {}".format(s))
        self.selectors.register(s.fileno(), selectors.EVENT_READ)
        log(self.user_servers)
        return user_server, s.getsockname()[1]

    def _new_user_server_socket(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setblocking(False)
        for i in range(0, 20):
            try:
                port = random.randint(self.user_port_range[0], self.user_port_range[1])
                # port = 12345
                s.bind((self.ip, port))
                s.listen(5)
                return s
            except OSError as e:
                print(e)
        return None

    def app_gone(self, user_fd):
        """
        app closed socket for user_fd
        :param user_fd:
        """
        # if there is unsent data to user, send it before close user connection
        if self.send_buffers.get(user_fd, None):
            log("has write buffer, close later")
            self.user_fd_close_list.add(user_fd)
        else:
            log("no write buffer, close now")
            self.user_servers[self.user_conns[user_fd].server_fd].user_gone(user_fd)
            self._close_user(user_fd)

    def wrong_client(self, client_fd):
        self._close_client(client_fd)

    def send_data(self, fd, packet):
        log("gonna send data to {}: {}".format(fd, packet))
        self.send_buffers[fd] = self.send_buffers.get(fd, b'') + packet
        self.selectors.modify(fd, selectors.EVENT_READ | selectors.EVENT_WRITE)

    def start_serve(self):
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
        self.sock.setblocking(False)
        self.sock.bind((self.ip, self.port))
        self.sock.listen(5)

        self.selectors.register(self.sock.fileno(), selectors.EVENT_READ)

        log("start listening on port {}".format(self.port))
        while True:
            log("=========\nwaiting for I/O")
            log("memory usage: {}".format(self._show_memory()))
            for key, event in self.selectors.select(60):
                fd = key.fileobj

                if event == selectors.EVENT_READ:
                    # 1.ProxyServer
                    if fd == self.sock.fileno():  # server listening socket
                        new_conn, addr = self.sock.accept()
                        new_conn.setblocking(False)
                        log("new connection from {}:{}".format(addr[0], addr[1]))
                        self.selectors.register(new_conn.fileno(), selectors.EVENT_READ)
                        self.client_conns[new_conn.fileno()] = ClientConnection(new_conn, self)
                        log(self.client_conns, brief=False)

                    # 2.UserServer
                    elif self.user_servers.get(fd, None):
                        log("user server {} has new connection".format(fd))
                        user_conn, addr = self.user_servers[fd].accept()
                        self.selectors.register(user_conn.fileno(), selectors.EVENT_READ)
                        self.user_conns[user_conn.fileno()] = user_conn
                        user_conn.set_proxy_server(self)
                        log(self.user_conns, brief=False)

                    # 3.ClientConnection
                    elif self.client_conns.get(fd, None):
                        log("new read event from client connection {}".format(fd))
                        try:
                            self.client_conns[fd].recv(self._read_chunk(fd))
                        except SocketBrokenException:
                            self._close_client(fd)
                    # 4.UserConnection
                    elif self.user_conns.get(fd, None):
                        log("new read event from user connection {}".format(fd))
                        try:
                            self.user_conns[fd].recv(self._read_chunk(fd))
                        except SocketBrokenException:
                            log("user closed")
                            self._close_user(fd, True)

                elif event == selectors.EVENT_WRITE:
                    # 1.ClientConnection or 2.UserConnection
                    if self.client_conns.get(fd, None) or self.user_conns.get(fd, None):
                        log("new write event from client {}".format(fd))
                        log("packet to send is: {}".format(self.send_buffers[fd]))
                        self._write_chunk(fd)
                        if self.send_buffers[fd] == b'':
                            self.selectors.modify(fd, selectors.EVENT_READ)

            # check heartbeats
            self._check_heartbeats()

            # close listed user conns
            self._close_user_list()

    def _close_user_list(self):
        if len(self.user_fd_close_list) > 0:
            log("close user list")
            log(self.user_fd_close_list)
            for fd in set(self.user_fd_close_list):
                if self.send_buffers.get(fd, b'') == b'':
                    self._close_user(fd)
                    self.user_fd_close_list.remove(fd)
            log(self.user_fd_close_list)

    def _check_heartbeats(self):
        now = time.time()
        if now > self.last_heartbeat_check_time + HEARTBEAT_INTERVAL + 20:
            for fd in list(self.client_conns.keys()):
                client_conn = self.client_conns[fd]
                if now > client_conn.last_heartbeat_time + HEARTBEAT_INTERVAL + 10:
                    self._close_client(fd)
            self.last_heartbeat_check_time = now

    def _close_client(self, fd):
        log("client closed")
        client_conn = self.client_conns[fd]
        # 1.unregister own
        self.selectors.unregister(fd)
        # 2.close own socket
        client_conn.close()
        # 3.clear buffers
        self.client_conns.pop(fd, None)
        self.send_buffers.pop(fd, None)
        log("client conns: {}".format(self.client_conns), brief=False)

        if client_conn.user_server:
            log("closing related user server")
            # 1.unregister user server
            self.selectors.unregister(client_conn.user_server.fileno())
            # 2.close related user server
            user_fd_list = client_conn.user_server.close()
            log("user fd list: {}".format(user_fd_list), brief=False)
            # 3.remove user server from user_server list
            self.user_servers.pop(client_conn.user_server.fileno(), None)
            self.send_buffers.pop(client_conn.user_server.fileno(), None)
            # 4.close all user connection
            for user_fd in user_fd_list:
                self._close_user(user_fd)

    def _close_user(self, fd, need_notify=False):
        user_conn = self.user_conns[fd]
        # 1.unregister it from selectors
        self.selectors.unregister(fd)
        # 2.close own socket
        user_conn.close()
        if need_notify:
            # 3.notify the server
            self.user_servers[user_conn.server_fd].user_gone(fd)
            # 4.notify the client
            self.client_conns[user_conn.client_fd].user_gone(fd)
        # 5.clear buffers
        self.user_conns.pop(fd, None)
        self.send_buffers.pop(fd, None)
        log(self.user_conns, brief=False)

    @staticmethod
    def _show_memory():
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 / 1024


# proxy client
class Client(ParsableMixin, OSReadWriteMixin):
    def __init__(self, server_ip, server_port, app_ip, app_port):
        OSReadWriteMixin.__init__(self)
        self.server_ip = server_ip
        self.server_port = server_port
        self.app_ip = app_ip
        self.app_port = app_port
        self.selectors = selectors.DefaultSelector()
        self.server = None
        self.app_conns = {}  # app socket fd => socket obj
        self.user_fd_map = {}  # user fd => app socket fd
        self.app_conn_map = {}  # app socket fd => user fd
        self.app_port_range = (10000, 20000)
        # self.send_buffers = {}
        self.readbuffer = b''
        self.app_fd_close_list = set()
        self.last_heartbeat_time = time.time()

    def _recv(self, new_data):
        self.readbuffer += new_data
        try:
            while True:
                packet = self._parse()
                if packet:
                    self._process(packet)
                else:
                    break
        except WrongChecksumException:
            log("wrong checksum, close client")
            self._close_server()

    def _process(self, packet):
        log("processing packet: {}".format(packet))
        header = packet[0]
        data = packet[1]
        command = header['command']

        if command == COMMAND_NEW_FAILED:  # new proxy request
            log("server refused")
            sys.exit(1)
        elif command == COMMAND_NEW_ACCEPT:
            port = struct.unpack('!H', data)[0]
            log("user server port is {}".format(port))
        elif command == COMMAND_PROXY:
            log("should proxy the data: {}".format(data))
            self._send_app_data(header['user_fd'], data)
        elif command == COMMAND_USER_GONE:
            user_fd = header['user_fd']
            app_fd = self.user_fd_map.get(user_fd, None)
            if app_fd is None:
                return
            if self.send_buffers.get(app_fd, None):
                self.app_fd_close_list.add(app_fd)
            else:
                log("user {} gone".format(user_fd))
                self._close_app(app_fd)

    def _send_app_data(self, user_fd, data):
        app_fd = self.user_fd_map.get(user_fd, None)
        if app_fd is None:
            # 1. new socket
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.app_ip, self.app_port))
            s.setblocking(False)
            app_fd = s.fileno()
            self.selectors.register(app_fd, selectors.EVENT_READ)
            # 2. map
            self.user_fd_map[user_fd] = app_fd
            self.app_conns[app_fd] = s
            self.app_conn_map[app_fd] = user_fd
        log("send data to app {} for user {}: {}".format(app_fd, user_fd, data))
        self._send_data(app_fd, data)

    def _proxy_data(self, app_fd, data):
        """
        send data from app to server
        :param app_fd:
        :param data: bytes
        """
        log("send data {} to user {}".format(data, self.app_conn_map[app_fd]))
        packet = self._package(COMMAND_PROXY, self.app_conn_map[app_fd], data)
        self._send_data(self.server.fileno(), packet)

    def _send_command(self, data):
        """
        send command to server
        :param data:
        """
        self._send_data(self.server.fileno(), data)

    def _send_data(self, fd, data):
        """
        put data to send buffers
        :param fd:
        :param data: bytes
        """
        self.send_buffers[fd] = self.send_buffers.get(fd, b'') + data
        self.selectors.modify(fd, selectors.EVENT_READ | selectors.EVENT_WRITE)

    def start_client(self):
        # 1.check app info
        try:
            app_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            app_socket.connect((self.app_ip, self.app_port))
            app_socket.close()
        except ConnectionRefusedError:
            log("app not exists")
            sys.exit(1)

        # 2.connect server
        try:
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.connect((self.server_ip, self.server_port))
        except ConnectionRefusedError:
            log("server not exists")
            sys.exit(1)
        # 3.send request command
        log("send request")
        self.server.send(self._package(COMMAND_NEW_REQUEST, 0, b''))
        self.server.setblocking(False)
        self.selectors.register(self.server.fileno(), selectors.EVENT_READ)

        # 4.proxy data
        log("start proxy for {}:{}".format(self.app_ip, self.app_port))
        while True:
            log("=========\nwaiting for I/O")
            for key, event in self.selectors.select(20):
                fd = key.fileobj
                # log(fd, event)

                if event == selectors.EVENT_READ:
                    # 1.ProxyServer
                    if fd == self.server.fileno():
                        log("new read event from server")
                        try:
                            self._recv(self._read_chunk(fd))
                        except SocketBrokenException:
                            self._close_server()
                    # 2.AppConnection
                    elif self.app_conns.get(fd, None):
                        log("new read event from app connection {}".format(fd))
                        try:
                            self._proxy_data(fd, self._read_chunk(fd))
                        except SocketBrokenException:
                            log("app {} closed".format(fd))
                            self._close_app(fd, True)

                elif event == selectors.EVENT_WRITE:
                    # 1.AppConnection or 2.ProxyServer
                    if self.app_conns.get(fd, None) or fd == self.server.fileno():
                        log("new write event for {}".format(fd))
                        log("packet to send is: {}".format(self.send_buffers[fd]))
                        self._write_chunk(fd)
                        if self.send_buffers[fd] == b'':
                            self.selectors.modify(fd, selectors.EVENT_READ)
                else:
                    log("surprise event??? fd: {}, event: {}".format(fd, event))
                    self.selectors.unregister(fd)

            # send hearbeat
            self._send_heartbeat()

            # close listed app conns
            self._close_app_list()

    def _close_app_list(self):
        if len(self.app_fd_close_list) > 0:
            log("close app list")
            log(self.app_fd_close_list)
            for fd in set(self.app_fd_close_list):
                if self.send_buffers.get(fd, b'') == b'':
                    self.app_fd_close_list.remove(fd)
                    self._close_app(fd)
            log(self.app_fd_close_list)

    def _send_heartbeat(self):
        now = time.time()
        if now > self.last_heartbeat_time + HEARTBEAT_INTERVAL:
            log("send heartbeat")
            self._send_command(self._package(COMMAND_HEARTBEAT_PING, 0))
            self.last_heartbeat_time = now

    @staticmethod
    def _close_server():
        log("server closed")
        sys.exit(1)

    def _close_app(self, fd, neednotify=False):
        app_conn = self.app_conns[fd]
        # 1.unregister it from selectors
        self.selectors.unregister(fd)
        # 2.close own socket
        app_conn.close()

        # 3.clear buffers
        user_fd = self.app_conn_map[fd]
        self.app_conns.pop(fd, None)
        self.user_fd_map.pop(user_fd, None)
        self.app_conn_map.pop(fd, None)
        self.send_buffers.pop(fd, None)

        # 4.notify the server
        if neednotify:
            self._send_command(self._package(COMMAND_APP_GONE, user_fd))
        log(self.app_conns)


if __name__ == '__main__':
    pass
    # server = ProxyServer('127.0.0.1', 8080)
    # server.start_serve()
    # client = Client('127.0.0.1', 8080, '127.0.0.1', 80)
    # client.start_client()
