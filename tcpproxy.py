import socket
import selectors
import resource
import struct
import random
import os
import sys


class SocketBrokenException(Exception):
    pass


# class WrongProtocolException(Exception):
#     pass


COMMAND_NEW_REQUEST = 1
COMMAND_NEW_ACCEPT = 2
COMMAND_NEW_FAILED = 3
COMMAND_PROXY = 4
COMMAND_USER_GONE = 5

HEADER_LENGTH = 9


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

    def _package(self, command, user_fd, data):
        return struct.pack('!BHHL', command, 0, user_fd, len(data)) + data


# connection from proxy client
class ClientConnection(BaseConnection):
    def __init__(self, sock, server):
        super().__init__(sock)
        self.server = server
        self.user_server = None

    def recv(self, new_data):
        self.readbuffer += new_data
        packet = self._parse()
        if packet:
            self.process(packet)

    def _parse(self):  # TODO: should be put in a while loop ??
        print("parsing protocol")
        if len(self.readbuffer) >= HEADER_LENGTH:
            command, checksum, user_fd, data_length = struct.unpack('!BHHL', self.readbuffer[:HEADER_LENGTH])
            header = {
                'command': command,
                'checksum': checksum,
                'user_fd': user_fd,
                'data_length': data_length
            }

            package_length = HEADER_LENGTH + data_length
            if len(self.readbuffer) >= package_length:
                data = self.readbuffer[HEADER_LENGTH: package_length]
                self.readbuffer = self.readbuffer[package_length:]
                return header, data
            else:
                print("not whole package")
        else:
            print("not whole header")
        return None

    def process(self, packet):
        print("processing packet: {}".format(packet))
        header = packet[0]
        data = packet[1]

        if header['command'] == COMMAND_NEW_REQUEST:  # new proxy request
            print("should add a new user port")
            user_server = self.server.add_user_server(self.fileno())
            if user_server:
                self.user_server = user_server
                response = self._package(COMMAND_NEW_ACCEPT, 0, b'')
            else:
                response = self._package(COMMAND_NEW_FAILED, 0, b'')
            self.server.send_data(self.fileno(), response)
        elif header['command'] == COMMAND_PROXY:
            print("should proxy the data: {}".format(data))
            self.server.send_data(header['user_fd'], data)
            # TODO: check if peer is closed, if so, return error code to let client close its port # really need check??

    def user_gone(self, fd):
        print("user {} closed, notify the client: {}".format(fd, self.fileno()))
        response = self._package(COMMAND_USER_GONE, fd, b'')
        self.server.send_data(self.fileno(), response)


# connection from end user
class UserConnection(BaseConnection):
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
        print("got new data: {}".format(self.readbuffer))
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
        print("new connection from {}:{}".format(addr[0], addr[1]))
        user_conn = UserConnection(new_conn, self.client_fd, self.sock.fileno())
        self.user_conns[user_conn.fileno()] = user_conn
        print("user server {} has user conns: {}".format(self.fileno(), self.user_conns))
        return user_conn, addr

    def user_gone(self, fd):
        self.user_conns.pop(fd, None)
        print("user server has user conns: {}".format(self.user_conns))

    # close self and rerun all user connection fds to proxy server to unregister them
    def close(self):
        self.sock.close()
        fd_list = list(self.user_conns.keys())
        print(fd_list)
        self.user_conns.clear()  # TODO: !!!!!!!!!!!!!!!!!!!!
        return fd_list


# main proxy server
class ProxyServer:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.selectors = selectors.DefaultSelector()
        self.client_conns = {}
        self.user_conns = {}
        self.user_servers = {}
        self.user_port_range = (10000, 20000)
        self.send_buffers = {}

    def add_user_server(self, fd):
        print("{} wants to add a new user server".format(fd))
        s = self._new_user_server_socket()
        user_server = UserServer(s, fd)
        self.user_servers[s.fileno()] = user_server
        print("new user server socket: {}".format(s))
        self.selectors.register(s.fileno(), selectors.EVENT_READ)
        print(self.user_servers)
        return user_server

    def _new_user_server_socket(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setblocking(False)
        while True:  # TODO: limit trial times
            try:
                # port = random.randint(self.user_port_range[0], self.user_port_range[1])
                port = 12345  # TODO: change this
                s.bind((self.ip, port))
                s.listen(5)
                return s
            except OSError:
                pass

    def send_data(self, fd, packet):
        print("{} wants to send data: {}".format(fd, packet))
        self.send_buffers[fd] = self.send_buffers.get(fd, b'') + packet
        self.selectors.modify(fd, selectors.EVENT_READ | selectors.EVENT_WRITE)

    def start_serve(self):
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)  # TODO: comment this
        self.sock.setblocking(False)
        self.sock.bind((self.ip, self.port))
        self.sock.listen(5)

        self.selectors.register(self.sock, selectors.EVENT_READ)

        # TODO:need a thread to check heartbeats

        print("start listening on port {}".format(self.port))
        while True:
            print("=========\nwaiting for I/O")
            print("memory usage: {}".format(self._show_memory()))
            for key, event in self.selectors.select(100):
                fd = key.fileobj

                if event == selectors.EVENT_READ:
                    # 1.ProxyServer
                    if fd == self.sock:  # server listening socket
                        new_conn, addr = self.sock.accept()
                        print("new connection from {}:{}".format(addr[0], addr[1]))
                        self.selectors.register(new_conn.fileno(), selectors.EVENT_READ)
                        self.client_conns[new_conn.fileno()] = ClientConnection(new_conn, self)
                        print(self.client_conns)

                    # 2.UserServer
                    elif self.user_servers.get(fd, None):
                        print("user server {} has new connection".format(fd))
                        user_conn, addr = self.user_servers[fd].accept()
                        self.selectors.register(user_conn.fileno(), selectors.EVENT_READ)
                        self.user_conns[user_conn.fileno()] = user_conn
                        user_conn.set_proxy_server(self)
                        print(self.user_conns)

                    # 3.ClientConnection
                    elif self.client_conns.get(fd, None):
                        print("new read event from client connection {}".format(fd))
                        try:
                            self.client_conns[fd].recv(self._read_chunk(fd))
                        except SocketBrokenException:
                            self._close_client(fd)
                    # 4.UserConnection
                    elif self.user_conns.get(fd, None):
                        print("new read event from user connection {}".format(fd))
                        try:
                            self.user_conns[fd].recv(self._read_chunk(fd))
                        except SocketBrokenException:
                            print("user closed")
                            self._close_user(fd, True)

                elif event == selectors.EVENT_WRITE:

                    # TODO: refactor two if to one
                    # TODO: what if error happens on write
                    # 1.ClientConnection
                    if self.client_conns.get(fd, None):
                        print("new write event from client {}".format(fd))
                        print("packet to send is: {}".format(self.send_buffers[fd]))
                        self._write_chunk(fd)
                        self.selectors.modify(fd, selectors.EVENT_READ)
                    # 2.UserConnection
                    elif self.user_conns.get(fd, None):
                        print("new write event from user {}".format(fd))
                        print("packet to send is: {}".format(self.send_buffers[fd]))
                        self._write_chunk(fd)
                        self.selectors.modify(fd, selectors.EVENT_READ)
                else:
                    print("surprise event??? fd: {}, event: {}".format(fd, event))
                    self.selectors.unregister(fd)

    def _close_client(self, fd):
        print("client closed")
        client_conn = self.client_conns[fd]
        # 1.unregister own
        self.selectors.unregister(fd)
        # 2.close own socket
        client_conn.close()
        # 3.clear buffers
        self.client_conns.pop(fd, None)
        self.send_buffers.pop(fd, None)
        print("client conns: {}".format(self.client_conns))

        if client_conn.user_server:
            print("closing related user server")
            # 1.unregister user server
            self.selectors.unregister(client_conn.user_server.fileno())
            # 2.close related user server
            user_fd_list = client_conn.user_server.close()
            print("user fd list: {}".format(user_fd_list))
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
        print(self.user_conns)

    def _read_chunk(self, fd):
        chunk = b''
        try:
            while True:  # read all data currently available
                # tmp = fd.recv(3)
                tmp = os.read(fd, 3)
                if tmp == b'':
                    raise SocketBrokenException()
                print("tmp is: {}".format(tmp))
                chunk += tmp
                if len(tmp) < 3:
                    break
        except BlockingIOError:  # if no data available, python throw this error if non-blocking socket is used
            print("blockio error!!!")
            pass
        print("{} get new chunk: {}".format(fd, chunk))
        return chunk

    def _write_chunk(self, fd):
        # TODO: write error check;  may not be able to write all data at once?
        os.write(fd, self.send_buffers[fd])
        self.send_buffers[fd] = b''

    @staticmethod
    def _show_memory():
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 / 1024


# proxy client
class Client:
    def __init__(self, server_ip, server_port, app_ip, app_port):
        self.server_ip = server_ip
        self.server_port = server_port
        self.app_ip = app_ip
        self.app_port = app_port
        self.selectors = selectors.DefaultSelector()
        self.server = None
        self.app_conns = {}         # app socket fd => socket obj
        self.user_fd_map = {}       # user fd => app socket fd
        self.app_conn_map = {}      # app socket fd => user fd
        self.app_port_range = (10000, 20000)
        self.send_buffers = {}
        self.readbuffer = b''

    def _recv(self, new_data):
        self.readbuffer += new_data
        packet = self._parse()
        if packet:
            self._process(packet)

    def _parse(self):  # TODO: should be put in a while loop ??
        print("parsing protocol")
        if len(self.readbuffer) >= HEADER_LENGTH:
            command, checksum, user_fd, data_length = struct.unpack('!BHHL', self.readbuffer[:HEADER_LENGTH])
            header = {
                'command': command,
                'checksum': checksum,
                'user_fd': user_fd,
                'data_length': data_length
            }

            package_length = HEADER_LENGTH + data_length
            if len(self.readbuffer) >= package_length:
                data = self.readbuffer[HEADER_LENGTH: package_length]
                self.readbuffer = self.readbuffer[package_length:]
                return header, data
            else:
                print("not whole package")
        else:
            print("not whole header")
        return None

    def _process(self, packet):
        print("processing packet: {}".format(packet))
        header = packet[0]
        data = packet[1]

        if header['command'] == COMMAND_NEW_FAILED:  # new proxy request
            print("server refused")
            sys.exit(1)
        elif header['command'] == COMMAND_PROXY:
            print("should proxy the data: {}".format(data))
            self._send_app_data(header['user_fd'], data)
        # TODO: user gone

    def _send_app_data(self, user_fd, data):
        print("send data {} to app for user {}".format(data, user_fd))
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

        self.send_buffers[app_fd] = self.send_buffers.get(app_fd, b'') + data
        self.selectors.modify(app_fd, selectors.EVENT_READ | selectors.EVENT_WRITE)

    def _proxy_data(self, app_fd, data):
        print("send data {} to user {}".format(data, self.app_conn_map[app_fd]))
        packet = self._package(COMMAND_PROXY, self.app_conn_map[app_fd], data)
        self.send_buffers[self.server.fileno()] = packet
        self.selectors.modify(self.server, selectors.EVENT_READ | selectors.EVENT_WRITE)

    def start_client(self):
        # 1.check app info
        try:
            app_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            app_socket.connect((self.app_ip, self.app_port))
            app_socket.close()
        except ConnectionRefusedError:
            print("app not exists")
            sys.exit(1)

        # 2.connect server
        try:
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.connect((self.server_ip, self.server_port))
        except ConnectionRefusedError:
            print("server not exists")
            sys.exit(1)
        # 3.send request command
        print("send request")
        self.server.send(self._package(COMMAND_NEW_REQUEST, 0, b''))
        self.server.setblocking(False)
        self.selectors.register(self.server, selectors.EVENT_READ)

        # 4.proxy data
        # TODO:need a thread to send hearbeat         ????? really need??
        print("start proxy for {}:{}".format(self.app_ip, self.app_port))
        while True:
            print("=========\nwaiting for I/O")
            for key, event in self.selectors.select(100):
                fd = key.fileobj
                print(fd, event)

                if event == selectors.EVENT_READ:
                    # 1.ProxyServer
                    if fd == self.server:
                        print("new read event from server")
                        try:
                            self._recv(self._read_chunk(fd.fileno()))
                        except SocketBrokenException:
                            self._close_server()
                    # 2.AppConnection
                    elif self.app_conns.get(fd, None):
                        print("new read event from app connection {}".format(fd))
                        try:
                            self._proxy_data(fd, self._read_chunk(fd))
                        except SocketBrokenException:
                            print("app closed")
                            self._close_app(fd, True)

                elif event == selectors.EVENT_WRITE:

                    # TODO: refactor two if to one
                    # TODO: what if error happens on write
                    # 1.AppConnection
                    if self.app_conns.get(fd, None):
                        print("new write event for app {}".format(fd))
                        print("packet to send is: {}".format(self.send_buffers[fd]))
                        self._write_chunk(fd)
                        self.selectors.modify(fd, selectors.EVENT_READ)
                    # 2.ProxyServer
                    elif fd == self.server:
                        print("new write event from server")
                        print("packet to send is: {}".format(self.send_buffers[fd.fileno()]))
                        self._write_chunk(fd.fileno())
                        self.selectors.modify(fd, selectors.EVENT_READ)
                else:
                    print("surprise event??? fd: {}, event: {}".format(fd, event))
                    self.selectors.unregister(fd)

    def _close_server(self):
        print("server closed")
        sys.exit(1)

    def _close_app(self, fd, need_notify=False):
        app_conn = self.app_conns[fd]
        # 1.unregister it from selectors
        self.selectors.unregister(fd)
        # 2.close own socket
        app_conn.close()

        # TODO: notify server
        # if need_notify:
        #     # 3.notify the server
        #     self.user_servers[app_conn.server_fd].user_gone(fd)
        #     # 4.notify the client
        #     self.client_conns[app_conn.client_fd].user_gone(fd)
        # 5.clear buffers
        user_fd = self.app_conn_map[fd]
        self.app_conns.pop(fd, None)
        self.user_fd_map.pop(user_fd,None)
        self.app_conn_map.pop(fd, None)
        self.send_buffers.pop(fd, None)
        print(self.app_conns)

    def _read_chunk(self, fd):
        chunk = b''
        try:
            while True:  # read all data currently available
                # tmp = fd.recv(3)
                tmp = os.read(fd, 3)
                if tmp == b'':
                    raise SocketBrokenException()
                print("tmp is: {}".format(tmp))
                chunk += tmp
                if len(tmp) < 3:
                    break
        except BlockingIOError:  # if no data available, python throw this error if non-blocking socket is used
            print("blockio error!!!")
            pass
        print("{} get new chunk: {}".format(fd, chunk))
        return chunk

    def _write_chunk(self, fd):
        # TODO: write error check;  may not be able to write all data at once?
        os.write(fd, self.send_buffers[fd])
        self.send_buffers[fd] = b''

    def _package(self, command, user_fd, data):
        return struct.pack('!BHHL', command, 0, user_fd, len(data)) + data

if __name__ == '__main__':
    pass
    # server = ProxyServer('127.0.0.1', 8080)
    # server.start_serve()
    # client = Client('127.0.0.1', 8080, '127.0.0.1', 80)
    # client.start_client()
