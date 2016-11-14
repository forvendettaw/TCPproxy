from tcpproxy import ProxyServer

server = ProxyServer('127.0.0.1', 8080)
server.start_serve()