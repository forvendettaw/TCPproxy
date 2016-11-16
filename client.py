from tcpproxy import Client

client = Client('taguxdesign.com', 8080, '127.0.0.1', 80)
# client = Client('127.0.0.1', 8080, '127.0.0.1', 80)
client.start_client()