from MyBase import MyBase
from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client
import sys
import socket

def main(server_num):
	print("Starting server {}".format(server_num))
	base = MyBase()
	ip = socket.gethostbyname(socket.gethostname())
	port = 1234
	server = SimpleXMLRPCServer((ip, port))
	server.register_instance(base)
	server.serve_forever()


if __name__ == '__main__':
	main(sys.argv[1])