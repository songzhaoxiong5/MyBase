import xmlrpc.client
import string
import random


class Client:
	MAP_IP = "54.202.219.21"
	# MAP_IP = "localhost"
	MAP_PORT = 12345
	SERVER_PORT = 1234
	MAP_URL = "http://{}:{}/".format(MAP_IP, MAP_PORT)

	def __init__(self, map_ip=MAP_IP, map_port=MAP_PORT, server_port=SERVER_PORT):
		self.map_ip = map_ip
		self.map_port = map_port
		self.server_port = server_port
		self.map_url = "http://{}:{}/".format(map_ip, map_port)
		self.client_name = self._id_generator()
		self.mapper = xmlrpc.client.ServerProxy(self.map_url)
		self.mapper.register_client(self.client_name)

	def createTable(self, tableName):
		success = self.mapper.create_table(self.client_name, tableName)
		if not success:
			print("There was an error creating the table")

	def destroyTable(self, tableName):
		success = self.mapper.destroy_table(self.client_name, tableName)
		if not success:
			print("There was an error destroying the table")

	def openTable(self, tableName):
		success = self.mapper.open_table(self.client_name, tableName)
		if not success:
			print("There was an error opening the table")

	def closeTable(self, tableName):
		success = self.mapper.close_table(self.client_name, tableName)
		if not success:
			print("There was an error closing the table")

	def getRow(self, tableName, rowKey):
		server_ip = self.mapper.get_server(self.client_name, tableName, rowKey)
		if server_ip:
			server_url = "http://{}:{}/".format(server_ip, self.server_port)
			with xmlrpc.client.ServerProxy(server_url) as server:
				return server.getRow(tableName, rowKey)

	def putRow(self, tableName, rowKey, columns):
		server_ips = self.mapper.get_server(self.client_name, tableName, rowKey, True)
		success = True
		for server_ip in server_ips:
			server_url = "http://{}:{}/".format(server_ip, self.server_port)
			with xmlrpc.client.ServerProxy(server_url) as server:
				success = success and server.putRow(tableName, rowKey, columns)
		return success

	def getRows(self, tableName, startRow, endRow):
		servers = self.mapper.get_rows(self.client_name, tableName, startRow, endRow)
		results = []
		for s in servers:
			server_url = "http://{}:{}/".format(s['ip'], self.server_port)
			with xmlrpc.client.ServerProxy(server_url) as server:
				results += server.getRows(tableName, s['start'], s['end'])
		return results

	def getColumnByRow(self, tableName, rowKey, family, qualifiers):
		server_ip = self.mapper.get_server(self.client_name, tableName, rowKey)
		if server_ip:
			server_url = "http://{}:{}/".format(server_ip, self.server_port)
			with xmlrpc.client.ServerProxy(server_url) as server:
				return server.getColumnByRow(tableName, rowKey, family, qualifiers)

	def memTableLimit(self, tableName, newLimit):
		return self.mapper.mem_table_limit(self.client_name, tableName, newLimit)

	def getSchema(self, tableName):
		server_ip = self.mapper.get_schema(self.client_name, tableName)
		if server_ip:
			server_url = "http://{}:{}/".format(server_ip, self.server_port)
			with xmlrpc.client.ServerProxy(server_url) as server:
				return server.getSchema(tableName)


	def close(self):
		self.mapper.unregister_client(self.client_name)

	def _id_generator(self, size=6, chars=string.ascii_uppercase + string.digits):
		return ''.join(random.choice(chars) for _ in range(size))


def main():
	client = Client()
	# client.createTable("DistTable2")
	client.openTable("DistTable2")
	client.putRow('DistTable2', 'zhang', {'name' : {'first': 'paul', 'last': 'zhang'}})
	client.putRow('DistTable2', 'bstock', {'name' : {'first': 'brad', 'last': 'stocks'}})
	print(client.getRows("DistTable2", 'a', 'zz'))
	print(client.getRow("DistTable2", 'bstocks'))
	print(client.getRow("DistTable2", 'bstock'))
	print(client.getColumnByRow("DistTable2", 'zhang', 'name', ['last']))
	client.memTableLimit('DistTable2', 1)
	client.putRow('DistTable2', 'alynn', {'name' : {'first': 'stacey', 'last': 'lynn'}})
	client.close()

if __name__ == '__main__':
	main()
