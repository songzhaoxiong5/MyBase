from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client
import json
import socket

MY_PORT = 12345
PORT = 1234
SERVERS = ["35.166.106.4", "54.202.23.56", "54.186.9.90", "54.71.110.37", "34.213.163.149"]

# MAP_IP = "54.202.219.21"
# MAP_IP = 'localhost'
# MAP_IP = "172.31.33.28"
MAP_IP = socket.gethostbyname(socket.gethostname())

class Server:

	def __init__(self, ip, obj=None):
		if not obj:
			self.ip = ip
			self.tables = {}
		else:
			self.ip = obj['ip']
			self.tables = obj['tables']

	def add_tables(self, tables):
		for table in tables:
			self.tables[table['name']] = {'start': table['start'],
											'end': table['end'],
											'count' : table['count']}

	def removeTable(self, tableName):
		del self.tables['tableName']

	def get_obj(self):
		return {'ip': self.ip, 'tables':self.tables}

class Map:

	def __init__(self, fresh_start=True, W=2):
		self.num_servers = len(SERVERS)
		self.server_file = 'servers.json'
		self.tables_file = 'tables.json'
		self.table_leases = {}
		self.W = W
		self.clients = {}
		self.servers = []
		for s in SERVERS:
			self.servers.append(Server(s))
		self.ip = MAP_IP
		self.port = MY_PORT
		if fresh_start:
			self.tables = ['testTable']
			for t in self.tables:
				self._init_ranges(t)
		else:
			self.tables = []
			self._load_data()


	def get_server(self, client_name, tableName, key, put=False):
		if client_name not in self.clients:
			print("{} has not registered".format(client_name))
			return False
		if tableName not in self.tables:
			print("{} does not exist".format(tableName))
			return ''
		lower_key = key.lower()
		for i, server in enumerate(self.servers):
			if lower_key >= server.tables[tableName]['start'] and lower_key < server.tables[tableName]['end']:
				if put:
					servers = [server.ip]
					for j in range(1, self.W):
						servers.append(self.servers[(i+j)%self.num_servers].ip)
					return servers
				else:
					return server.ip
		print("This is an invalid key with no corresponding server")
		return ''

	def register_client(self, client_name):
		if client_name in self.clients:
			print("{} has already been registered".format(client_name))
			return False
		self.clients[client_name] = {'leases': []}
		print("Client {} has been registered".format(client_name))
		return True

	def unregister_client(self, client_name):
		if client_name not in self.clients:
			print("{} has not been registered".format(client_name))
			return False
		for lease in self.clients[client_name]['leases']:
			self.close_table(client_name, lease)
		del self.clients[client_name]
		print("Client {} has been unregistered".format(client_name))
		return True

	def open_table(self, client_name, tableName):
		if client_name not in self.clients:
			print("{} has not registered".format(client_name))
			return False
		if tableName not in self.tables:
			print("{} does not exist".format(tableName))
			return False
		if tableName in self.table_leases:
			if client_name in self.table_leases[tableName]['clients']:
				print("Client already has a lease on this table")
				return False
			self.table_leases[tableName]['count'] += 1
			self.table_leases[tableName]['clients'].append(client_name)
			self.clients[client_name]['leases'].append(tableName)
			return True
		else:
			success = True
			for server in self.servers:
				url = "http://{}:{}/".format(server.ip, PORT)
				with xmlrpc.client.ServerProxy(url) as curr_server:
					success = success and curr_server.openTable(tableName)

			self.table_leases[tableName] = {
							'count': 1, 'clients': [client_name]
							}
			self.clients[client_name]['leases'].append(tableName)
			return success


	def close_table(self, client_name, tableName):
		if client_name not in self.clients:
			print("{} has not registered".format(client_name))
			return False
		if tableName not in self.clients[client_name]['leases']:
			print('{} does not have a lease on {}'.format(client_name, tableName))
		if tableName in self.table_leases:
			self.table_leases[tableName]['count'] -= 1
			self.table_leases[tableName]['clients'].remove(client_name)
			self.clients[client_name]['leases'].remove(tableName)
			if self.table_leases[tableName]['count'] == 0:
				success = True
				for server in self.servers:
					url = "http://{}:{}/".format(server.ip, PORT)
					with xmlrpc.client.ServerProxy(url) as curr_server:
						success = success and curr_server.closeTable(tableName)
				del self.table_leases[tableName]
				return success
			return True
		else:
			print("Something has gone wrong in closing the table")
			return False

	def create_table(self, client_name, tableName):
		if client_name not in self.clients:
			print("{} has not registered".format(client_name))
			return False
		success = True
		for s in self.servers:
			url = "http://{}:{}/".format(s.ip, PORT)
			with xmlrpc.client.ServerProxy(url) as curr_server:
				success = success and curr_server.createTable(tableName)
		if success:
			self._init_ranges(tableName)
			self.tables.append(tableName)
		else:
			print("Something went wrong with creating the table")
		self._update_servers()
		self._update_tables()
		return success

	def destroy_table(self, client_name, tableName):
		if client_name not in self.clients:
			print("{} has not registered".format(client_name))
			return False
		if tableName not in self.tables:
			print("{} does not exist and can't be destroyed".format(tableName))
			return False
		if tableName in self.table_leases:
			print("{} is in use and can't be destroyed".format(tableName))
			return False
		success = True
		for s in self.servers:
			url = "http://{}:{}/".format(s.ip, PORT)
			with xmlrpc.client.ServerProxy(url) as curr_server:
				success = success and curr_server.destroyTable(tableName)
		if success:
			self.tables.remove(tableName)
			for s in self.servers:
				s.removeTable(tableName)
		else:
			print("Something went wrong in destroy")
		self._update_servers()
		self._update_tables()
		return success

	def get_rows(self, client_name, tableName, start, end):
		if client_name not in self.clients:
			print("{} has not registered".format(client_name))
			return False
		if tableName not in self.tables:
			print("{} does not exist".format(tableName))
			return ''
		servers = []
		start_server = 0
		l_start = start.lower()
		l_end = end.lower()
		end_server = self.num_servers
		for i, s in enumerate(self.servers):
			if l_start < s.tables[tableName]['end'] and l_start > s.tables[tableName]['start']:
				start_server = i
			if l_end < s.tables[tableName]['end'] and l_end > s.tables[tableName]['start']:
				end_server = i + 1
		for i in range(start_server, end_server):
			curr_ip = self.servers[i].ip
			if i == start_server:
				curr_start = start
			else:
				curr_start = self.servers[i].tables[tableName]['start']
			if i == end_server - 1:
				curr_end = end
			else:
				curr_end = self.servers[i].tables[tableName]['end']
			servers.append({'ip': curr_ip, 'start':curr_start, 'end':curr_end})
		return servers


	def mem_table_limit(self, client_name, tableName, newLimit):
		if client_name not in self.clients:
			print("{} has not registered".format(client_name))
			return False
		if tableName not in self.tables:
			print("{} does not exist".format(tableName))
			return ''
		success = True
		for s in self.servers:
			url = "http://{}:{}/".format(s.ip, PORT)
			with xmlrpc.client.ServerProxy(url) as curr_server:
				success = success and curr_server.memTableLimit(tableName, newLimit)
		return success


	def get_schema(self, client_name, tableName):
		if client_name not in self.clients:
			print("{} has not registered".format(client_name))
			return False
		if tableName not in self.tables:
			print("{} does not exist".format(tableName))
			return False
		return self.servers[0].ip

	def update_count(self, tableName, server_num):
		self.servers[server_num].tables[tableName]['count'] += 1
		self._update_servers()

	def _init_ranges(self, tableName):
		"""
		Make assumption that keys will begin either with capital
		or lower case letter
		TODO: make this a check
		send keys starting with "A" and 'a' to the same place
		therefore 26 letters to split (assuming uniform dist to start)
		"""
		total_range = ord('z') - ord('a') + 2
		range_per_server = total_range // self.num_servers
		start = ord('a')
		tables = []
		for i in range(self.num_servers):
			tables.append({'name': tableName,
							'start': chr(start + i*range_per_server),
							'end': chr(start + (i+1) * range_per_server + 1),
							'count': 0
							})
		for i, server in enumerate(self.servers):
			server.add_tables([tables[i]])

	def _load_data(self):
		with open(self.server_file, 'r') as f:
			l = json.load(f)
		for s in l:
			self.servers.append(Server('', s))
		with open(self.tables_file, 'r') as f:
			self.tables = json.load(f)

	def _update_servers(self):
		l = []
		for s in self.servers:
			l.append(s.get_obj())
		with open(self.server_file, 'w') as f:
			json.dump(l, f)

	def _update_tables(self):
		with open(self.tables_file, 'w') as f:
			json.dump(self.tables, f)


def main():
	mapper = Map()
	server = SimpleXMLRPCServer((mapper.ip, mapper.port))
	# register_functions(server, mapper)
	server.register_instance(mapper)
	server.serve_forever()

def register_functions(server, mapper):
	server.register_function(mapper.get_server, 'get_server')
	server.register_function(mapper.get_rows, 'get_rows')
	server.register_function(mapper.open_table, 'open_table')
	server.register_function(mapper.close_table, 'close_table')
	server.register_function(mapper.create_table, 'create_table')
	server.register_function(mapper.destroy_table, 'destroy_table')
	server.register_function(mapper.register_client, 'register_client')
	server.register_function(mapper.unregister_client, 'unregister_client')
	server.register_function(mapper.get_schema, 'get_schema')
	server.register_function(mapper.mem_table_limit, 'mem_table_limit')


if __name__ == '__main__':
	main()
