from table import Table
# import utils

class MyBase:

	def __init__(self, distributed_mode=False, meta_file="meta.txt", fresh_start=False):
		"""
		Start a new session with the system. 
		distributed_mode: TBD
		meta_file: pointer to table meta_data, shouldn't be changed
		fresh_start: ignore all previous tables, mostly for testing purposes
		"""
		self.tables = set() # all tables in systems
		# sessions could be expanded to include all user ID's to ensure each user only
		# gets 1 session as well as track timeout
		self.tableSessions = {} # All tables open in memory
		self.meta_file = meta_file
		if not fresh_start:
			self._load_meta()


	def openTable(self, tableName):
		"""
		Explicit opening of table and loading into memory. TODO: expand to have timeout on lease
		"""
		if tableName not in self.tables:
			print("No table called {}, please verify name".format(tableName))
			return False
		if not tableName in self.tableSessions:
			# Create session in memory
			self.tableSessions[tableName] = {
				'count': 1, 
				'table_obj': Table(tableName)
				}
		else:
			# add new session for table already in memory
			self.tableSessions[tableName]['count'] += 1
		return True

	def closeTable(self, tableName):
		"""
		Explicit ending of user session, if all user sessions ended then flush from memory
		to persistent

		"""
		if not tableName in self.tableSessions:
			print("Table {} is not open and therefore can't be closed")
			return False
		self.tableSessions[tableName]['count'] -= 1
		print("Closed user session for {}, current user count {}".format(tableName, self.tableSessions[tableName]['count']))
		# Check if all sessions closed
		if self.tableSessions[tableName]['count'] == 0:
			self.tableSessions[tableName]['table_obj'].close()
			del self.tableSessions[tableName]
			print("Moved {} out of memory".format(tableName))
		return True

	def createTable(self, tableName, auto_open=True):
		"""
		Create a new table with name tableName, check if table already exists before
		doing so. If auto_open enabled (default) add a session and load into memory
		"""
		if tableName in self.tables:
			print("{} already exists".format(tableName))
			return False
		# add to table list
		print("Created table {}".format(tableName))
		self.tables.add(tableName)
		# update meta data for recovery
		self._update_meta()
		if auto_open:
			self.openTable(tableName)
		return True

	def destroyTable(self, tableName):
		"""
		If table is exists and is closed then remove all artifacts
		"""
		# Check open
		if tableName in self.tableSessions:
			print("{} is currently in use and can't be destroyed".format(tableName))
			return False
		# check exists (probably should be first)
		if not tableName in self.tables:
			print("{} doesn't exist and can't be destroyed".format(tableName))
			return False
		# Kill it
		deleted = self.tables[tableName].destroy()
		if deleted:
			print("{} was successfully destroyed".format(tableName))
			del self.tables[tableName]
			self._update_meta()
			return True
		else:
			print("Something went wrong with the destroy operation")
			return False

	# columns: {'familyName':{'columnName1' : value1, 'columnName2': value2}, 'fam2' ...}
	def putRow(self, tableName, rowKey, columns):
		"""
		Insert a row into table, column format will be a nested dictionary as shown above
		"""
		# Should abstract these checks to private function
		if not tableName in self.tables:
			print("{} doesn't exist".format(tableName))
			return False
		if not tableName in self.tableSessions:
			print("Table is not currently open, please call openTable() first")
			return False

		success = self.tableSessions[tableName]['table_obj'].putRow(rowKey, columns)
		if success:
			print("Successfully put row {}".format(rowKey))
			return True
		else:
			print("Something went wrong with put operation")
			return False

	def getRow(self, tableName, rowKey):
		"""
		Query the table on a row key
		"""
		if not tableName in self.tables:
			print("{} doesn't exist".format(tableName))
			return False
		if not tableName in self.tableSessions:
			print("Table is not currently open, please call openTable() first")
			return False

		row = self.tableSessions[tableName]['table_obj'].getRow(rowKey)
		if not row:
			print("Unable to complete get operation")
		return row

	def getRows(self, tableName, startRow, endRow):
		"""
		Query table on range of keys
		"""
		if not tableName in self.tables:
			print("{} doesn't exist".format(tableName))
			return False
		if not tableName in self.tableSessions:
			print("Table is not currently open, please call openTable() first")
			return False

		rows = self.tableSessions[tableName]['table_obj'].getRows(startRow, endRow)
		if not rows:
			print("Unable to complete get operation")
		return rows

	def getColumnByRow(self, tableName, rowKey, family, qualifiers):
		"""
		Get the specified columns from a particular row, note that only 1 column family is allowed
		but qualifiers is a list of columns in that family
		"""
		if not tableName in self.tables:
			print("{} doesn't exist".format(tableName))
			return False
		if not tableName in self.tableSessions:
			print("Table is not currently open, please call openTable() first")
			return False

		data = self.tableSessions[tableName]['table_obj'].getColumnByRow(rowKey, family, qualifiers)
		if not data:
			print("Something went wrong with getColumnByRow")
		return data

	# def getSchema(self, tableName):
	# 	"""
	# 	Unused for now, not sure its needed
	# 	"""
	# 	if not tableName in self.tables:
	# 		print("{} doesn't exist".format(tableName))
	# 		return False
	# 	self.tableSessions[tableName]['table_obj'].printSchema()

	def memTableLimit(self, tableName, newLimit):
		"""
		Set the new limit for the memtable for this table
		"""
		if not tableName in self.tables:
			print("{} doesn't exist".format(tableName))
			return False
		if not tableName in self.tableSessions:
			print("Table is not currently open, please call openTable() first")
			return False
		success = self.tableSessions[tableName]['table_obj'].setMemTableLimit(newLimit)
		if not success:
			print("Unable to set new memtable limit for {}".format(tableName))
			return False
		else:
			print("Set memtable limit for {} to {}".format(tableName, newLimit))
			return True

	def _load_meta(self):
		"""
		Populate the list of all tables stored in persistent meta (just names, nothing
		put in memory yet)
		"""
		with open(self.meta_file, 'r') as f:
			for line in f:
				name = line[:-1]
				self.tables.add(name)
		print("Loaded {} tables from stored meta".format(len(self.tables)))

	def check_membership(self, name, dictionary, key='name'):
		for i in dictionary:
			if name == dictionary[i][key]:
				return True
		return False

	def _update_meta(self):
		"""
		Update the persistent meta store with new table list
		"""
		with open(self.meta_file, 'w') as f:
			for t in self.tables:
				f.write(t+"\n")
