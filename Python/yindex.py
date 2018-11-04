import os
import sqlite3 as sql

class Yindex:
	
	def __init__(self, tableName, directory="./yindex/"):
		self.name = tableName
		self.dir = directory
		if not os.path.isdir(self.dir):
			os.makedirs(self.dir)
		self.path = "{}{}.db".format(self.dir, self.name)
		first_time = False
		if not os.path.exists(self.path):
			first_time = True
		self.conn = sql.connect(self.path)
		self.c = self.conn.cursor()
		if first_time:
			self.c.execute('''CREATE TABLE yindex (key text, offset integer, size integer)''')
			self.conn.commit()

	def get(self, rowKey):
		t = (rowKey,)
		self.c.execute("SELECT offset, size FROM yindex WHERE key=?", t)
		row = self.c.fetchone()
		return row


	def getRange(self, start, end):
		query_string = "SELECT * FROM yindex WHERE key BETWEEN '{}' and '{}' ORDER BY key DESC".format(start, end)
		self.c.execute(query_string)
		rows = self.c.fetchall()
		return rows


	def getAll(self):
		query_string = "SELECT * FROM yindex ORDER BY key ASC"
		self.c.execute(query_string)
		rows = self.c.fetchall()
		return rows

	# idxs = [{key: '', offset: 123, size: 123}]
	def update(self, idxs):
		for i in idxs:
			# HACK: will improve later
			query_string = "DELETE FROM yindex WHERE key='{}'".format(i['key'])
			self.c.execute(query_string)
			self.conn.commit()
			query_string = "INSERT INTO yindex (key, offset, size) VALUES('{}', {}, {}) ".format(i['key'], i['offset'], i['size'])
			self.c.execute(query_string)
			self.conn.commit()
			# query_string = "UPDATE yindex SET offset={}, size={} ".format(i['offset'], i['size'])
			# query_string += "WHERE key='{}'".format(i['key'])
			# self.c.execute(query_string)
			# self.conn.commit()

	# TODO: remove all files and artifacts on destroy
	def destroy(self):
		self.conn.close()
		if os.path.exists(self.path):
			os.remove(self.path)


