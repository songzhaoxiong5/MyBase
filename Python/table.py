import os
import json
from memtable import Memtable
from ystore import Ystore
from yindex import Yindex
from memindex import Memindex

class Table():

	def __init__(self, tableName, load=False):
		self.tableName = tableName
		self.memtable = Memtable(tableName)
		if load:
			self._load_meta()
			self.recover()
		else:
			self.entries = 0
			self.schema = {}
		self.ystore = Ystore(tableName)
		self.yindex = Yindex(tableName)
		self.memindex = Memindex(tableName, self.yindex)
		self._write_meta()

	def destroy(self):
		self.yindex.destroy()
		self.ystore.destroy()
		if os.path.exists("./WAL/{}.txt".format(self.tableName)):
			os.remove("./WAL/{}.txt".format(self.tableName))
		if os.path.exists("./meta/{}.txt".format(self.tableName)):
			os.remove("./meta/{}.txt".format(self.tableName))
		return True

	def close(self):
		self.spill()

	def putRow(self, rowKey, columns):
		self._write_WAL(rowKey, columns)
		memFull = self.memtable.put(rowKey, columns)
		self.entries += 1
		if memFull:
			self.spill()
		self.updateSchema(columns)
		return True

	def recover(self):
		if os.path.exists("./WAL/{}.txt".format(self.tableName)):
			with open("./WAL/{}.txt".format(self.tableName), 'r') as f:
				recovery_count = 0
				commands = []
				for line in f:
					commands.append(json.loads(line))
					recovery_count += 1
				for c in commands:
					self.putRow(c['rowKey'], c['columns'])
			if recovery_count > 0:
				print("Recovered {} entries from WAL".format(recovery_count))



	def getRow(self, rowKey):
		memFind = self.memtable.get(rowKey)
		if memFind:
			temp = {memFind['key']: memFind['val']}
			return temp	
		data = self.memindex.get(rowKey)
		# data = self.yindex.get(rowKey)
		if not data:
			print("Couldn't find row with key: {}".format(rowKey))
			return ''
		return self.ystore.get(data['offset'], data['size'])

	def getRows(self, startRow, endRow):
		memFind = self.memtable.getRange(startRow, endRow)
		# storeFind = self.yindex.getRange(startRow, endRow)
		storeFind = self.memindex.getRange(startRow, endRow)
		memFind_keys = set()
		only_store = []
		mem_temp = []
		for i in memFind:
			memFind_keys.add(i['key'])
			mem_temp.append({i['key']:i['val']})
		rows = mem_temp
		for s in storeFind:
			if not s['key'] in memFind_keys:
				only_store.append((s['offset'], s['size']))
		for t in only_store:
			rows.append(self.ystore.get(t[0], t[1]))
		return rows

	def getColumnByRow(self, rowKey, fam, quals):
		row = self.getRow(rowKey)
		row = row[rowKey]
		data = {fam:{}}
		if not fam in row:
			print("{} not present in row".format(fam))
			return ''
		for q in quals:
			if not q in row[fam]:
				print("{} not present in row".format(q))
				return ''
			data[fam][q] = row[fam][q]
		return data


	def setMemTableLimit(self, newLimit):
		return self.memtable.setLimit(newLimit)


	def updateSchema(self, columns):
		changes = False
		for fam in columns:
			if fam in self.schema:
				for col in columns[fam]:
					if not col in self.schema[fam]:
						changes = True
						self.schema[fam].append(col)
			else:
				self.schema[fam] = []
				for col in columns[fam]:
					changes = True
					self.schema[fam].append(col)
		if changes:
			self._write_meta()


	def open(self):
		self.memtable = Memtable(self.tableName)


	def close(self):
		self.spill()
		del self.memtable

	def printSchema(self):
		print("Schema for {}:".format(self.tableName))
		start = '['
		for fam in self.schema:
			s = '{' + "{}:\t".format(fam)
			for col in self.schema[fam]:
				s = "{}{}, ".format(s, col)
			start += s + '}, '
		start += ']'
		print(start)

	def spill(self):
		print("Spilling to disk")
		memTableContents = self.memtable.flush()
		idx_updates = self.ystore.store(memTableContents)
		self.yindex.update(idx_updates)
		self.memindex.populate()
		self._clear_WAL()

	def _load_meta(self):
		if os.path.exists('./meta/{}.txt'.format(self.tableName)):
			with open('./meta/{}.txt'.format(self.tableName), 'r') as f:
				self.entries = int(f.readline())
				self.schema = json.loads(f.readline())
		else:
			self.schema = {}
			self.entries = 0

	def _write_meta(self):
		if not os.path.isdir("./meta"):
			os.makedirs("./meta")
		with open('./meta/{}.txt'.format(self.tableName), 'w') as f:
			f.write(str(self.entries)+"\n")
			f.write(json.dumps(self.schema))

	def _write_WAL(self, rowKey, columns):
		if not os.path.isdir("./WAL"):
			os.makedirs("./WAL")
		with open("./WAL/{}.txt".format(self.tableName), 'a') as f:
			log = {'rowKey': rowKey, 'columns': columns}
			f.write(json.dumps(log) + "\n")


	def _clear_WAL(self):
		if os.path.exists("./WAL/{}.txt".format(self.tableName)):
			open('./WAL/{}.txt'.format(self.tableName), 'w').close()