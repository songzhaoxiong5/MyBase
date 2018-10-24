import os
import json

class Ystore:
	def __init__(self, tableName, directory="./ystore/"):
		self.name = tableName
		self.dir = directory
		if not os.path.isdir(self.dir):
			os.makedirs(self.dir)
		self.path = "{}{}.json".format(self.dir, self.name)


	# memtable = [{key: str, value:{}}]
	def store(self, memTable):
		updates = []
		with open(self.path, 'a') as f:
			currOffset = f.tell()
			for entry in memTable:
				temp = json.dumps({entry['key']: entry['val']})
				size = len(temp)
				f.write(temp)
				updates.append({'key':entry['key'], 'offset':currOffset, 'size':size})
				currOffset = f.tell()
		return updates

	def get(self, offset, size):
		with open(self.path, 'r') as f:
			f.seek(offset)
			temp = f.read(size)
		return json.loads(temp)

	# TODO: remove all artifacts
	def destroy(self):
		return True