import copy

class Memtable:

	def __init__(self, tableName, limit=25):
		self.limit = 25
		self.count = 0
		self.name = tableName
		self.entries = []

	def put(self, rowKey, columns):
		idx = self._check(rowKey)
		if idx == -1:
			self.entries.append({'key': rowKey, 'val': columns})
			self.count += 1
		else:
			self.entries[idx] = {'key': rowKey, 'val': columns}
		self.entries.sort(key=lambda x: x['key'])
		if self.count > self.limit:
			return True
		return False

	def get(self, rowKey):
		idx = self._check(rowKey)
		if idx == -1:
			return ''
		return self.entries[idx]


	def getRange(self, start, end):
		start_idx = self._checkRange(start, low=True)
		end_idx = self._checkRange(end, low=False)
		hits = self.entries[start_idx:end_idx+1]
		return hits


	def setLimit(self, newLimit):
		self.limit = newLimit
		return True

	def flush(self):
		temp = copy.deepcopy(self.entries)
		self.entries = []
		self.count = 0
		return temp


	# TODO: make efficient search for sorted entries (binary)
	# pre: self.entries already sorted in ascending order
	# def _check(self, rowKey):
	# 	max_index = len(self.entries) - 1
	# 	min_index = 0
	# 	mid_index = min_index + (max_index - min_index) / 2
	# 	while max_index >= min_index:
	# 		if self.entries[mid_index]['key'] > rowKey:
	# 			max_index = mid_index - 1
	# 		elif self.entries[mid_index]['key'] < rowKey:
	# 			min_index = mid_index + 1
	# 		else:
	# 			return rowKey
	# 	return -1

	# def _checkRange(self, rowKey, low):
	# 	for i, entry in enumerate(self.entries):
	# 		if entry['key'] == rowKey:
	# 			return i
	# 		if low:
	# 			if entry['key'] < rowKey:
	# 				return i + 1
	# 		if not low:
	# 			if entry['key'] > rowKey:
	# 				return i - 1
	# 	# case where we are looking for upper bound but don't find it
	# 	if not low:
	# 		return len(self.entries) - 1
	# 	if low:
	# 		return 0

	# TODO: make efficient search for sorted entries (binary)
	def _check(self, rowKey):
		for i, entry in enumerate(self.entries):
			if entry['key'] == rowKey:
				return i
		return -1

	def _checkRange(self, rowKey, low):
		for i, entry in enumerate(self.entries):
			if entry['key'] == rowKey:
				return i
			if low:
				if entry['key'] > rowKey:
					return i
			if not low:
				if entry['key'] > rowKey:
					return i - 1
		# case where we are looking for upper bound but don't find it
		return len(self.entries)


