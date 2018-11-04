class Memindex:
	def __init__(self, tablename, yindex):
		self.count = 0
		self.yindex = yindex
		self.name = tablename
		self.entries = []
		self.populate()


	def populate(self):
		self.entries = []
		rows = self.yindex.getAll()
		for r in rows:
			self.entries.append({'key': r[0], 'offset': r[1], 'size': r[2]})
		print("Updated in-memory index")

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

	# TODO: make efficient search for sorted entries (binary)
	def _check(self, rowKey):
		for i, entry in enumerate(self.entries):
			if entry['key'] == rowKey:
				return i
		return -1

	# # TODO: make efficient search for sorted entries (binary)
	# # pre: self.entries already sorted in ascending order
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