from MyBase import MyBase

def main():
	base = MyBase(fresh_start=False)
	# base.createTable("First Table")
	base.openTable("First Table")
	# base.memTableLimit("Second Table", 1)
	base.putRow("First Table", 'dzhang', {'name':{'first': 'dave', 'last': 'zhang'}})	
	# base.putRow("Second Table", 'dzhang', {'name':{'first': 'dave', 'last': 'zhang'}})	
	# base.putRow("Second Table", 'alynn', {'name':{'first': 'stacey', 'last': 'lynn'}})	
	# print(base.getRows("Marriages", 'V', 'Zb'))
	# print(base.getRow("Marriages", "Zimbabwe"))
	# print(base.getColumnByRow("Marriages", "Zimbabwe", 'mean marriage age', ['women', 'men']))
	# base.closeTable("First Table")
	# base.destroyTable("First Table")


if __name__ == '__main__':
	main()