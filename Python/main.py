from MyBase import MyBase

def main():
	base = MyBase(fresh_start=False)
	# base.createTable("Second Table")
	base.openTable("Marriages")
	# base.memTableLimit("Second Table", 1)
	# base.putRow("Second Table", 'bstocks', {'name':{'first': 'Brad', 'last': 'stocks'}})	
	# base.putRow("Second Table", 'dzhang', {'name':{'first': 'dave', 'last': 'zhang'}})	
	# base.putRow("Second Table", 'alynn', {'name':{'first': 'stacey', 'last': 'lynn'}})	
	print(base.getRows("Marriages", 'V', 'Zb'))
	base.closeTable("Marriages")


if __name__ == '__main__':
	main()