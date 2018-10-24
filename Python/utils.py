import csv
from MyBase import MyBase

def populate_marriages(filepath):
	lists = []
	with open(filepath, encoding='mac_roman') as f:
		reader = csv.reader(f)
		for row in reader:
			lists.append(row)
	base = MyBase(fresh_start=True)
	base.createTable("Marriages", auto_open=False)
	base.openTable("Marriages")
	base.memTableLimit("Marriages", 25)
	for l in lists:
		key = l[0]
		col = {'date':{'year':l[1]}, 'Perc ever married between 15-19':{'women':l[2], 'men':l[3]},
				'mean marriage age': {'women':l[4], 'men':l[5]}, 'misc':{'source':l[6]}}
		base.putRow("Marriages", key, col)
	base.closeTable("Marriages")
	

populate_marriages("../Data/Marriages.csv")

