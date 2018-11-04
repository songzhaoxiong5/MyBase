import csv
from client import Client

def populate_marriages(filepath):
    lists = []
    with open(filepath, encoding='mac_roman') as f:
        reader = csv.reader(f)
        for row in reader:
            lists.append(row)
    client = Client()
    client.createTable("Marriages")
    client.openTable("Marriages")
    client.memTableLimit("Marriages", 25)
    for l in lists:
        key = l[0]
        col = {'date': {'year': l[1]}, 'Perc ever married between 15-19': {'women': l[2], 'men': l[3]},
               'mean marriage age': {'women': l[4], 'men': l[5]}, 'misc': {'source': l[6]}}
        client.putRow("Marriages", key, col)
    client.closeTable("Marriages")


populate_marriages("../Data/Marriages.csv")
