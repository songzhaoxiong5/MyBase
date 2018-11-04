from MyBase import MyBase
from client import Client
import json

def main():
	base = Client()
	response = ['']
	print("Welcome to MyBase!!!")
	print("Basic Commands:")
	print("API - display all API calls and usage")
	print("exit/quit - end the program")
	while response[0] != "exit" and response[0] != 'quit':
		response = input(">>> ").split(',')
		response[0] = response[0].lower()
		clearLeadingSpaces(response)
		if response[0] == "api":
			printAPI()
		elif response[0] == 'getschema':
			if checkResponse(response, 2):
				base.getSchema(response[1])

		elif response[0] == "createtable":
			if checkResponse(response, 2):
				autoOpen = False
				base.createTable(response[1])
			
		elif response[0] == 'destroytable':
			if checkResponse(response, 2):
				base.destroyTable(response[1])
			
		elif response[0] == 'opentable':
			if checkResponse(response, 2):
				base.openTable(response[1])
			
		elif response[0] == 'closetable':
			if checkResponse(response, 2):
				base.closeTable(response[1])
			
		elif response[0] == 'putrow':
			if checkResponse(response, 4):
				temp = ','.join(response[3:])
				columns = json.loads(temp)
				base.putRow(response[1], response[2], columns)
			
		elif response[0] == 'getrow':
			if checkResponse(response, 3):
				print(base.getRow(response[1], response[2]))
			
		elif response[0] == 'getrows':
			if checkResponse(response, 4):
				print(base.getRows(response[1], response[2], response[3]))
			
		elif response[0] == 'getcolumnbyrow':
			if checkResponse(response, 5):
				temp = ','.join(response[5:])
				quals = json.loads(temp)
				print(base.getColumnByRow(response[1], response[2], response[3], quals))
			
		elif response[0] == 'memtablelimit':
			if checkResponse(response, 3):
				quals = json.loads(response[5])
				base.memTableLimit(response[1], int(response[2]))
		elif response[0] == "exit" or response[0] == 'quit':
			continue
		else:
			print("Invalid command, please consult API:")
			printAPI()

	base.close()


def clearLeadingSpaces(response):
	for i, r in enumerate(response):
		if r[0] == ' ':
			response[i] = r[1:]

def printAPI():
	print("API (commands are not case sensitive):")
	print("All arguments are comma-separated, leading space will be ignores eg. 'opentable, test table' = 'opentable,test table'")
	print("<> is compulsory. [] is optional")
	print("createTable, <tableName>, [autoOpen] - create a new table with option to automatically open. \n\ttableName: string\n\tautoOpen: bool")
	print("destroyTable, <tableName> - destroy the table and any related artifacts.\n\ttableName: string")
	print("openTable, <tableName> - open a session with the table.\n\ttableName: string")
	print("closeTable, <tableName> - close the session with the table.\n\ttableName: string")
	print("putRow, <tableName>, <rowKey>, <columns> - insert a new key value pair into the table. \n\ttableName: string\n\trowKey: string\n\tcolumns: json-type object using double quotes eg. {\"famname\":{\"qualname\":\"qualval\", \"qualname2\":\"qualval2\"}}")
	print("getRow, <tableName>, <rowKey> - fetch a row from the table with the rowkey.\n\ttableName: string\n\trowKey: string")
	print("getRows, <tableName>, <startKey>, <endKey> - fetch all the rows within the specified range of keys.\n\ttableName: string\n\tstartKey: string\n\tendKey: string")
	print("getColumnByRow, <tableName>, <rowKey>, <family>, <qualifiers> - get the specific columns with the given family for the provided key. \n\ttableName: string\n\trowKey: string\n\tfamily - string\n\tqualifiers - list of string eg. ['first', 'last']")
	print("memTableLimit, <tableName>, <limit> - set the new limit for the number of entries in the memtable for the given table.\n\ttableName: string\n\tlimit: integer")
	print("getSchema, <tableName> - view the current schema for the table.\n\ttableName: string")

def checkResponse(response, i):
	if len(response) < i:
		print("Invalid Command, please consult API:")
		printAPI()
		return False
	return True

if __name__ == '__main__':
	main()