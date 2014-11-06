# Tests for the datastore component.
import requests as r
import json
import csv
import urllib
import sys
import os
import pandas as pd
import scraperwiki

print('------------------------------------------------')
print('Performing tests:')
print('------------------------------------------------')

resource_id = sys.argv[1]
api_key = sys.argv[2]

# Check if the id of the resource has changed.
def checkID(id, key):

	url = 'https://data.hdx.rwlabs.org/api/action/package_show?id=topline-ebola-outbreak-figures'
	h = {'Authorization': key}
	doc = r.get(url, headers=h);
	data = doc.json();
	new_id = data["result"]["resources"][0]["id"]

	if new_id != id:
		print "ID Check: FAIL"

	else:
		print "ID Check: PASS"

# check if the dataset has one file
def checkNFiles(id, key):

	url = 'https://data.hdx.rwlabs.org/api/action/package_show?id=topline-ebola-outbreak-figures'
	h = {'Authorization': key}
	doc = r.get(url, headers=h)
	data = doc.json()
	nfiles = len(data["result"]["resources"])

	if nfiles == 1:
		print "Number of Files Check: PASS"

	else:
		print "Numbe of Files Check: FAIL"

checkNFiles(resource_id, api_key)

# check if the file has the right name
def checkFileName(id, key):

	url = 'https://data.hdx.rwlabs.org/api/action/package_show?id=topline-ebola-outbreak-figures'
	h = {'Authorization': key}
	doc = r.get(url, headers=h)
	data = doc.json()
	file_name = data["result"]["resources"][0]["name"]

	if file_name == 'topline-ebola-outbreak-figures.csv':
		print "File Name Check: PASS"

	else:
		print "File Name Check: FAIL"

checkFileName(resource_id, api_key)


# check if the datastore is active
def checkDataStore(id, key):

	url = 'https://data.hdx.rwlabs.org/api/action/package_show?id=topline-ebola-outbreak-figures'
	h = {'Authorization': key}
	doc = r.get(url, headers=h)
	data = doc.json()
	ds = data["result"]["resources"][0]["datastore_active"]

	if ds:
		print "DataStore Active Check: PASS"

	else:
		print "DataStore Active Check: FAIL"

# check if the datastore has the same length of results
def checkRecords(key):

	# downloading original file
	fileUrl = 'https://docs.google.com/spreadsheets/d/1LcGzK41O5xANVxTvympUwBBz_eioQJ7VJqzRh6r5XJc/export?format=csv'
	urllib.urlretrieve(fileUrl, "data.csv")

	# reading file
	df = pd.read_csv('data.csv')
	row_count = len(df)

	# getting data from datastore
	ds_query = 'https://data.hdx.rwlabs.org/api/action/datastore_search?resource_id=a02903a9-022b-4047-bbb5-45127b591c85&amp;limit=100'
	h = {'Authorization': key}
	doc = r.get(ds_query, headers=h)
	data = doc.json()
	data_count = len(data["result"]["records"])

	if row_count == data_count:
		print "Same Number of Rows Check: PASS"

	else:
		print "Same Number of Rows Check: FAIL"
		print "CSV File: " + str(row_count) + " | " + "DataStore: " + str(data_count)


# check if a single query is the same
# as the datapoint of the original file
def checkValue(key):

	# downloading original file
	fileUrl = 'https://docs.google.com/spreadsheets/d/1LcGzK41O5xANVxTvympUwBBz_eioQJ7VJqzRh6r5XJc/export?format=csv'
	urllib.urlretrieve(fileUrl, "data.csv")

	# reading file
	df = pd.read_csv('data.csv')
	file_value = df["value"][5]

	# getting data from datastore
	ds_query = 'https://data.hdx.rwlabs.org/api/action/datastore_search?resource_id=a02903a9-022b-4047-bbb5-45127b591c85&amp;limit=100'
	h = {'Authorization': key}
	doc = r.get(ds_query, headers=h)
	data = doc.json()
	ds_value = data["result"]["records"][5]["value"]

	if file_value == ds_value:
		print "Same Value: PASS"
		print('------------------------------------------------')

	else:
		print "Same Value: FAIL"
		print "File: " + str(file_value) + " | " + "DataStore: " + str(ds_value)
		print('------------------------------------------------')


# running functions
def runAllTests():

	try:
		checkID(resource_id, api_key)
		checkDataStore(resource_id, api_key)
		checkRecords(api_key)
		checkValue()

		# if everything ok
		print "All tests passed."
		scraperwiki.status('ok')

	except Exception as e:
		print e
		scraperwiki.status('error', 'Tests failed')
		os.system("mail -s 'Ebola toplines: tests failed.' luiscape@gmail.com")


runAllTests()


