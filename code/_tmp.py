# Script to interact with the datastore
# on the Data Team's end.

import os
import csv
<<<<<<< HEAD
import sys
import json
import requests
import urllib
import hashlib
import scraperwiki
import ckanapi

# Collecting configuration variables
remote = 'http://data.hdx.rwlabs.org'
resource_id = sys.argv[1]
apikey = sys.argv[2]

# path to the locally stored CSV file
PATH = 'tool/data/topline-ebola-outbreak-figures.csv'
=======
import ckanapi

PATH = '/tmp/topline-ebola-outbreak-figures.csv'
>>>>>>> 2b519108fae43c10389a1c3c4e0e75a54e232e83

# ckan will be an instance of ckan api wrapper
ckan = None

<<<<<<< HEAD
# Function to download a resource from CKAN.
def downloadResource(filename):

    # querying
    url = 'https://data.hdx.rwlabs.org/api/action/resource_show?id=' + resource_id
    r = requests.get(url)
    doc = r.json()
    fileUrl = doc["result"]["url"]

    # downloading
    try:
        urllib.urlretrieve(fileUrl, filename)
    except:
        print 'There was an error downlaoding the file.'


# Function that checks for old SHA hash
# and stores as a SW variable the new hash
# if they differ. If this function returns true,
# then the datastore is created.
def checkHash(filename, first_run):
    hasher = hashlib.sha1()
    with open(filename, 'rb') as afile:
        buf = afile.read()
        hasher.update(buf)
        new_hash = hasher.hexdigest()

    # checking if the files are identical or if
    # they have changed
    if first_run:
        scraperwiki.sqlite.save_var('hash', new_hash)
        new_data = False

    else:
        old_hash = scraperwiki.sqlite.get_var('hash')
        scraperwiki.sqlite.save_var('hash', new_hash)
        new_data = old_hash != new_hash

    # returning a boolean
    return new_data


def updateDatastore(filename):

    # Checking if there is new data
    # pass True to the first_run parameter
    # if this is the first run.
    update_data = checkHash(filename, first_run = False)
    if (update_data == False):
        print "\nDataStore Status: No new data. Not updating datastore."
        return

    # proceed if the hash is different, i.e. update
    print "DataStore Status: New data. Updating datastore."
=======

def runDataStoreUpdater():

    # downloading original file
    os.system(
            'wget https://docs.google.com/spreadsheets/d/1LcGzK41O5xANVxTvympUwBBz_eioQJ7VJqzRh6r5XJc/export?format=csv -O ' + PATH)
>>>>>>> 2b519108fae43c10389a1c3c4e0e75a54e232e83

    # defining the schema
    # sample csv file for this schema can be found here: https://gist.github.com/alexandru-m-g/15365642de5926fec4f6
    resources = [
        {
<<<<<<< HEAD
            'resource_id': resource_id,
            'path': filename,
=======
            'resource_id': 'a02903a9-022b-4047-bbb5-45127b591c85',
            'path': '/tmp/topline-ebola-outbreak-figures.csv',
>>>>>>> 2b519108fae43c10389a1c3c4e0e75a54e232e83
            'schema': {
                "fields": [
                    {"id": "code", "type": "text"},
                    {"id": "title", "type": "text"},
                    {"id": "value", "type": "float"},
                    {"id": "latest_date", "type": "timestamp"},
                    {"id": "source", "type": "text"},
                    {"id": "source_link", "type": "text"},
                    {"id": "notes", "type": "text"},
                    {"id": "explore", "type": "text"},
                    {"id": "units", "type": "text"}
                ],
                "primary_key": "code"
            },
        }
    ]

<<<<<<< HEAD
    def upload_data_to_datastore(ckan_resource_id, resource, delete):

        # if the delete flag is True, then delete the current
        # datastore before creating a new one
        if delete:
            try:
                ckan.action.datastore_delete(resource_id=ckan_resource_id, force=True)
            except:
                pass
=======
    def upload_data_to_datastore(ckan_resource_id, resource):

        # let's delete any existing data before we upload again
        #         try:
        #             ckan.action.datastore_delete(
        #                 resource_id=ckan_resource_id, force=True)
        #         except:
        #             pass
>>>>>>> 2b519108fae43c10389a1c3c4e0e75a54e232e83

        ckan.action.datastore_create(
            resource_id=ckan_resource_id,
            force=True,
            fields=resource['schema']['fields'],
            primary_key=resource['schema'].get('primary_key'))

        reader = csv.DictReader(open(resource['path']))
        rows = [row for row in reader]
        chunksize = 10000
        offset = 0
        print('Uploading data for file: %s' % resource['path'])
        while offset < len(rows):
            rowset = rows[offset:offset + chunksize]
            ckan.action.datastore_upsert(
                resource_id=ckan_resource_id,
                force=True,
                method='upsert',
                records=rowset)
            offset += chunksize
            print('Done: %s' % offset)

<<<<<<< HEAD

    # if running as a command line script
    if __name__ == '__main__':
        if len(sys.argv) <= 2:
            usage = '''python scripts/create-datastore.py {ckan-resource-id} {api-key}
=======
    import sys
    if __name__ == '__main__':
        if len(sys.argv) <= 2:
            usage = '''python create-datastore.py {ckan-instance} {api-key}
>>>>>>> 2b519108fae43c10389a1c3c4e0e75a54e232e83

                    e.g.

<<<<<<< HEAD
                    python scripts/create-datastore.py CKAN_RESOURCE_ID API-KEY
                    '''
=======
    python create-datastore.py http://localhost:5000/ MY-API-KEY
    '''
>>>>>>> 2b519108fae43c10389a1c3c4e0e75a54e232e83
            print(usage)
            sys.exit(1)

        ckan = ckanapi.RemoteCKAN(remote, apikey=apikey)

        resource = resources[0]
        upload_data_to_datastore(resource['resource_id'], resource, delete=False)


# wrapper call for all functions
def runEverything(p):
    downloadResource(p)
    updateDatastore(p)

# ScraperWiki-specific error handler
try:
    runEverything(PATH)
    # if everything ok
    print "Everything seems to be just fine."
#     scraperwiki.status('ok')

except Exception as e:
    print e
#     scraperwiki.status('error', 'Creating datastore failed')
#     os.system(
#         "mail -s 'Ebola toplines: creating datastore failed.' luiscape@gmail.com")
