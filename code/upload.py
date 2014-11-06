import os
import csv
import json

import ckanapi

remote = 'http://data.hdx.rwlabs.org'
APIKey = 'XXXXX'

# ckan will be an instance of ckan api wrapper
ckan = None

resources = [
    {
        'resource_id': 'f48a3cf9-110e-4892-bedf-d4c1d725a7d1',
        'path': 'ebola-data-db-format.csv',
        'schema': {
            "fields": [
              { "id": "Indicator", "type": "text" },
              { "id": "Country", "type": "text" },
              { "id": "Date", "type": "timestamp"},
              { "id": "value", "type": "float" }
            ]
        },
    }
]

# #########################
# Code

def upload_data_to_datastore(ckan_resource_id, resource):
    # let's delete any existing data before we upload again
    try:
        ckan.action.datastore_delete(resource_id=ckan_resource_id)
    except:
        pass

    ckan.action.datastore_create(
            resource_id=ckan_resource_id,
            force=True,
            fields=resource['schema']['fields'],
            primary_key=resource['schema'].get('primary_key'))

    reader = csv.DictReader(open(resource['path']))
    rows = [ row for row in reader ]
    chunksize = 10000
    offset = 0
    print('Uploading data for file: %s' % resource['path'])
    while offset < len(rows):
        rowset = rows[offset:offset+chunksize]
        ckan.action.datastore_upsert(
                resource_id=ckan_resource_id,
                force=True,
                method='insert',
                records=rowset)
        offset += chunksize
        print('Done: %s' % offset)


import sys
if __name__ == '__main__':
    if len(sys.argv) <= 2:
        usage = '''python scripts/upload.py {ckan-instance} {api-key}

e.g.

python scripts/upload.py http://datahub.io/ MY-API-KEY
'''
        print(usage)
        sys.exit(1)

    remote = sys.argv[1]
    apikey = sys.argv[2]
    ckan = ckanapi.RemoteCKAN(remote, apikey=apikey)

    resource = resources[0]
    upload_data_to_datastore(resource['resource_id'], resource)
