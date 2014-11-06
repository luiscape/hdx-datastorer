import os
import csv
import json
import scraperwiki
import ckanapi

remote = 'http://data.hdx.rwlabs.org'
APIKey = 'XXXXX'

# ckan will be an instance of ckan api wrapper
ckan = None

def runDataStoreUpdater():

    # downloading original file
    os.system('wget https://docs.google.com/spreadsheets/d/1LcGzK41O5xANVxTvympUwBBz_eioQJ7VJqzRh6r5XJc/export?format=csv -O data/topline-ebola-outbreak-figures.csv')


    # defining the schema
    resources = [
        {
            'resource_id': 'a02903a9-022b-4047-bbb5-45127b591c85',
            'path': 'data/topline-ebola-outbreak-figures.csv',
            'schema': {
                "fields": [
                  { "id": "title", "type": "text" },
                  { "id": "value", "type": "float" },
                  { "id": "latest_date", "type": "timestamp"},
                  { "id": "source", "type": "text" },
                  { "id": "source_link", "type": "text"},
                  { "id": "notes", "type": "text" },
                  { "id": "explore", "type": "text" }
                ]
            },
        }
    ]


    def upload_data_to_datastore(ckan_resource_id, resource):
        # let's delete any existing data before we upload again
        try:
            ckan.action.datastore_delete(resource_id=ckan_resource_id, force=True)
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

try:
    runDataStoreUpdater()
    # if everything ok
    print "Everything seems to be just fine."
    scraperwiki.status('ok')

except Exception as e:
    print e
    scraperwiki.status('error', 'Creating datastore failed')
    os.system("mail -s 'Ebola toplines: creating datastore failed.' luiscape@gmail.com")
