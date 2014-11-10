# Series of Scripts Push data to CKAN's DataStore
The scripts in this repository were designed to:

- Download a CSV file from a particular CKAN resource
- Parse that CSV file based on a defined schema
- Push that file into CKAN's DataStore

The script uses a bit of a **brute force** approach as of updating the DataStore. Due to the heterogeneity of resources on CKAN (sometimes they are Google Docs, sometimes files updated to the FileStore), I decided to download the original file at each run, parse, and send it over to the DataStore for that particular resource.

The script checks if a DataStore already exists. If it does, it deletes the old DataStore and replaces it with a new one.
