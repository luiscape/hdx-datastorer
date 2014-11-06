#!/bin/bash

# activating the virtualenv
source venv/bin/activate

# creating / updating datastore
python code/create-datastore.py https://data.hdx.rwlabs.org API_KEY

# deactivating
deactivate