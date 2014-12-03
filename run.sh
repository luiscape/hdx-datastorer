#!/bin/bash

# activating the virtualenv
source ~/venv/bin/activate

# creating / updating datastore
python ~/tool/code/create-datastore.py RESOURCE_ID API_KEY
