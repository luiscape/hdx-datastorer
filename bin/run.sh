#!/bin/bash

#
# Configuration.
#
API_KEY="FOO"
RESOURCE_ID="BAR"

#
# Running script.
#
source venv/bin/activate
python script/create-datastore.py $RESOURCE_ID $API_KEY
