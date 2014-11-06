#!/bin/bash

# creating virtual environment
virtualenv venv
source venv/bin/activate

# installing dependencies
pip install ckanapi

# deactivating
deactivate