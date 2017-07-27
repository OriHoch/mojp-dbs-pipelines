#!/usr/bin/env sh

# local install for development / testing

pip install -r requirements.txt
pip install --upgrade -e .[develop]
