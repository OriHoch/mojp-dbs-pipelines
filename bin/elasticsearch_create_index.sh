#!/usr/bin/env bash

docker run -it orihoch/mojp-dbs-back scripts/elasticsearch_create_index.py ${*:---help}

