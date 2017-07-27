#!/bin/sh

# this is run on dbs-back image

echo "ELASTICSEARCH_HOST=${ELASTICSEARCH_HOST}"
echo "MOJP_INDEX=${MOJP_INDEX}"

scripts/elasticsearch_create_index.py --host $ELASTICSEARCH_HOST --index $MOJP_INDEX $*
