#!/bin/sh

until curl -f $TESTS_MOJP_ELASTICSEARCH_DB; do
  >&2 echo "Elasticsearch is unavailable - sleeping 1 second"
  sleep 1
done

bin/test.sh "$*"
