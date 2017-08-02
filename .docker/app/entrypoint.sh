#!/bin/sh

# tests directory has some pipelines which we do need in the docker for tests, but don't want to appear in pipelines
# this is the best way to do it - tests app uses a different entrypoint, so tests directory will remain
rm -rf tests/

if [ "${1}" == "" ]; then
    dpp init
    rm -f *.pid
    python3 -m celery -b "redis://${DPP_REDIS_HOST}:6379/6" -A datapackage_pipelines.app -l INFO beat &
    python3 -m celery -b "redis://${DPP_REDIS_HOST}:6379/6" --concurrency=1 -A datapackage_pipelines.app -Q datapackage-pipelines-management -l INFO worker &
    python3 -m celery -b "redis://${DPP_REDIS_HOST}:6379/6" --concurrency=4 -A datapackage_pipelines.app -Q datapackage-pipelines -l INFO worker &
    dpp serve
else
    /bin/sh -c "$*"
fi
