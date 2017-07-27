#!/usr/bin/env sh

set -e

docker pull orihoch/mojp-dbs-back
docker pull orihoch/mojp-pipelines-tests
docker pull orihoch/mojp-dbs-tests

docker tag orihoch/mojp-dbs-back mojp/dbs-back
docker tag orihoch/mojp-pipelines-tests mojp/pipelines-tests
docker tag orihoch/mojp-dbs-tests mojp/pipelines-app
