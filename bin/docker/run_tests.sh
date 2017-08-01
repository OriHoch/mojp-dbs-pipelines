#!/usr/bin/env sh

docker-compose build tests
docker-compose run tests "$*"
