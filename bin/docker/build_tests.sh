#!/usr/bin/env sh

# this command builds the initial tests environment
# you can then make changes to the code and run bin/docker/run_tests.sh to re-run quickly

bin/docker/build.sh
docker-compose build tests
