#!/usr/bin/env sh

docker build -t mojp/dbs-back https://github.com/beit-hatfutsot/dbs-back.git
docker-compose build elasticsearch-index
docker-compose run elasticsearch-index --force
