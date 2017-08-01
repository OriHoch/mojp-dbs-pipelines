#!/usr/bin/env sh

mkdir -p .docker/.data/redis
mkdir -p .docker/.data/db
mkdir -p .docker/.data/elasticsearch
mkdir -p .docker/.data/.cache
docker-compose up -d "${*:-app}"
