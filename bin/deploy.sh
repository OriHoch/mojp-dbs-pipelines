#!/usr/bin/env bash

git pull origin ${DEPLOY_BRANCH:-master}
bin/docker/build_all.sh
bin/docker/start.sh
