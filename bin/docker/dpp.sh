#!/usr/bin/env sh

. bin/docker/constants.sh

docker exec -it $MOJP_PIPELINES_APP_CONTAINER dpp "$@"
