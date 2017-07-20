#!/usr/bin/env bash

if [ "${DOCKER_USERNAME}" != "" ] && [ "${DOCKER_PASSWORD}" != "" ]; then
    echo "Docker deployment"
    docker --version
    docker login -u="${DOCKER_USERNAME}" -p="${DOCKER_PASSWORD}"
    make docker-build
    make docker-push
fi

echo "Ssh deployment"
if [ ! -f ./deploy-mojp-dbs-pipelines.id_rsa ]; then
    if [ "${encrypted_debe730ce1aa_key}" != "" ] && [ "${encrypted_debe730ce1aa_iv}" != "" ]; then
        echo "decrypting the deploy key"
        openssl aes-256-cbc -K $encrypted_debe730ce1aa_key -iv $encrypted_debe730ce1aa_iv -in ./deploy-mojp-dbs-pipelines.id_rsa.enc -out ./deploy-mojp-dbs-pipelines.id_rsa -d
    else
        echo "missing required environment variables"
    fi
else
    echo "deploy key already exists"
fi

if [ -f ./deploy-mojp-dbs-pipelines.id_rsa ]; then
    # ssh requires private key to have limited permissions
    chmod 400 ./deploy-mojp-dbs-pipelines.id_rsa

    echo "deploying..."
    ssh -i ./deploy-mojp-dbs-pipelines.id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no bhs@devapi.dbs.bh.org.il
else
    echo "no deploy key file, skipping ssh deployment"
fi

echo "OK"
