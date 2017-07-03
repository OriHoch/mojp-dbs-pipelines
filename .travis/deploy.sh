#!/usr/bin/env bash

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

echo "deploying..."
ssh -i ./deploy-mojp-dbs-pipelines.id_rsa bhs@devapi.dbs.bh.org.il

echo "OK"
