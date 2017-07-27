#!/bin/sh

echo "waiting 1 second to let elasticsearch start"
sleep 1

bin/test.sh "$*"
