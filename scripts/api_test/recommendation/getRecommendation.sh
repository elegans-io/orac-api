#!/usr/bin/env bash

PORT=${1:-8888}
INDEX_NAME=${2:-index_0}
# retrieve one or more entries with given ids; ids can be specified multiple times
curl -v -H "Authorization: Basic `echo -n 'test_user2:p4ssw0rd2' | base64`" \
  -H "Content-Type: application/json" "http://localhost:${PORT}/${INDEX_NAME}/recommendation?ids=d290f1ee-6c54-4b01-90e6-d701748f0851&ids=d290f1ee-6c54-4b01-90e6-d701748f0852"

