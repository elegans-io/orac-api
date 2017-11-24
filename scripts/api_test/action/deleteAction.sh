#!/usr/bin/env bash

PORT=${1:-8888}
INDEX_NAME=${2:-index_0}
# delete one or more entries with given ids; ids can be specified multiple times
curl -v -H "Authorization: Basic `echo -n 'test_user:p4ssw0rd' | base64`" \
  -H "Content-Type: application/json" -X DELETE "http://localhost:${PORT}/${INDEX_NAME}/action/d290f1ee-6c54-4b01-90e6-d701748f0851"

