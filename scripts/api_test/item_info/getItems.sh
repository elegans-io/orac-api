#!/usr/bin/env bash

PORT=${1:-8888}
INDEX_NAME=${2:-index_english_0}
# retrieve one or more entries with given ids; ids can be specified multiple times
curl -v -H "Authorization: Basic `echo -n 'test_user:p4ssw0rd' | base64`" \
  -H "Content-Type: application/json" "http://localhost:${PORT}/${INDEX_NAME}/item_info?id=csrec_0_4_1"


