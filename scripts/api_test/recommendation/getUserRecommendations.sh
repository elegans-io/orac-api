#!/usr/bin/env bash

PORT=${1:-8888}
INDEX_NAME=${2:-index_0}

curl -v -H "Authorization: Basic `echo -n 'test_user:p4ssw0rd' | base64`" \
  -H "Content-Type: application/json" -X GET "http://localhost:${PORT}/${INDEX_NAME}/user_recommendation/d290f1ee-6c54-4b01-90e6-d701748f0851"

