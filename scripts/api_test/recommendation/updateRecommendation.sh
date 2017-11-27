#!/usr/bin/env bash

PORT=${1:-8888}
INDEX_NAME=${2:-index_0}
curl -v -H "Authorization: Basic `echo -n 'test_user:p4ssw0rd' | base64`" \
  -H "Content-Type: application/json" -X PUT "http://localhost:${PORT}/${INDEX_NAME}/recommendation/d290f1ee-6c54-4b01-90e6-d701748f0851" -d '
{
  "id": "d290f1ee-6c54-4b01-90e6-d701748f0851",
  "user_id": "d290f1ee-6c54-4b01-90e6-d701748f0851",
  "item_id": "d290f1ee-6c54-4b01-90e6-d701748f0851",
  "name": "click",
  "generation_timestamp": 1509460536000,
  "score": 1.3232
}' 

