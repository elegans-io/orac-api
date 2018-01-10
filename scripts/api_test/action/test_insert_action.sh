#!/usr/bin/env bash

PORT=${1:-8888}
INDEX_NAME=${2:-index_0}

curl -v -H "Authorization: Basic `echo -n 'test_user:p4ssw0rd' | base64`" \
  -H "Content-Type: application/json" -X POST http://localhost:${PORT}/${INDEX_NAME}/action -d '{
  "name": "like",
  "user_id": "user1",
  "item_id": "item1",
  "score": 1.0
}' 

curl -v -H "Authorization: Basic `echo -n 'test_user:p4ssw0rd' | base64`" \
  -H "Content-Type: application/json" -X POST http://localhost:${PORT}/${INDEX_NAME}/action -d '{
  "name": "like",
  "user_id": "user2",
  "item_id": "item3",
  "score": 1.0
}' 

curl -v -H "Authorization: Basic `echo -n 'test_user:p4ssw0rd' | base64`" \
  -H "Content-Type: application/json" -X POST http://localhost:${PORT}/${INDEX_NAME}/action -d '{
  "name": "like",
  "user_id": "user3",
  "item_id": "item4",
  "score": 1.0
}' 

curl -v -H "Authorization: Basic `echo -n 'test_user:p4ssw0rd' | base64`" \
  -H "Content-Type: application/json" -X POST http://localhost:${PORT}/${INDEX_NAME}/action -d '{
  "name": "like",
  "user_id": "user4",
  "item_id": "item4",
  "score": 1.0
}' 

