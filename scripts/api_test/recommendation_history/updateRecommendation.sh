#!/usr/bin/env bash

PORT=${1:-8888}
INDEX_NAME=${2:-index_0}
curl -v -H "Authorization: Basic `echo -n 'admin:adminp4ssw0rd' | base64`" \
  -H "Content-Type: application/json" -X PUT "http://localhost:${PORT}/${INDEX_NAME}/recommendation_history/a6494c3521120a2853cb7700d5790dfdd4e98821754f30ae838b6c7a067b6d7a54117af585004be8fcc4c7f74c5da31c0e6c56432329c19e4125242d8ef2ee34" -d '{ 
          "recommendation_id" : "d290f1ee-6c54-4b01-90e6-d701748f0852",
          "access_user_id" : "test_user",
          "name" : "like",
          "generation_batch" : "test",
          "user_id" : "d290f1ee-6c54-4b01-90e6-d701748f0851",
          "item_id" : "d290f1ee-6c54-4b01-90e6-d701748f0851",
          "generation_timestamp" : 1509460536,
          "access_timestamp" : 1511619954,
          "score" : 2.3232
}'

