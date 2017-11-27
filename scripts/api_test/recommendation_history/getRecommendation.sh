#!/usr/bin/env bash

PORT=${1:-8888}
INDEX_NAME=${2:-index_0}
# retrieve one or more entries with given ids; ids can be specified multiple times
curl -v -H "Authorization: Basic `echo -n 'admin:adminp4ssw0rd' | base64`" \
  -H "Content-Type: application/json" "http://localhost:${PORT}/${INDEX_NAME}/recommendation_history?id=a6494c3521120a2853cb7700d5790dfdd4e98821754f30ae838b6c7a067b6d7a54117af585004be8fcc4c7f74c5da31c0e6c56432329c19e4125242d8ef2ee34"

