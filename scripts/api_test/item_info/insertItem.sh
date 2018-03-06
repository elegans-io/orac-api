#!/usr/bin/env bash

PORT=${1:-8888}
INDEX_NAME=${2:-index_english_0}
curl -v -H "Authorization: Basic `echo -n 'test_user:p4ssw0rd' | base64`" \
  -H "Content-Type: application/json" -X POST http://localhost:${PORT}/${INDEX_NAME}/item_info -d '
{
  "id": "csrec_0_4_1",
  "base_fields": ["category"],
  "tag_filters": ".*",
  "numerical_filters": "",
  "string_filters": "author|category",
  "timestamp_filters": "",
  "geopoint_filters": ""
}' 

