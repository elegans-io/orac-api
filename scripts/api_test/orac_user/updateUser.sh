#!/usr/bin/env bash

PORT=${1:-8888}
INDEX_NAME=${2:-index_english_0}
curl -v -H "Authorization: Basic `echo -n 'test_user:p4ssw0rd' | base64`" \
  -H "Content-Type: application/json" \
  -X PUT "http://localhost:${PORT}/${INDEX_NAME}/orac_user/d290f1ee-6c54-4b01-90e6-d701748f0851" -d'
{
  "name": "Mario Rossi",
  "email": "user@example.com",
  "phone": "string",
  "props": {
    "string": [
      {
        "key": "address",
        "value": "3 Abbey Road, London NW8 9AY, UK"
      }
    ],
    "geopoint": [
      {
        "key": "livingplace",
        "value": {"lat": 41.1, "lon":31.2}
      }
    ],
    "tags": [
      "premium_user",
      "early_adopter"
    ],
    "timestamp": [
      {
        "key": "birthdate",
        "value": 1511943854000
      }
    ]
  }
}'
