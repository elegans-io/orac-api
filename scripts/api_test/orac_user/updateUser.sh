#!/usr/bin/env bash

PORT=${1:-8888}
INDEX_NAME=${2:-index_0}
curl -v -H "Authorization: Basic `echo -n 'test_user:p4ssw0rd' | base64`" \
  -H "Content-Type: application/json" -X PUT http://localhost:${PORT}/${INDEX_NAME}/orac_user/d290f1ee-6c54-4b01-90e6-d701748f0851 -d'
{
  "birthdate": 1509460536,
  "name": "Mario Rossi",
  "gender": "male",
  "email": "user@example.com",
  "phone": "12345678910",
  "tags": [
    "premium_user",
    "early_adopter"
  ]
}' 

