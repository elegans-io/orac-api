#!/usr/bin/env bash

PORT=${1:-8888}
INDEX_NAME=${2:-index_0}
curl -v -H "Authorization: Basic `echo -n 'test_user:p4ssw0rd' | base64`" \
  -H "Content-Type: application/json" -X POST http://localhost:${PORT}/${INDEX_NAME}/orac_user -d '
{
  "id": "d290f1ee-6c54-4b01-90e6-d701748f0851",
  "birthdate": 1509460536000,
  "birthplace": {"lat": 41.589, "lon": 28.59},
  "livingplace": {"lat": 41.589, "lon": 28.59},
  "name": "Mario Rossi",
  "gender": "male",
  "email": "user@example.com",
  "phone": "string",
  "tags": [
    "premium_user",
    "early_adopter"
  ]
}' 

