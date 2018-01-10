#!/usr/bin/env bash

PORT=${1:-8888}
INDEX_NAME=${2:-index_0}
curl -v -H "Authorization: Basic `echo -n 'test_user:p4ssw0rd' | base64`" \
  -H "Content-Type: application/json" -X POST http://localhost:${PORT}/${INDEX_NAME}/item -d '
{
  "id": "item1",
  "name": "book1",
  "type": "book",
  "description": "descripion of the book 1",
  "properties": {
    "string": [
      {
        "key": "author",
        "value": "Author A"
      }
    ],
    "tags": [
      "nice",
      "good",
      "new"
    ]
  }
}' 

curl -v -H "Authorization: Basic `echo -n 'test_user:p4ssw0rd' | base64`" \
  -H "Content-Type: application/json" -X POST http://localhost:${PORT}/${INDEX_NAME}/item -d '
{
  "id": "item2",
  "name": "book2",
  "type": "book",
  "description": "descripion of the book 2",
  "properties": {
    "string": [
      {
        "key": "author",
        "value": "Author B"
      },
      {
        "key": "author",
        "value": "Author Z"
      }
    ],
    "tags": [
      "nice",
      "fair"
    ]
  }
}'

curl -v -H "Authorization: Basic `echo -n 'test_user:p4ssw0rd' | base64`" \
  -H "Content-Type: application/json" -X POST http://localhost:${PORT}/${INDEX_NAME}/item -d '
{
  "id": "item3",
  "name": "book3",
  "type": "book",
  "description": "descripion of the book 3",
  "properties": {
    "string": [
      {
        "key": "author",
        "value": "Author B"
      }
    ],
    "tags": [
      "nice",
      "good"
    ]
  }
}'

curl -v -H "Authorization: Basic `echo -n 'test_user:p4ssw0rd' | base64`" \
  -H "Content-Type: application/json" -X POST http://localhost:${PORT}/${INDEX_NAME}/item -d '
{
  "id": "item4",
  "name": "book4",
  "type": "book",
  "description": "descripion of the book 4",
  "properties": {
    "string": [
      {
        "key": "author",
        "value": "Author C"
      }
    ],
    "tags": [
      "new",
      "fashion"
    ]
  }
}'

