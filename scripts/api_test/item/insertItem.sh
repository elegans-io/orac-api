#!/usr/bin/env bash

PORT=${1:-8888}
INDEX_NAME=${2:-index_0}
curl -v -H "Authorization: Basic `echo -n 'test_user:p4ssw0rd' | base64`" \
  -H "Content-Type: application/json" -X POST http://localhost:${PORT}/${INDEX_NAME}/item -d '
{
  "id": "d290f1ee-6c54-4b01-90e6-d701748f0851",
  "name": "A Mind at Play: How Claude Shannon Invented the Information Age",
  "type": "book",
  "description": "Claude Shannon was a groundbreaking polymath, a brilliant tinkerer, and a digital pioneer. He constructed the first wearable computer, outfoxed Vegas casinos, and built juggling robots. He also wrote the seminal text of the digital revolution, which has been called “the Magna Carta of the Information Age.” A Mind at Play brings this singular innovator and always playful genius to life.",
  "properties": {
    "numerical": [
      {
        "key": "suggested_price_eur",
        "value": 32
      }
    ],
    "string": [
      {
        "key": "author",
        "value": "Jimmy Soni"
      },
      {
        "key": "url",
        "value": "https://www.amazon.com/Mind-Play-Shannon-Invented-Information/dp/1476766681"
      }
    ],
    "geopoint": [
      {"key": "available_at", 
        "value": {"lat": 40.1, "lon":31.2}},
      {"key": "available_at", 
        "value": {"lat": 41.1, "lon":31.2}}
    ],
    "tags": [
      "mathematics",
      "biography",
      "computer science"
    ],
    "timestamp": [
      {
        "key": "release_date",
        "value": 1509461176000
      }
    ]
  }
}' 

