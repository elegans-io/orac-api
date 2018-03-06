#!/usr/bin/env bash

curl -H "Content-Type: application/json" -X POST 'http://localhost:9200/index_english_0.recommendation_history/_search?pretty=true' -d'{
    "query": {
        "match_all": {}
    }
}'

