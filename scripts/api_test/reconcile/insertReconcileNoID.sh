#!/usr/bin/env bash

PORT=${1:-8888}
INDEX_NAME=${2:-index_english_0}
OLD_ID=${3:-0}
NEW_ID=${4:-AAAA_1}
curl -v -H "Authorization: Basic `echo -n 'test_user:p4ssw0rd' | base64`" \
  -H "Content-Type: application/json" -X POST http://localhost:${PORT}/${INDEX_NAME}/reconcile -d "{
		    \"old_id\": \"${OLD_ID}\",
		    \"new_id\": \"${NEW_ID}\",
                    \"item_type\": \"orac_user\",
                    \"retry\": 5
}"

