#!/usr/bin/env bash

PORT=${1:-8888}
INDEX_NAME=${2:-index_0}
OLD_ID=${3:-"d290f1ee-6c54-4b01-90e6-d701748f0851"}
NEW_ID=${4:-"new_user_id_0001"}
curl -v -H "Authorization: Basic `echo -n 'test_user:p4ssw0rd' | base64`" \
  -H "Content-Type: application/json" -X POST http://localhost:${PORT}/${INDEX_NAME}/reconcile -d "{
		    \"id\": \"reconciliation_operation_id_0001\",
                    \"item_type\": \"orac_user\",
                    \"old_id\": \"${OLD_ID}\",
                    \"new_id\": \"${NEW_ID}\",
                    \"retry\": 5
}"

