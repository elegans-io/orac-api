#!/usr/bin/env bash

PORT=${1:-8888}

curl -v -H "Authorization: Basic `echo -n 'admin:adminp4ssw0rd' | base64`" \
	     -H "Content-Type: application/json" -X POST http://localhost:${PORT}/user -d '{
	     "id": "test_user",
	     "password": "3c98bf19cb962ac4cd0227142b3495ab1be46534061919f792254b80c0f3e566f7819cae73bdc616af0ff555f7460ac96d88d56338d659ebd93e2be858ce1cf9", 
	     "salt": "salt",
	     "permissions": {
		     "index_0": [ "create_action", "update_action", "read_action", "delete_action",
		     "create_item", "update_item", "read_item", "delete_item",
		     "create_orac_user", "update_orac_user", "read_orac_user", "delete_orac_user",
		     "create_recomm", "update_recomm", "read_recomm", "delete_recomm"
		     ]
	     }
}'

