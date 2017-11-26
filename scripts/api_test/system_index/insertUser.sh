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
		     "read_recomm"
		     ]
	     }
}'

curl -v -H "Authorization: Basic `echo -n 'admin:adminp4ssw0rd' | base64`" \
	     -H "Content-Type: application/json" -X POST http://localhost:${PORT}/user -d '{
	     "password" : "374f6caba1c8e8980a63752ac01a079cfb6a29bbdcce67bb2a78a66dbd5fe02b67422b5badbeddac34750be610f1852770a8457293f94657a5cd1c6d118b25ef",
	     "id" : "test_user2",
	     "permissions" : {
		     "index_0" : [
			     "create_action", "read_recomm" ]
	     },
	     "salt" : "A766j93b2Ngc4Thr"

	     }'

