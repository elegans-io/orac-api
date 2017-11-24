#!/usr/bin/env bash

PORT=${1:-8888}

curl -v -H "Authorization: Basic `echo -n 'admin:adminp4ssw0rd' | base64`" \
 -H "Content-Type: application/json" -X PUT http://localhost:${PORT}/user/test_user -d '{
	"permissions": {
		"index_0": ["read"]
	}
}'

