#!/usr/bin/env bash

PORT=${1:-8888}
USER_NAME=${2:-"test_user"}
PASSWORD=${3:-"p4ssw0rd"}
curl -v -H "Authorization: Basic `echo -n 'admin:adminp4ssw0rd' | base64`" \
  -H "Content-Type: application/json" -X POST http://localhost:${PORT}/user_gen/${USER_NAME} -d "{
        \"password\": \"${PASSWORD}\",
	\"permissions\": {
		\"index_0\": [\"create_action\", \"read_recomm\"]
	}
}"

