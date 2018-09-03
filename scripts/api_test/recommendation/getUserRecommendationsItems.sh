#!/usr/bin/env bash

SCRIPT_PATH=$(dirname $(readlink -e ${0}))
USERNAME=${1:-test_user}
PORT=${2:-8443}
INDEX_NAME=${3:-index_english_0}

${SCRIPT_PATH}/getUserRecommendations.sh ${PORT} ${INDEX_NAME} ${USERNAME} | json_pp | grep item_id | sed -e 's/.*: //g' -e 's/"//g' -e 's/\,//g' | while read line
do
        ${SCRIPT_PATH}/../item/getItems.sh 8443 ${INDEX_NAME} $line 2> /dev/null | json_pp
done

