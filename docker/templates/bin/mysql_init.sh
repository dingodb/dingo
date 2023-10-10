#!/bin/bash

#
# Copyright 2021 DataCanvas
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
JAR_PATH=$(find $ROOT -name dingo-mysql-init-*.jar)

DINGODB_STORE_NORMAL=0
DINGODB_MYSQL_INIT=0
DINGODB_SCHEMA_MYSQL=0

while [ "${DINGODB_STORE_NORMAL}" -lt 1 ]; do
    echo "DINGODB_STORE_NORMAL = ${DINGODB_STORE_NORMAL}, wait 1 second"
    sleep 1
    echo "DINGO_MYSQL_COORDINATORS: ${DINGO_MYSQL_COORDINATORS}"
    ERRCODE=$(curl -s  http://${DINGO_MYSQL_COORDINATORS}/CoordinatorService/GetStoreMap -d '{"epoch": 0}' 2>&1  | grep -o '"ERAFT_NOTLEADER"' | wc -l)
    echo "ERRCODE ${ERRCODE}"
    if [ "${ERRCODE}" -ne 0 ]; then
         ERR_INFO=$(curl -s  http://${DINGO_MYSQL_COORDINATORS}/CoordinatorService/GetStoreMap -d '{"epoch": 0}')
         LEADER_HOST=$(echo "${ERR_INFO}" | grep -oP '(?<="host":")[^"]+')
         LEADER_PORT=$(echo "${ERR_INFO}" | grep -oP '(?<="port":)[^,}]+')
         echo "LEADER_HOST: ${LEADER_HOST} LEADER_PORT: ${LEADER_PORT}"
         DINGODB_STORE_NORMAL=$(curl http://${LEADER_HOST}:${LEADER_PORT}/CoordinatorService/GetStoreMap -d '{"epoch": 0}'  2>&1  | grep -o 'STORE_NORMAL' | wc -l)
         DINGODB_SCHEMA_MYSQL=$(curl http://${LEADER_HOST}:${LEADER_PORT}/MetaService/GetSchemaByName -d '{"schema_name": "mysql"}' 2>&1 | grep -o 'table_ids' | wc -l)
    else
        DINGODB_STORE_NORMAL=$(curl http://${DINGO_MYSQL_COORDINATORS}/CoordinatorService/GetStoreMap -d '{"epoch": 0}'  2>&1 | grep -o 'STORE_NORMAL' | wc -l)
        DINGODB_SCHEMA_MYSQL=$(curl http://${DINGO_MYSQL_COORDINATORS}/MetaService/GetSchemaByName -d '{"schema_name": "mysql"}' 2>&1 | grep -o 'table_ids' | wc -l)
        echo "DINGODB_STORE_NORMAL: ${DINGODB_STORE_NORMAL} DINGODB_SCHEMA_MYSQL ${DINGODB_SCHEMA_MYSQL}"
    fi

done

echo "DINGODB_STORE_NORMAL >= 3, start to initialize MySQL"
if [ "${DINGODB_SCHEMA_MYSQL}" -ne 0 ]; then
    echo "mysql init has been completed"
else
    # run Java start mysql_init 
    java -cp ${JAR_PATH} io.dingodb.mysql.MysqlInit ${DINGO_MYSQL_COORDINATORS}

    # check status
    if [ $? -eq 0 ]
    then
      echo "Java mysql init succes"
    else
      echo "Java mysql init fail"
    fi
fi




