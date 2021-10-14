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
JAR_PATH=$(find $ROOT -name dingo-cli-*.jar)
STORE_JAR_PATH=$(find $ROOT -name dingo-store*.jar)

ROLE=$DINGO_ROLE

if [[ $ROLE == "coordinator" ]]
then
    java ${JAVA_OPTS} \
        -Dlog4j.configuration=file:${ROOT}/conf/log4j.properties \
        -classpath ${JAR_PATH} \
        io.dingodb.cli.SetUp \
        --config ${ROOT}/conf/config.yaml \
        cluster

    java ${JAVA_OPTS} \
        -Dlog4j.configuration=file:${ROOT}/conf/log4j-coordinator.properties \
        -classpath ${JAR_PATH} \
        io.dingodb.cli.Starter coordinator \
        --config ${ROOT}/conf/config.yaml \
        > ${ROOT}/log/coordinator.out

elif [[ $ROLE == "executor" ]]
then
    /opt/dingo/bin/wait-for-it.sh coordinator:19181 -t 0 -s -- echo "Wait Coordniator Start Successfully!"
    java ${JAVA_OPTS} \
        -Dlog4j.configuration=file:${ROOT}/conf/log4j-executor.properties \
        -classpath ${JAR_PATH}:${STORE_JAR_PATH}  \
        io.dingodb.cli.Starter executor \
        --config ${ROOT}/conf/config.yaml \
        > ${ROOT}/log/executor.out
else
    echo "Invalid Dingo_Role: $ROLE. Will Exist!" > ${ROOT}/log/invalid.out
fi
