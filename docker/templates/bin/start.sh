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
COORDINATOR_JAR_PATH=$(find $ROOT -name dingo-*-coordinator-*.jar)
EXECUTOR_JAR_PATH=$(find $ROOT -name dingo-*-executor-*.jar)

ROLE=$DINGO_ROLE
HOSTNAME=$DINGO_HOSTNAME

if [ X"$HOSTNAME" = "X" ]; then
   echo -e "HOSTNAME has not set, will exist" > ${ROOT}/log/$ROLE.out
   exit -1
fi

sed -i 's/XXXXXX/'"$HOSTNAME"'/g' ${ROOT}/conf/coordinator.yaml
sed -i 's/XXXXXX/'"$HOSTNAME"'/g' ${ROOT}/conf/executor.yaml
sed -i 's/XXXXXX/'"$HOSTNAME"'/g' ${ROOT}/conf/client.yaml

if [[ $ROLE == "coordinator" ]]
then
    java ${JAVA_OPTS} \
         -Dlogback.configurationFile=file:${ROOT}/conf/logback-coordinator.xml \
         -classpath ${COORDINATOR_JAR_PATH} \
         io.dingodb.server.coordinator.Starter \
         --config ${ROOT}/conf/coordinator.yaml \
         > ${ROOT}/log/coordinator.out
elif [[ $ROLE == "executor" ]]
then
    sleep 30
    /opt/dingo/bin/wait-for-it.sh coordinator1:19181 -t 0 -s -- echo "Wait Coordniator1 Start Successfully!"
    /opt/dingo/bin/wait-for-it.sh coordinator2:19181 -t 0 -s -- echo "Wait Coordniator2 Start Successfully!"
    /opt/dingo/bin/wait-for-it.sh coordinator3:19181 -t 0 -s -- echo "Wait Coordniator3 Start Successfully!"
    java ${JAVA_OPTS} \
         -Dlogback.configurationFile=file:${ROOT}/conf/logback-executor.xml \
         -classpath ${EXECUTOR_JAR_PATH}  \
         io.dingodb.server.executor.Starter \
         --config ${ROOT}/conf/executor.yaml \
         > ${ROOT}/log/executor.out
elif [[ $ROLE == "driver" ]]
then
    sleep 60
    java ${JAVA_OPTS} \
    -Dlogback.configurationFile=file:${ROOT}/conf/logback-sqlline.xml \
    -classpath ${JAR_PATH} \
    io.dingodb.cli.Tools driver \
    --config ${ROOT}/conf/client.yaml \
    > ${ROOT}/log/driver.out 
else
    echo -e "Invalid DingoDB cluster roles"
fi
