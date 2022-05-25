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
DRIVER_JAR_PATH=$(find $ROOT -name dingo-cli-*.jar)
WEB_JAR_PATH=$(find $ROOT -name dingo-web*.jar)

ROLE=$DINGO_ROLE
HOSTNAME=$DINGO_HOSTNAME

if [ X"$HOSTNAME" = "X" ]; then
   echo -e "HOSTNAME has not set, will exist" > ${ROOT}/log/$ROLE.out
   exit -1
fi

sed -i 's/XXXXXX/'"$HOSTNAME"'/g' ${ROOT}/conf/coordinator.yaml
sed -i 's/XXXXXX/'"$HOSTNAME"'/g' ${ROOT}/conf/executor.yaml
sed -i 's/XXXXXX/'"$HOSTNAME"'/g' ${ROOT}/conf/client.yaml
sed -i 's/XXXXXX/'"$HOSTNAME"'/g' ${ROOT}/conf/application-dev.yaml

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
    sleep 20
    /opt/dingo/bin/wait-for-it.sh coordinator1:19181 -t 0 -s -- echo "Wait Coordniator1 Start Successfully!"
    /opt/dingo/bin/wait-for-it.sh coordinator2:19181 -t 0 -s -- echo "Wait Coordniator2 Start Successfully!"
    /opt/dingo/bin/wait-for-it.sh coordinator3:19181 -t 0 -s -- echo "Wait Coordniator3 Start Successfully!"
    ./bin/start-executor.sh  &
    P1=$!
    sleep 20
    ./bin/start-driver.sh &
    P2=$!
    wait $P1 $P2
elif [[ $ROLE == "driver" ]]
then
    sleep 60
    ./bin/start-driver.sh &
    P3=$!
    wait $P3
elif [[ $ROLE == "web" ]]
then
    java ${JAVA_OPTS} \
     -Dlogback.configurationFile=file:${ROOT}/conf/logback-web.xml \
     -jar ${WEB_JAR_PATH} \
     --spring.config.location=${ROOT}/conf/application.yaml \
     io.dingodb.web.DingoApplication \
     > ${ROOT}/log/web.out
else
    echo -e "Invalid DingoDB cluster roles"
fi
