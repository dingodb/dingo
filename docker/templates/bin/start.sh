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
#EXECUTOR_JAR_PATH=$(find $ROOT -name dingo-*-executor-*.jar)
WEB_JAR_PATH=$(find $ROOT -name dingo-web*.jar)
#NET_JAR_PATH=$(find $ROOT -name dingo-net-*.jar)

ROLE=$DINGO_ROLE
HOSTNAME=$DINGO_HOSTNAME
COORDINATORS=$DINGO_COORDINATORS

if [ X"$HOSTNAME" = "X" ]; then
   echo -e "HOSTNAME has not set, will exist" > ${ROOT}/log/$ROLE.out
   exit -1
fi

sed -i 's/HOSTNAME/'"$HOSTNAME"'/g' ${ROOT}/conf/executor.yaml
sed -i 's/HOSTNAME/'"$HOSTNAME"'/g' ${ROOT}/conf/client.yaml
sed -i 's/HOSTNAME/'"$HOSTNAME"'/g' ${ROOT}/conf/application-dev.yaml

sed -i 's/COORDINATORS/'"$COORDINATORS"'/g' ${ROOT}/conf/executor.yaml
sed -i 's/COORDINATORS/'"$COORDINATORS"'/g' ${ROOT}/conf/client.yaml
sed -i 's/COORDINATORS/'"$COORDINATORS"'/g' ${ROOT}/conf/application-dev.yaml

if [[ $ROLE == "executor" ]]
then
    ./bin/start-executor.sh  &
elif [[ $ROLE == "web" ]]
then
    java ${JAVA_OPTS} \
     -Dlogback.configurationFile=file:${ROOT}/conf/logback-web.xml \
     -jar ${WEB_JAR_PATH} \
     --spring.config.location=${ROOT}/conf/application.yaml \
     > ${ROOT}/log/web.out
else
    echo -e "Invalid DingoDB cluster roles"
fi
