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
JAR_PATH=$(find $ROOT -name dingo-executor-*.jar)
LOCAL_STORE_JAR_PATH=$(find $ROOT -name dingo-store-local*.jar)
NET_JAR_PATH=$(find $ROOT -name dingo-net-*.jar)
APP_HOME=$( cd "$( dirname "$0" )/.." && pwd )
PLATFORM=$(uname -s)-$(uname -m | sed 's/x86_64/x64/')
LOG_DIR="$ROOT/log"

if [ ! -d "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
fi

JAVA_OPTS="\
    -Xms8g -Xmx8g \
    -XX:+UseG1GC \
    -XX:MaxGCPauseMillis=100 \
    -XX:G1HeapRegionSize=4M \
    -XX:+ParallelRefProcEnabled \
    -XX:+AlwaysPreTouch \
    -XX:+DisableExplicitGC \
    -XX:+HeapDumpOnOutOfMemoryError \
    -XX:MaxDirectMemorySize=4096m \
    -XX:ReservedCodeCacheSize=256m \
    -XX:+UseCodeCacheFlushing \
    -XX:+TieredCompilation \
    -XX:TieredStopAtLevel=4 \
    -XX:InitialCodeCacheSize=256m \
    -Xlog:gc*:file=${LOG_DIR}/gc.log:time:filecount=5,filesize=100M \
"

EMBEDDED_JDK="${APP_HOME}/${PLATFORM}"
if [ -d "${EMBEDDED_JDK}" ]; then
    export JAVA_HOME="${EMBEDDED_JDK}"
    PATH="${JAVA_HOME}/bin:${PATH}"
fi

nohup ${JAVA_HOME}/bin/java ${JAVA_OPTS} \
     --add-opens java.base/java.util=ALL-UNNAMED \
     --add-opens java.base/java.lang=ALL-UNNAMED \
     -Dlogback.configurationFile=file:${ROOT}/conf/logback-executor.xml \
     -classpath ${JAR_PATH}:${NET_JAR_PATH}:${LOCAL_STORE_JAR_PATH}  \
     io.dingodb.server.executor.Starter \
     --config ${ROOT}/conf/executor.yaml \
     > ${ROOT}/log/executor.out
