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

java ${JAVA_OPTS} \
    -Dlog4j.configuration=file:${ROOT}/conf/log4j-driver.properties \
    -classpath ${JAR_PATH} \
    io.dingodb.cli.Tools driver \
    --config ${ROOT}/conf/client.yaml \
    $@ > ${ROOT}/log/driver.out

