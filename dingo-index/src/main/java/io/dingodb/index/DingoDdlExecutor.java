/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.index;

import io.dingodb.index.api.CoordinatorServerApi;
import io.dingodb.index.api.TableCoordinatorServerApi;

import java.util.HashMap;
import java.util.Map;

public class DingoDdlExecutor {
    CoordinatorServerApi coordinatorServerApi;
    Map<String, TableCoordinatorServerApi> tableCoordinatorServerApiMap = new HashMap<>();

    public void init(String coordinatorAddr) {
        //使用配置的coordinator地址初始化coordinatorServerApi
        coordinatorServerApi = null;
    }

    public void executeAddIndex(String tableName, String indexName, String[] columnNames, boolean unique) throws Exception {
        // 1. 获取该Table对应的tableCoordinator服务地址
        String tableCoordinatorAddr = coordinatorServerApi.getTableCoordinatorAddr(tableName);
        // 2. 使用tableCoordinatorAddr初始化tableCoordinatorServerApi，优先使用缓存
        if (!tableCoordinatorServerApiMap.containsKey(tableName)) {
            TableCoordinatorServerApi tableCoordinatorServerApi = null;
            tableCoordinatorServerApiMap.put(tableName, tableCoordinatorServerApi);
        }
        TableCoordinatorServerApi tableCoordinatorServerApi = tableCoordinatorServerApiMap.get(tableName);
        // 3. 调用tableCoordinatorServerApi的addIndex方法
        tableCoordinatorServerApi.addIndex(tableName, indexName, columnNames, unique);
    }
}
