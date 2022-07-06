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

package io.dingodb.sdk.client;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.partition.PartitionStrategy;
import io.dingodb.common.partition.RangeStrategy;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.KeyValueCodec;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.Part;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.sdk.common.Column;
import io.dingodb.sdk.common.Key;
import io.dingodb.server.api.ExecutorApi;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

@Slf4j
public class DingoConnection extends ClientBase {
    private MetaClient metaClient;

    private ApiRegistry apiRegistry;

    public DingoConnection(String configPath) {
        super(configPath);
        this.metaClient = new MetaClient(configPath);
    }

    public DingoConnection(String coordinatorExchangeSvrList,
                           String currentHost,
                           Integer currentPort) {
        super(coordinatorExchangeSvrList, currentHost, currentPort);
        this.metaClient = new MetaClient(coordinatorExchangeSvrList, currentHost, currentPort);
    }

    @Override
    public void initConnection() throws Exception {
        super.initConnection();
        this.apiRegistry = super.getNetService().apiRegistry();
    }


    public MetaClient getMetaClient() {
        return metaClient;
    }

    public ApiRegistry getApiRegistry() {
        return apiRegistry;
    }
}
