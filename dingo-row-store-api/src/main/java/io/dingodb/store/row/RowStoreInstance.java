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

package io.dingodb.store.row;

import io.dingodb.common.table.TableId;
import io.dingodb.raft.option.CliOptions;
import io.dingodb.raft.util.Endpoint;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.row.client.DefaultDingoRowStore;
import io.dingodb.store.row.errors.DingoRowStoreRuntimeException;
import io.dingodb.store.row.options.DingoRowStoreOptions;
import io.dingodb.store.row.options.PlacementDriverOptions;
import io.dingodb.store.row.options.RegionEngineOptions;
import io.dingodb.store.row.options.StoreDBOptions;
import io.dingodb.store.row.options.StoreEngineOptions;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

@Slf4j
public class RowStoreInstance implements StoreInstance {
    private final Map<byte[], RowPartitionOper> blockMap;

    private static final DefaultDingoRowStore kvStore;

    static {
        kvStore = new DefaultDingoRowStore();
        DingoRowStoreOptions options = getKvStoreOptions();
        if (!kvStore.init(options)) {
            throw new DingoRowStoreRuntimeException("Fail to start [DefaultDingoRowStore].");
        }
    }

    public RowStoreInstance(String path) {
        this.blockMap = new LinkedHashMap<>();
    }

    public static DefaultDingoRowStore kvStore() {
        return kvStore;
    }

    @Override
    public synchronized RowPartitionOper getKvBlock(@Nonnull TableId tableId, Object partId, boolean isMain) {
        // `partId` is not used for `DefaultDingoRowStore`.
        return blockMap.computeIfAbsent(
            tableId.getValue(),
            value -> new RowPartitionOper(kvStore, tableId.getValue())
        );
    }

    @Nonnull
    public static DingoRowStoreOptions getKvStoreOptions() {
        RowStoreConfiguration configuration = RowStoreConfiguration.INSTANCE;

        DingoRowStoreOptions options = new DingoRowStoreOptions();

        options.setClusterId(configuration.clusterId());
        options.setClusterName(configuration.clusterName());
        options.setInitialServerList(configuration.clusterInitialServerList());
        options.setFailoverRetries(configuration.clusterFailoverRetries());
        options.setFutureTimeoutMillis(configuration.futureTimeMillis());

        PlacementDriverOptions driverOptions = new PlacementDriverOptions();
        driverOptions.setFake(configuration.coordinatorOptionsFake());
        driverOptions.setPdGroupId(configuration.coordinatorOptionsRaftGroupId());
        driverOptions.setInitialPdServerList(configuration.coordinatorOptionsInitCoordinatorSrvList());
        CliOptions cliOptions = new CliOptions();
        cliOptions.setMaxRetry(configuration.coordinatorOptionsCliOptionsMaxRetry());
        cliOptions.setTimeoutMs(configuration.coordinatorOptionsCliOptionsTimeoutMs());
        driverOptions.setCliOptions(cliOptions);
        options.setPlacementDriverOptions(driverOptions);

        StoreEngineOptions storeEngineOptions = new StoreEngineOptions();
        StoreDBOptions storeDBOptions = new StoreDBOptions();
        storeDBOptions.setDataPath(configuration.storeEngineOptionsRocksDbOptionsDbPath());
        storeEngineOptions.setStoreDBOptions(storeDBOptions);
        Endpoint endpoint = new Endpoint(configuration.storeEngineOptionsServerAddressId(),
            configuration.storeEngineOptionsServerAddressPort());
        storeEngineOptions.setServerAddress(endpoint);

        List<RegionEngineOptions> regionEngineOptionsList = configuration.storeEngineOptionsRegionEngineOptionsList();
        storeEngineOptions.setRegionEngineOptionsList(regionEngineOptionsList);

        options.setStoreEngineOptions(storeEngineOptions);
        return options;
    }
}
