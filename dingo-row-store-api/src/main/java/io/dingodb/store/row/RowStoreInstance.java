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

import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.util.Endpoint;
import io.dingodb.dingokv.client.DefaultDingoKVStore;
import io.dingodb.dingokv.errors.DingoKVRuntimeException;
import io.dingodb.dingokv.options.DingoKVStoreOptions;
import io.dingodb.dingokv.options.PlacementDriverOptions;
import io.dingodb.dingokv.options.RegionEngineOptions;
import io.dingodb.dingokv.options.RocksDBOptions;
import io.dingodb.dingokv.options.StoreEngineOptions;
import io.dingodb.store.api.StoreInstance;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

@Slf4j
public class RowStoreInstance implements StoreInstance {
    private final String path;

    private final Map<String, RowPartitionOp> blockMap;

    private static DefaultDingoKVStore kvStore;

    static {
        kvStore = new DefaultDingoKVStore();
        DingoKVStoreOptions options = getKvStoreOptions();
        if (!kvStore.init(options)) {
            throw new DingoKVRuntimeException("Fail to start [DefaultDingoKVStore].");
        }
    }

    public RowStoreInstance(String path) {
        this.path = path;
        this.blockMap = new LinkedHashMap<>();
    }

    @Nonnull
    public String blockDir(@Nonnull String tableName, @Nonnull Object partId) {
        return path + File.separator
            + tableName.replace(".", File.separator).toLowerCase() + File.separator
            + partId;
    }

    @Override
    public synchronized RowPartitionOp getKvBlock(String tableName, Object partId, boolean isMain) {
        String blockDir = blockDir(tableName, partId);
        return blockMap.computeIfAbsent(blockDir, value -> new RowPartitionOp(path, kvStore));
    }

    public static DingoKVStoreOptions getKvStoreOptions() {
        RowStoreConfiguration configuration = RowStoreConfiguration.INSTANCE;

        DingoKVStoreOptions options = new DingoKVStoreOptions();

        options.setClusterId(configuration.clusterId());
        options.setClusterName(configuration.clusterName());
        options.setInitialServerList(configuration.clusterInitialServerList());
        options.setFailoverRetries(configuration.clusterFailoverRetries());
        options.setFutureTimeoutMillis(configuration.clusterFutureTimeoutMillis());

        PlacementDriverOptions driverOptions = new PlacementDriverOptions();
        driverOptions.setFake(configuration.placementDriverOptionsFake());
        driverOptions.setPdGroupId(configuration.placementDriverOptionsPDGroupId());
        driverOptions.setInitialPdServerList(configuration.placementDriverOptionsInitialPDServerList());
        CliOptions cliOptions = new CliOptions();
        cliOptions.setMaxRetry(configuration.placementDriverOptionsCliOptionsMaxRetry());
        cliOptions.setTimeoutMs(configuration.placementDriverOptionsCliOptionsTimeoutMs());
        driverOptions.setCliOptions(cliOptions);
        options.setPlacementDriverOptions(driverOptions);

        StoreEngineOptions storeEngineOptions = new StoreEngineOptions();
        RocksDBOptions rocksDBOptions = new RocksDBOptions();
        rocksDBOptions.setDbPath(configuration.storeEngineOptionsRocksDbOptionsDbPath());
        storeEngineOptions.setRocksDBOptions(rocksDBOptions);
        storeEngineOptions.setRaftDataPath(configuration.storeEngineOptionsRaftDataPath());
        Endpoint endpoint = new Endpoint(configuration.storeEngineOptionsServerAddressId(),
            configuration.storeEngineOptionsServerAddressPort());
        storeEngineOptions.setServerAddress(endpoint);

        List<RegionEngineOptions> regionEngineOptionsList = configuration.storeEngineOptionsRegionEngineOptionsList();
        storeEngineOptions.setRegionEngineOptionsList(regionEngineOptionsList);

        options.setStoreEngineOptions(storeEngineOptions);
        return options;
    }
}
