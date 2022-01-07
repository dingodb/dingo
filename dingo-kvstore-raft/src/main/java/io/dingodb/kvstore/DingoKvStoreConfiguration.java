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

package io.dingodb.kvstore;

import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.dingokv.options.RegionEngineOptions;
import lombok.experimental.Delegate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DingoKvStoreConfiguration {

    public static final DingoKvStoreConfiguration INSTANCE = new DingoKvStoreConfiguration();

    private static final String CLUSTER_ID_KEY = "kv.cluster.id";
    private static final String CLUSTER_NAME_KEY = "kv.cluster.name";
    private static final String CLUSTER_INITIAL_SERVER_LIST_KEY = "kv.initialServerList";
    private static final String CLUSTER_FAILOVER_RETRIES_KEY = "kv.failoverRetries";
    private static final String CLUSTER_FUTURE_TIMEOUT_MILLIS_KEY = "kv.futureTimeoutMillis";

    // PlacementDriverOptions
    private static final String PLACEMENT_DRIVER_OPTIONS_FAKE_KEY = "placement.driver.options.fake";
    private static final String PLACEMENT_DRIVER_OPTIONS_PD_GROUP_ID_KEY = "placement.driver.options.pdGroupId";
    private static final String PLACEMENT_DRIVER_OPTIONS_INITIAL_PD_SERVER_LIST_KEY =
        "placement.driver.options.initialPdServerList";

    // PlacementDriverOptions.cliOptions
    private static final String PLACEMENT_DRIVER_OPTIONS_CLI_OPTIONS_TIMEOUT_MS_KEY =
        "placement.driver.options.cliOptions.timeoutMs";
    private static final String PLACEMENT_DRIVER_OPTIONS_CLI_OPTIONS_MAX_RETRY_KEY =
        "placement.driver.options.cliOptions.maxRetry";

    // StoreEngineOptions
    //private static final String STORE_ENGINE_OPTIONS_STORAGE_TYPE_KEY = "store.engine.options.storageType";
    private static final String STORE_ENGINE_OPTIONS_RAFT_DATA_PATH_KEY = "store.engine.options.raftDataPath";

    // StoreEngineOptions.RocksDBOptions
    private static final String STORE_ENGINE_OPTIONS_ROCKS_DB_OPTIONS_DB_PATH_KEY =
        "store.engine.options.rocksDBOptions.dbPath";

    // StoreEngineOptions.Endpoint
    private static final String STORE_ENGINE_OPTIONS_SERVER_ADDRESS_ID_KEY = "store.engine.options.serverAddress.ip";
    private static final String STORE_ENGINE_OPTIONS_SERVER_ADDRESS_PORT_KEY =
        "store.engine.options.serverAddress.port";

    private static final String STORE_ENGINE_OPTIONS_REGION_ENGINE_OPTIONS_LIST =
        "store.engine.options.regionEngineOptionsList";

    @Delegate
    private final DingoConfiguration dingoConfiguration = DingoConfiguration.INSTANCE;

    private DingoKvStoreConfiguration() {
    }

    public static DingoKvStoreConfiguration instance() {
        return INSTANCE;
    }

    public DingoKvStoreConfiguration clusterId(int clusterId) {
        setInt(CLUSTER_ID_KEY, clusterId);
        return this;
    }

    public int clusterId() {
        return getInt(CLUSTER_ID_KEY);
    }

    public DingoKvStoreConfiguration clusterName(String clusterName) {
        setString(CLUSTER_NAME_KEY, clusterName);
        return this;
    }

    public String clusterName() {
        return getString(CLUSTER_NAME_KEY);
    }

    public DingoKvStoreConfiguration clusterInitialServerList(String clusterInitialServerList) {
        setString(CLUSTER_INITIAL_SERVER_LIST_KEY, clusterInitialServerList);
        return this;
    }

    public String clusterInitialServerList() {
        return getString(CLUSTER_INITIAL_SERVER_LIST_KEY);
    }

    public DingoKvStoreConfiguration clusterFailoverRetries(int clusterFailoverRetries) {
        setInt(CLUSTER_FAILOVER_RETRIES_KEY, clusterFailoverRetries);
        return this;
    }

    public int clusterFailoverRetries() {
        return getInt(CLUSTER_FAILOVER_RETRIES_KEY);
    }

    public DingoKvStoreConfiguration clusterFutureTimeoutMillis(int clusterFutureTimeoutMillis) {
        setInt(CLUSTER_FUTURE_TIMEOUT_MILLIS_KEY, clusterFutureTimeoutMillis);
        return this;
    }

    public int clusterFutureTimeoutMillis() {
        return getInt(CLUSTER_FUTURE_TIMEOUT_MILLIS_KEY);
    }

    public DingoKvStoreConfiguration placementDriverOptionsFake(boolean placementDriverOptionsFake) {
        setBool(PLACEMENT_DRIVER_OPTIONS_FAKE_KEY, placementDriverOptionsFake);
        return this;
    }

    public boolean placementDriverOptionsFake() {
        return getBool(PLACEMENT_DRIVER_OPTIONS_FAKE_KEY);
    }

    public DingoKvStoreConfiguration placementDriverOptionsPDGroupId(String placementDriverOptionsPDGroupId) {
        setString(PLACEMENT_DRIVER_OPTIONS_PD_GROUP_ID_KEY, placementDriverOptionsPDGroupId);
        return this;
    }

    public String placementDriverOptionsPDGroupId() {
        return getString(PLACEMENT_DRIVER_OPTIONS_PD_GROUP_ID_KEY);
    }

    public DingoKvStoreConfiguration placementDriverOptionsInitialPDServerList(
        String placementDriverOptionsInitialPDServerList) {
        setString(PLACEMENT_DRIVER_OPTIONS_INITIAL_PD_SERVER_LIST_KEY, placementDriverOptionsInitialPDServerList);
        return this;
    }

    public String placementDriverOptionsInitialPDServerList() {
        return getString(PLACEMENT_DRIVER_OPTIONS_INITIAL_PD_SERVER_LIST_KEY);
    }

    public DingoKvStoreConfiguration placementDriverOptionsCliOptionsTimeoutMs(
        int placementDriverOptionsCliOptionsTimeoutMs) {
        setInt(PLACEMENT_DRIVER_OPTIONS_CLI_OPTIONS_TIMEOUT_MS_KEY, placementDriverOptionsCliOptionsTimeoutMs);
        return this;
    }

    public int placementDriverOptionsCliOptionsTimeoutMs() {
        return getInt(PLACEMENT_DRIVER_OPTIONS_CLI_OPTIONS_TIMEOUT_MS_KEY);
    }

    public DingoKvStoreConfiguration placementDriverOptionsCliOptionsMaxRetry(
        int placementDriverOptionsCliOptionsMaxRetry) {
        setInt(PLACEMENT_DRIVER_OPTIONS_CLI_OPTIONS_MAX_RETRY_KEY, placementDriverOptionsCliOptionsMaxRetry);
        return this;
    }

    public int placementDriverOptionsCliOptionsMaxRetry() {
        return getInt(PLACEMENT_DRIVER_OPTIONS_CLI_OPTIONS_MAX_RETRY_KEY);
    }

    public DingoKvStoreConfiguration storeEngineOptionsRaftDataPath(String storeEngineOptionsRaftDataPath) {
        setString(STORE_ENGINE_OPTIONS_RAFT_DATA_PATH_KEY, storeEngineOptionsRaftDataPath);
        return this;
    }

    public String storeEngineOptionsRaftDataPath() {
        return getString(STORE_ENGINE_OPTIONS_RAFT_DATA_PATH_KEY);
    }

    public DingoKvStoreConfiguration storeEngineOptionsRocksDbOptionsDbPath(
        String storeEngineOptionsRocksDbOptionsDbPath) {
        setString(STORE_ENGINE_OPTIONS_ROCKS_DB_OPTIONS_DB_PATH_KEY, storeEngineOptionsRocksDbOptionsDbPath);
        return this;
    }

    public String storeEngineOptionsRocksDbOptionsDbPath() {
        return getString(STORE_ENGINE_OPTIONS_ROCKS_DB_OPTIONS_DB_PATH_KEY);
    }

    public DingoKvStoreConfiguration storeEngineOptionsServerAddressId(String storeEngineOptionsServerAddressId) {
        setString(STORE_ENGINE_OPTIONS_SERVER_ADDRESS_ID_KEY, storeEngineOptionsServerAddressId);
        return this;
    }

    public String storeEngineOptionsServerAddressId() {
        return getString(STORE_ENGINE_OPTIONS_SERVER_ADDRESS_ID_KEY);
    }

    public DingoKvStoreConfiguration storeEngineOptionsServerAddressPort(int storeEngineOptionsServerAddressPort) {
        setInt(STORE_ENGINE_OPTIONS_SERVER_ADDRESS_PORT_KEY, storeEngineOptionsServerAddressPort);
        return this;
    }

    public int storeEngineOptionsServerAddressPort() {
        return getInt(STORE_ENGINE_OPTIONS_SERVER_ADDRESS_PORT_KEY);
    }

    public List<RegionEngineOptions> storeEngineOptionsRegionEngineOptionsList() {
        List<RegionEngineOptions> optionsList = new ArrayList<>();
        List<Map<String, Object>> list = getList(STORE_ENGINE_OPTIONS_REGION_ENGINE_OPTIONS_LIST);
        for (Map<String, Object> map : list) {
            map.put("regionId", Long.valueOf((int)map.get("regionId")));
            try {
                RegionEngineOptions regionEngineOptions = mapToBean(map, RegionEngineOptions.class);
                optionsList.add(regionEngineOptions);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return optionsList;
    }
}
