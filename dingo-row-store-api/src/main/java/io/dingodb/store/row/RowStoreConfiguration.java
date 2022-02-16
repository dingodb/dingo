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

import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.store.row.options.RegionEngineOptions;
import lombok.experimental.Delegate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Deprecated
public class RowStoreConfiguration {

    public static final RowStoreConfiguration INSTANCE = new RowStoreConfiguration();

    private static final String STORE_CLUSTER_ID_KEY = "store.cluster.id";
    private static final String STORE_CLUSTER_INITIAL_SERVER_LIST_KEY = "store.initialServerList";
    private static final String STORE_CLUSTER_FAILOVER_RETRIES_KEY = "store.failoverRetries";

    // PlacementDriverOptions
    private static final String COORDINATOR_OPTIONS_FAKE_KEY = "coordinator.options.fake";
    private static final String COORDINATOR_OPTIONS_RAFT_GROUP_ID_KEY = "coordinator.options.raftGroupId";
    private static final String COORDINATOR_OPTIONS_INITIAL_COORDINATOR_SERVER_LIST_KEY =
        "coordinator.options.initCoordinatorSrvList";

    // CoordinatorOptions.cliOptions
    private static final String COORDINATOR_OPTIONS_CLI_OPTIONS_TIMEOUT_MS_KEY =
        "coordinator.options.cliOptions.timeoutMs";
    private static final String COORDINATOR_OPTIONS_CLI_OPTIONS_MAX_RETRY_KEY =
        "coordinator.options.cliOptions.maxRetry";

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

    public static final String DEFAULT_COORDINATOR_RAFT_ID = "COORDINATOR_RAFT";

    @Delegate
    private final DingoConfiguration dingoConfiguration = DingoConfiguration.instance();

    private RowStoreConfiguration() {
    }

    public static RowStoreConfiguration instance() {
        return INSTANCE;
    }

    public RowStoreConfiguration clusterId(int clusterId) {
        setInt(STORE_CLUSTER_ID_KEY, clusterId);
        return this;
    }

    public int clusterId() {
        return getInt(STORE_CLUSTER_ID_KEY, 0);
    }

    public RowStoreConfiguration clusterInitialServerList(String clusterInitialServerList) {
        setString(STORE_CLUSTER_INITIAL_SERVER_LIST_KEY, clusterInitialServerList);
        return this;
    }

    public String clusterInitialServerList() {
        return getString(STORE_CLUSTER_INITIAL_SERVER_LIST_KEY);
    }

    public RowStoreConfiguration clusterFailoverRetries(int clusterFailoverRetries) {
        setInt(STORE_CLUSTER_FAILOVER_RETRIES_KEY, clusterFailoverRetries);
        return this;
    }

    public int clusterFailoverRetries() {
        return getInt(STORE_CLUSTER_FAILOVER_RETRIES_KEY);
    }

    public RowStoreConfiguration coordinatorOptionsFake(boolean coordinatorOptionsFake) {
        setBool(COORDINATOR_OPTIONS_FAKE_KEY, coordinatorOptionsFake);
        return this;
    }

    public boolean coordinatorOptionsFake() {
        return getBool(COORDINATOR_OPTIONS_FAKE_KEY);
    }

    public RowStoreConfiguration coordinatorOptionsRaftGroupId(String coordinatorOptionsRaftGroupId) {
        setString(COORDINATOR_OPTIONS_RAFT_GROUP_ID_KEY, coordinatorOptionsRaftGroupId);
        return this;
    }

    public String coordinatorOptionsRaftGroupId() {
        return getString(COORDINATOR_OPTIONS_RAFT_GROUP_ID_KEY, DEFAULT_COORDINATOR_RAFT_ID);
    }

    public RowStoreConfiguration coordinatorOptionsInitCoordinatorSrvList(
        String coordinatorOptionsInitCoordinatorSrvList) {
        setString(COORDINATOR_OPTIONS_INITIAL_COORDINATOR_SERVER_LIST_KEY, coordinatorOptionsInitCoordinatorSrvList);
        return this;
    }

    public String coordinatorOptionsInitCoordinatorSrvList() {
        return getString(COORDINATOR_OPTIONS_INITIAL_COORDINATOR_SERVER_LIST_KEY);
    }

    public RowStoreConfiguration coordinatorOptionsCliOptionsTimeoutMs(
        int coordinatorOptionsCliOptionsTimeoutMs) {
        setInt(COORDINATOR_OPTIONS_CLI_OPTIONS_TIMEOUT_MS_KEY, coordinatorOptionsCliOptionsTimeoutMs);
        return this;
    }

    public int coordinatorOptionsCliOptionsTimeoutMs() {
        return getInt(COORDINATOR_OPTIONS_CLI_OPTIONS_TIMEOUT_MS_KEY);
    }

    public RowStoreConfiguration coordinatorOptionsCliOptionsMaxRetry(
        int coordinatorOptionsCliOptionsMaxRetry) {
        setInt(COORDINATOR_OPTIONS_CLI_OPTIONS_MAX_RETRY_KEY, coordinatorOptionsCliOptionsMaxRetry);
        return this;
    }

    public int coordinatorOptionsCliOptionsMaxRetry() {
        return getInt(COORDINATOR_OPTIONS_CLI_OPTIONS_MAX_RETRY_KEY);
    }

    public RowStoreConfiguration storeEngineOptionsRaftDataPath(String storeEngineOptionsRaftDataPath) {
        setString(STORE_ENGINE_OPTIONS_RAFT_DATA_PATH_KEY, storeEngineOptionsRaftDataPath);
        return this;
    }

    public String storeEngineOptionsRaftDataPath() {
        return getString(STORE_ENGINE_OPTIONS_RAFT_DATA_PATH_KEY);
    }

    public RowStoreConfiguration storeEngineOptionsRocksDbOptionsDbPath(
        String storeEngineOptionsRocksDbOptionsDbPath) {
        setString(STORE_ENGINE_OPTIONS_ROCKS_DB_OPTIONS_DB_PATH_KEY, storeEngineOptionsRocksDbOptionsDbPath);
        return this;
    }

    public String storeEngineOptionsRocksDbOptionsDbPath() {
        return getString(STORE_ENGINE_OPTIONS_ROCKS_DB_OPTIONS_DB_PATH_KEY);
    }

    public RowStoreConfiguration storeEngineOptionsServerAddressId(String storeEngineOptionsServerAddressId) {
        setString(STORE_ENGINE_OPTIONS_SERVER_ADDRESS_ID_KEY, storeEngineOptionsServerAddressId);
        return this;
    }

    public String storeEngineOptionsServerAddressId() {
        return getString(STORE_ENGINE_OPTIONS_SERVER_ADDRESS_ID_KEY);
    }

    public RowStoreConfiguration storeEngineOptionsServerAddressPort(int storeEngineOptionsServerAddressPort) {
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
