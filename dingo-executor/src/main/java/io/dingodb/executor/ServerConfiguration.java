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

package io.dingodb.executor;

import com.alipay.sofa.jraft.entity.PeerId;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.store.row.options.MemoryDBOptions;
import lombok.experimental.Delegate;

import java.util.List;
import java.util.stream.Collectors;

public class ServerConfiguration {

    public static final ServerConfiguration INSTANCE = new ServerConfiguration();
    private static final String CLUSTER_NAME_KEY = "cluster.name";

    private static final String COORDINATOR_NAME_KEY = "cluster.coordinator.name";
    private static final String COORDINATOR_TAG_KEY = "cluster.coordinator.tag";

    private static final String COORDINATOR_REPLICAS_KEY = "cluster.coordinator.replicas";
    private static final String COORDINATOR_PARTITIONS_KEY = "cluster.coordinator.partitions";

    private static final String COORDINATOR_SERVERS_KEY = "cluster.coordinator.raft.servers";
    private static final String COORDINATOR_RAFT_MEMORY_KEY = "cluster.coordinator.raft.memory";
    private static final String COORDINATOR_RAFT_GROUP_ID = "cluster.coordinator.raft.group.appId";
    public static final String COORDINATOR_RAFT_DATA_DIR_KEY = "cluster.coordinator.raft.data.dir";

    private static final String INSTANCE_HOST_KEY = "instance.host";
    private static final String ZK_SERVERS_KEY = "zookeeper.servers";

    private static final String CONTROLLER_NAME_KEY = "controller.name";
    private static final String CONTROLLER_MODE_KEY = "controller.mode";

    private static final String COORDINATOR_PORT_KEY = "server.coordinator.port";
    private static final String EXECUTOR_PORT_KEY = "server.executorView.port";
    private static final String DATA_DIR_KEY = "data.dir";


    private static final String PORT_KEY = "active.port";
    private static final String STATE_MODE_KEY = "active.state.mode";
    private static final String STORE_KEY = "active.executorView";

    private static final String STORE_DEFAULT_REPLICAS_KEY = "store.default.replicas";
    private static final String STORE_DEFAULT_PARTITIONS_KEY = "store.default.partitions";
    private static final String STORE_DEFAULT_TABLE_TAG_KEY = "store.default.tag";

    private static final String TABLE_TAG = "TableTag";
    private static final String ROCKSDB = "io.dingodb.executorView.rocks.RocksStoreServiceProvider";

    private static final String DEFAULT_COORDINATOR_RAFT_ID = "COORDINATOR_RAFT";

    @Delegate private final DingoConfiguration dingoConfiguration = DingoConfiguration.INSTANCE;

    private ServerConfiguration() {
    }

    public static ServerConfiguration instance() {
        return INSTANCE;
    }

    public ServerConfiguration store(String store) {
        setString(STORE_KEY, store);
        return this;
    }

    public String store() {
        return getString(STORE_KEY, ROCKSDB);
    }

    public String tableTag() {
        return getString(STORE_DEFAULT_TABLE_TAG_KEY, TABLE_TAG);
    }

    public ServerConfiguration tableTag(String tag) {
        setString(STORE_DEFAULT_TABLE_TAG_KEY, tag);
        return this;
    }

    public ServerConfiguration clusterName(String clusterName) {
        setString(CLUSTER_NAME_KEY, clusterName);
        return this;
    }

    public String clusterName() {
        return getString(CLUSTER_NAME_KEY);
    }

    public ServerConfiguration coordinatorName(String coordinatorName) {
        setString(COORDINATOR_NAME_KEY, coordinatorName);
        return this;
    }

    public String coordinatorName() {
        return getString(COORDINATOR_NAME_KEY);
    }

    public ServerConfiguration coordinatorTag(String coordinatorTag) {
        setString(COORDINATOR_TAG_KEY, coordinatorTag);
        return this;
    }

    public String coordinatorTag() {
        return getString(COORDINATOR_TAG_KEY);
    }

    public ServerConfiguration coordinatorReplicas(int coordinatorReplicas) {
        setInt(COORDINATOR_REPLICAS_KEY, coordinatorReplicas);
        return this;
    }

    public int coordinatorReplicas() {
        return getInt(COORDINATOR_REPLICAS_KEY);
    }

    public ServerConfiguration coordinatorPartitions(int coordinatorPartitions) {
        setInt(COORDINATOR_PARTITIONS_KEY, coordinatorPartitions);
        return this;
    }

    public int coordinatorPartitions() {
        return getInt(COORDINATOR_PARTITIONS_KEY);
    }

    public ServerConfiguration instanceHost(String instanceHost) {
        setString(INSTANCE_HOST_KEY, instanceHost);
        return this;
    }

    public String instanceHost() {
        return getString(INSTANCE_HOST_KEY);
    }

    public ServerConfiguration controllerName(String controllerName) {
        setString(CONTROLLER_NAME_KEY, controllerName);
        return this;
    }

    public String controllerName() {
        return getString(CONTROLLER_NAME_KEY);
    }

    public ServerConfiguration controllerMode(String controllerMode) {
        setString(CONTROLLER_MODE_KEY, controllerMode);
        return this;
    }

    public String controllerMode() {
        return getString(CONTROLLER_MODE_KEY);
    }

    public ServerConfiguration zkServers(String zkServers) {
        setString(ZK_SERVERS_KEY, zkServers);
        return this;
    }

    public String zkServers() {
        return getString(ZK_SERVERS_KEY);
    }

    public ServerConfiguration coordinatorPort(int coordinatorPort) {
        setInt(COORDINATOR_PORT_KEY, coordinatorPort);
        return this;
    }

    public int coordinatorPort() {
        return getInt(COORDINATOR_PORT_KEY);
    }


    public int executorPort() {
        return getInt(EXECUTOR_PORT_KEY);
    }

    public ServerConfiguration port(int port) {
        setInt(PORT_KEY, port);
        return this;
    }

    public int port() {
        return getInt(PORT_KEY);
    }

    public ServerConfiguration dataDir(String dataDir) {
        setString(DATA_DIR_KEY, dataDir);
        return this;
    }

    public String dataDir() {
        return getString(DATA_DIR_KEY, "/tmp");
    }

    public ServerConfiguration defaultReplicas(int defaultReplicas) {
        setInt(STORE_DEFAULT_REPLICAS_KEY, defaultReplicas);
        return this;
    }

    public int defaultReplicas() {
        return getInt(STORE_DEFAULT_REPLICAS_KEY, 3);
    }

    public ServerConfiguration defaultPartitions(int defaultPartitions) {
        setInt(STORE_DEFAULT_PARTITIONS_KEY, defaultPartitions);
        return this;
    }

    public int defaultPartitions() {
        return getInt(STORE_DEFAULT_PARTITIONS_KEY, 3);
    }

    public ServerConfiguration stateMode(String stateMode) {
        setString(STATE_MODE_KEY, stateMode);
        return this;
    }


    public String coordinatorRaftId() {
        return getString(COORDINATOR_RAFT_GROUP_ID, DEFAULT_COORDINATOR_RAFT_ID);
    }

    public List<PeerId> coordinatorServers() {
        List<String> servers = getList(COORDINATOR_SERVERS_KEY);
        return servers.stream()
               .map(server -> server.split(":"))
               .map(hostIp -> new PeerId(hostIp[0], Integer.parseInt(hostIp[1])))
               .collect(Collectors.toList());
    }

    public Boolean isMemoryRaft() {
        return exist(COORDINATOR_RAFT_MEMORY_KEY);
    }

    public MemoryDBOptions memoryCoordinator() {
        return getAndConvert(COORDINATOR_RAFT_MEMORY_KEY, MemoryDBOptions.class);
    }

    public String coordinatorRaftDataDir() {
        return getString(COORDINATOR_RAFT_DATA_DIR_KEY);
    }


}
