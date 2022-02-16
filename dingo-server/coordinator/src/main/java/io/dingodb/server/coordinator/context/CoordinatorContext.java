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

package io.dingodb.server.coordinator.context;

import io.dingodb.net.NetService;
import io.dingodb.raft.Node;
import io.dingodb.raft.rpc.RpcServer;
import io.dingodb.raft.util.Endpoint;
import io.dingodb.server.coordinator.config.CoordinatorConfiguration;
import io.dingodb.server.coordinator.config.CoordinatorOptions;
import io.dingodb.server.coordinator.meta.RowStoreMetaAdaptor;
import io.dingodb.server.coordinator.meta.ScheduleMetaAdaptor;
import io.dingodb.server.coordinator.meta.TableMetaAdaptor;
import io.dingodb.server.coordinator.meta.service.CoordinatorMetaService;
import io.dingodb.server.coordinator.service.LeaderFollowerServiceProvider;
import io.dingodb.server.coordinator.state.CoordinatorStateMachine;
import io.dingodb.server.coordinator.store.AsyncKeyValueStore;
import io.dingodb.store.row.storage.RocksRawKVStore;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Getter
@Setter
@ToString
@Accessors(chain = true, fluent = true)
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class CoordinatorContext {

    private Endpoint endpoint;

    private Node node;

    private NetService netService;
    private RpcServer rpcServer;
    private CoordinatorMetaService metaService;

    //private CoordinatorConfiguration configuration;
    private CoordinatorOptions coordOpts;

    private CoordinatorStateMachine stateMachine;

    private RocksRawKVStore rocksKVStore;
    private AsyncKeyValueStore keyValueStore;

    private ScheduleMetaAdaptor scheduleMetaAdaptor;
    private TableMetaAdaptor tableMetaAdaptor;
    private RowStoreMetaAdaptor rowStoreMetaAdaptor;

    private LeaderFollowerServiceProvider serviceProvider;

    public boolean isLeader() {
        return stateMachine.isLeader();
    }

}
