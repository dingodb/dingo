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

package io.dingodb.coordinator.context;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.Endpoint;
import io.dingodb.coordinator.config.ServerConfiguration;
import io.dingodb.coordinator.meta.MetaStore;
import io.dingodb.coordinator.meta.impl.TableMetaAdaptorImpl;
import io.dingodb.coordinator.service.impl.CoordinatorLeaderFollowerServiceProvider;
import io.dingodb.coordinator.state.CoordinatorStateMachine;
import io.dingodb.coordinator.store.RaftAsyncKeyValueStore;
import io.dingodb.dingokv.storage.RocksRawKVStore;
import io.dingodb.meta.MetaService;
import io.dingodb.net.NetService;
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
    private NetService netService;
    private Node node;
    private RpcServer rpcServer;
    private MetaService metaService;
    private RaftAsyncKeyValueStore keyValueStore;
    private ServerConfiguration configuration;
    private RocksRawKVStore rocksKVStore;
    private CoordinatorStateMachine stateMachine;
    private TableMetaAdaptorImpl metaAdaptor;

    private MetaStore metaStore;

    private CoordinatorLeaderFollowerServiceProvider serviceProvider;

    public boolean isLeader() {
        return stateMachine.isLeader();
    }

}
