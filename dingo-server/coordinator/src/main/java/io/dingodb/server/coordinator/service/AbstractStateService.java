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

package io.dingodb.server.coordinator.service;


import io.dingodb.raft.Node;
import io.dingodb.raft.core.NodeImpl;
import io.dingodb.raft.rpc.RpcClient;
import io.dingodb.raft.rpc.impl.AbstractClientService;
import io.dingodb.server.coordinator.context.CoordinatorContext;

public abstract class AbstractStateService implements StateService {

    protected final CoordinatorContext context;
    protected final Node node;
    protected final RpcClient rpcClient;

    protected boolean stop = false;

    public AbstractStateService(CoordinatorContext coordinatorContext) {
        this.context = coordinatorContext;
        this.node = context.node();
        this.rpcClient = ((AbstractClientService) ((NodeImpl) context.node()).getRpcService()).getRpcClient();
    }

    @Override
    public boolean isStop() {
        return stop;
    }

    @Override
    public void start() {
        stop = false;
    }

    @Override
    public void stop() {
        stop = true;
    }
}
