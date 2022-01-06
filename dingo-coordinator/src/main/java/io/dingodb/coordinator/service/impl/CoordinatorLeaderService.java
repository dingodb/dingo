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

package io.dingodb.coordinator.service.impl;

import com.alipay.sofa.jraft.rpc.RpcServer;
import io.dingodb.common.concurrent.ThreadPoolBuilder;
import io.dingodb.coordinator.context.CoordinatorContext;
import io.dingodb.coordinator.handler.GetClusterInfoHandler;
import io.dingodb.coordinator.handler.GetStoreIdHandler;
import io.dingodb.coordinator.handler.GetStoreInfoHandler;
import io.dingodb.coordinator.handler.RegionHeartbeatHandler;
import io.dingodb.coordinator.handler.SetStoreHandler;
import io.dingodb.coordinator.handler.StoreHeartbeatHandler;
import io.dingodb.coordinator.meta.impl.MetaAdaptorImpl;
import io.dingodb.coordinator.service.AbstractStateService;
import io.dingodb.dingokv.rpc.ExtSerializerSupports;
import io.dingodb.net.NetService;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ThreadPoolExecutor;

import static com.alipay.sofa.jraft.rpc.RaftRpcServerFactory.createRaftRpcServer;

@Slf4j
public class CoordinatorLeaderService extends AbstractStateService {

    private final NetService netService;
    private RpcServer rpcServer;

    public CoordinatorLeaderService(CoordinatorContext context, NetService netService) {
        super(context);
        this.netService = netService;
    }

    @Override
    public void start() {
        initRpcServer();
    }

    @Override
    public void stop() {
        rpcServer.shutdown();
    }


    private RpcServer initRpcServer() {
        ExtSerializerSupports.init();
        RpcServer rpcServer = createRaftRpcServer(context.endpoint(), raftExecutor(), cliExecutor());
        MetaAdaptorImpl metaAdaptor = new MetaAdaptorImpl(context.metaStore());
        rpcServer.registerProcessor(new GetClusterInfoHandler(metaAdaptor));
        rpcServer.registerProcessor(new GetStoreInfoHandler(metaAdaptor));
        rpcServer.registerProcessor(new GetStoreIdHandler(metaAdaptor));
        rpcServer.registerProcessor(new RegionHeartbeatHandler(metaAdaptor));
        rpcServer.registerProcessor(new SetStoreHandler(metaAdaptor));
        rpcServer.registerProcessor(new StoreHeartbeatHandler(metaAdaptor));
        log.info("Coordinator leader service init rpc server, result: {}.", rpcServer.init(null));
        return rpcServer;
    }

    private ThreadPoolExecutor raftExecutor() {
        return new ThreadPoolBuilder()
            .name("Raft-leader")
            .build();
    }

    private ThreadPoolExecutor cliExecutor() {
        return new ThreadPoolBuilder()
            .name("Cli-leader")
            .build();
    }

}
