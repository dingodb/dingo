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

package io.dingodb.raft.rpc;

import io.dingodb.raft.rpc.impl.PingRequestProcessor;
import io.dingodb.raft.rpc.impl.cli.AddLearnersRequestProcessor;
import io.dingodb.raft.rpc.impl.cli.AddPeerRequestProcessor;
import io.dingodb.raft.rpc.impl.cli.ChangePeersRequestProcessor;
import io.dingodb.raft.rpc.impl.cli.GetLeaderRequestProcessor;
import io.dingodb.raft.rpc.impl.cli.GetLocationProcessor;
import io.dingodb.raft.rpc.impl.cli.GetPeersRequestProcessor;
import io.dingodb.raft.rpc.impl.cli.RemoveLearnersRequestProcessor;
import io.dingodb.raft.rpc.impl.cli.RemovePeerRequestProcessor;
import io.dingodb.raft.rpc.impl.cli.ResetLearnersRequestProcessor;
import io.dingodb.raft.rpc.impl.cli.ResetPeerRequestProcessor;
import io.dingodb.raft.rpc.impl.cli.SnapshotRequestProcessor;
import io.dingodb.raft.rpc.impl.cli.TransferLeaderRequestProcessor;
import io.dingodb.raft.rpc.impl.core.AppendEntriesRequestProcessor;
import io.dingodb.raft.rpc.impl.core.GetFileRequestProcessor;
import io.dingodb.raft.rpc.impl.core.InstallSnapshotRequestProcessor;
import io.dingodb.raft.rpc.impl.core.ReadIndexRequestProcessor;
import io.dingodb.raft.rpc.impl.core.RequestVoteRequestProcessor;
import io.dingodb.raft.rpc.impl.core.TimeoutNowRequestProcessor;
import io.dingodb.raft.util.Endpoint;
import io.dingodb.raft.util.RpcFactoryHelper;

import java.util.concurrent.Executor;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class RaftRpcServerFactory {
    static {
        ProtobufMsgFactory.load();
    }

    /**
     * Creates a raft RPC server with default request executors.
     *
     * @param endpoint server address to bind
     * @return a rpc server instance
     */
    public static RpcServer createRaftRpcServer(final Endpoint endpoint) {
        return createRaftRpcServer(endpoint, null, null);
    }

    /**
     * Creates a raft RPC server with executors to handle requests.
     *
     * @param endpoint      server address to bind
     * @param raftExecutor  executor to handle RAFT requests.
     * @param cliExecutor   executor to handle CLI service requests.
     * @return a rpc server instance
     */
    public static RpcServer createRaftRpcServer(final Endpoint endpoint, final Executor raftExecutor,
                                                final Executor cliExecutor) {
        final RpcServer rpcServer = RpcFactoryHelper.rpcFactory().createRpcServer(endpoint);
        addRaftRequestProcessors(rpcServer, raftExecutor, cliExecutor);
        return rpcServer;
    }

    /**
     * Adds RAFT and CLI service request processors with default executor.
     *
     * @param rpcServer rpc server instance
     */
    public static void addRaftRequestProcessors(final RpcServer rpcServer) {
        addRaftRequestProcessors(rpcServer, null, null);
    }

    /**
     * Adds RAFT and CLI service request processors.
     *
     * @param rpcServer    rpc server instance
     * @param raftExecutor executor to handle RAFT requests.
     * @param cliExecutor  executor to handle CLI service requests.
     */
    public static void addRaftRequestProcessors(final RpcServer rpcServer, final Executor raftExecutor,
                                                final Executor cliExecutor) {
        // raft core processors
        final AppendEntriesRequestProcessor appendEntriesRequestProcessor = new AppendEntriesRequestProcessor(
            raftExecutor);
        rpcServer.registerConnectionClosedEventListener(appendEntriesRequestProcessor);
        rpcServer.registerProcessor(appendEntriesRequestProcessor);
        rpcServer.registerProcessor(new GetFileRequestProcessor(raftExecutor));
        rpcServer.registerProcessor(new InstallSnapshotRequestProcessor(raftExecutor));
        rpcServer.registerProcessor(new RequestVoteRequestProcessor(raftExecutor));
        rpcServer.registerProcessor(new PingRequestProcessor());
        rpcServer.registerProcessor(new TimeoutNowRequestProcessor(raftExecutor));
        rpcServer.registerProcessor(new ReadIndexRequestProcessor(raftExecutor));
        // raft cli service
        rpcServer.registerProcessor(new AddPeerRequestProcessor(cliExecutor));
        rpcServer.registerProcessor(new RemovePeerRequestProcessor(cliExecutor));
        rpcServer.registerProcessor(new ResetPeerRequestProcessor(cliExecutor));
        rpcServer.registerProcessor(new ChangePeersRequestProcessor(cliExecutor));
        rpcServer.registerProcessor(new GetLeaderRequestProcessor(cliExecutor));
        rpcServer.registerProcessor(new SnapshotRequestProcessor(cliExecutor));
        rpcServer.registerProcessor(new TransferLeaderRequestProcessor(cliExecutor));
        rpcServer.registerProcessor(new GetPeersRequestProcessor(cliExecutor));
        rpcServer.registerProcessor(new AddLearnersRequestProcessor(cliExecutor));
        rpcServer.registerProcessor(new RemoveLearnersRequestProcessor(cliExecutor));
        rpcServer.registerProcessor(new ResetLearnersRequestProcessor(cliExecutor));
        rpcServer.registerProcessor(new GetLocationProcessor());
    }

    /**
     * Creates a raft RPC server and starts it.
     *
     * @param endpoint server address to bind
     * @return a rpc server instance
     */
    public static RpcServer createAndStartRaftRpcServer(final Endpoint endpoint) {
        return createAndStartRaftRpcServer(endpoint, null, null);
    }

    /**
     * Creates a raft RPC server and starts it.
     *
     * @param endpoint     server address to bind
     * @param raftExecutor executor to handle RAFT requests.
     * @param cliExecutor  executor to handle CLI service requests.
     * @return a rpc server instance
     */
    public static RpcServer createAndStartRaftRpcServer(final Endpoint endpoint, final Executor raftExecutor,
                                                        final Executor cliExecutor) {
        final RpcServer server = createRaftRpcServer(endpoint, raftExecutor, cliExecutor);
        server.init(null);
        return server;
    }
}
