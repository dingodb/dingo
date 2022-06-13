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

package io.dingodb.raft.rpc.impl.core;

import com.google.protobuf.Message;
import io.dingodb.raft.Closure;
import io.dingodb.raft.ReplicatorGroup;
import io.dingodb.raft.Status;
import io.dingodb.raft.error.RaftError;
import io.dingodb.raft.error.RemotingException;
import io.dingodb.raft.option.NodeOptions;
import io.dingodb.raft.option.RpcOptions;
import io.dingodb.raft.rpc.InvokeContext;
import io.dingodb.raft.rpc.RaftClientService;
import io.dingodb.raft.rpc.RpcClient;
import io.dingodb.raft.rpc.RpcRequests;
import io.dingodb.raft.rpc.RpcResponseClosure;
import io.dingodb.raft.rpc.impl.AbstractClientService;
import io.dingodb.raft.rpc.impl.FutureImpl;
import io.dingodb.raft.util.Endpoint;
import io.dingodb.raft.util.Utils;
import io.dingodb.raft.util.concurrent.DefaultFixedThreadsExecutorGroupFactory;
import io.dingodb.raft.util.concurrent.FixedThreadsExecutorGroup;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class DefaultRaftClientService extends AbstractClientService implements RaftClientService {
    private static final FixedThreadsExecutorGroup  APPEND_ENTRIES_EXECUTORS
        = DefaultFixedThreadsExecutorGroupFactory
        .INSTANCE.newExecutorGroup(Utils.APPEND_ENTRIES_THREADS_SEND,
            "Append-Entries-Thread-Send", Utils.MAX_APPEND_ENTRIES_TASKS_PER_THREAD, true);

    private final ConcurrentMap<Endpoint, Executor> appendEntriesExecutorMap = new ConcurrentHashMap<>();

    // cached node options
    private NodeOptions nodeOptions;
    private final ReplicatorGroup rgGroup;

    @Override
    protected void configRpcClient(final RpcClient rpcClient) {
        rpcClient.registerConnectEventListener(this.rgGroup);
    }

    public DefaultRaftClientService(final ReplicatorGroup rgGroup) {
        this.rgGroup = rgGroup;
    }

    @Override
    public synchronized boolean init(final RpcOptions rpcOptions) {
        final boolean ret = super.init(rpcOptions);
        if (ret) {
            this.nodeOptions = (NodeOptions) rpcOptions;
        }
        return ret;
    }

    @Override
    public Future<Message> preVote(final Endpoint endpoint, final RpcRequests.RequestVoteRequest request,
                                   final RpcResponseClosure<RpcRequests.RequestVoteResponse> done) {
        if (!checkConnection(endpoint, true)) {
            return onConnectionFail(endpoint, request, done, this.rpcExecutor);
        }

        return invokeWithDone(endpoint, request, done, this.nodeOptions.getElectionTimeoutMs());
    }

    @Override
    public Future<Message> requestVote(final Endpoint endpoint, final RpcRequests.RequestVoteRequest request,
                                       final RpcResponseClosure<RpcRequests.RequestVoteResponse> done) {
        if (!checkConnection(endpoint, true)) {
            return onConnectionFail(endpoint, request, done, this.rpcExecutor);
        }

        return invokeWithDone(endpoint, request, done, this.nodeOptions.getElectionTimeoutMs());
    }

    @Override
    public Future<Message> appendEntries(final Endpoint endpoint, final RpcRequests.AppendEntriesRequest request,
                                         final int timeoutMs, final RpcResponseClosure<RpcRequests.AppendEntriesResponse> done) {
        final Executor executor = this.appendEntriesExecutorMap.computeIfAbsent(endpoint, k -> APPEND_ENTRIES_EXECUTORS.next());

        if (!checkConnection(endpoint, true)) {
            return onConnectionFail(endpoint, request, done, executor);
        }

        return invokeWithDone(endpoint, request, done, timeoutMs, executor);
    }

    @Override
    public Future<Message> getFile(final Endpoint endpoint, final RpcRequests.GetFileRequest request, final int timeoutMs,
                                   final RpcResponseClosure<RpcRequests.GetFileResponse> done) {
        // open checksum
        final InvokeContext ctx = new InvokeContext();
        ctx.put(InvokeContext.CRC_SWITCH, true);
        return invokeWithDone(endpoint, request, ctx, done, timeoutMs);
    }

    @Override
    public Future<Message> installSnapshot(final Endpoint endpoint, final RpcRequests.InstallSnapshotRequest request,
                                           final RpcResponseClosure<RpcRequests.InstallSnapshotResponse> done) {
        return invokeWithDone(endpoint, request, done, this.rpcOptions.getRpcInstallSnapshotTimeout());
    }

    @Override
    public Future<Message> timeoutNow(final Endpoint endpoint, final RpcRequests.TimeoutNowRequest request, final int timeoutMs,
                                      final RpcResponseClosure<RpcRequests.TimeoutNowResponse> done) {
        return invokeWithDone(endpoint, request, done, timeoutMs);
    }

    @Override
    public Future<Message> readIndex(final Endpoint endpoint, final RpcRequests.ReadIndexRequest request, final int timeoutMs,
                                     final RpcResponseClosure<RpcRequests.ReadIndexResponse> done) {
        return invokeWithDone(endpoint, request, done, timeoutMs);
    }

    // fail-fast when no connection
    private Future<Message> onConnectionFail(final Endpoint endpoint, final Message request, Closure done, final Executor executor) {
        final FutureImpl<Message> future = new FutureImpl<>();
        executor.execute(() -> {
            final String fmt = "Check connection[%s] fail and try to create new one";
            if (done != null) {
                try {
                    done.run(new Status(RaftError.EINTERNAL, fmt, endpoint));
                } catch (final Throwable t) {
                    LOG.error("Fail to run RpcResponseClosure, the request is {}.", request, t);
                }
            }
            if (!future.isDone()) {
                future.failure(new RemotingException(String.format(fmt, endpoint)));
            }
        });
        return future;
    }
}
