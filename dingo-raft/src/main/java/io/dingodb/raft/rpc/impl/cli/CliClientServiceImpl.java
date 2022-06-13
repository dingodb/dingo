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

package io.dingodb.raft.rpc.impl.cli;

import com.google.protobuf.Message;
import io.dingodb.raft.option.CliOptions;
import io.dingodb.raft.option.RpcOptions;
import io.dingodb.raft.rpc.CliClientService;
import io.dingodb.raft.rpc.CliRequests;
import io.dingodb.raft.rpc.RpcRequests;
import io.dingodb.raft.rpc.RpcResponseClosure;
import io.dingodb.raft.rpc.impl.AbstractClientService;
import io.dingodb.raft.util.Endpoint;

import java.util.concurrent.Future;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class CliClientServiceImpl extends AbstractClientService implements CliClientService {
    private CliOptions cliOptions;

    @Override
    public synchronized boolean init(final RpcOptions rpcOptions) {
        boolean ret = super.init(rpcOptions);
        if (ret) {
            this.cliOptions = (CliOptions) this.rpcOptions;
        }
        return ret;
    }

    @Override
    public Future<Message> addPeer(final Endpoint endpoint, final CliRequests.AddPeerRequest request,
                                   final RpcResponseClosure<CliRequests.AddPeerResponse> done) {
        return invokeWithDone(endpoint, request, done, this.cliOptions.getTimeoutMs());
    }

    @Override
    public Future<Message> removePeer(final Endpoint endpoint, final CliRequests.RemovePeerRequest request,
                                      final RpcResponseClosure<CliRequests.RemovePeerResponse> done) {
        return invokeWithDone(endpoint, request, done, this.cliOptions.getTimeoutMs());
    }

    @Override
    public Future<Message> resetPeer(final Endpoint endpoint, final CliRequests.ResetPeerRequest request,
                                     final RpcResponseClosure<RpcRequests.ErrorResponse> done) {
        return invokeWithDone(endpoint, request, done, this.cliOptions.getTimeoutMs());
    }

    @Override
    public Future<Message> snapshot(final Endpoint endpoint, final CliRequests.SnapshotRequest request,
                                    final RpcResponseClosure<RpcRequests.ErrorResponse> done) {
        return invokeWithDone(endpoint, request, done, this.cliOptions.getTimeoutMs());
    }

    @Override
    public Future<Message> changePeers(final Endpoint endpoint, final CliRequests.ChangePeersRequest request,
                                       final RpcResponseClosure<CliRequests.ChangePeersResponse> done) {
        return invokeWithDone(endpoint, request, done, this.cliOptions.getTimeoutMs());
    }

    @Override
    public Future<Message> addLearners(final Endpoint endpoint, final CliRequests.AddLearnersRequest request,
                                       final RpcResponseClosure<CliRequests.LearnersOpResponse> done) {
        return invokeWithDone(endpoint, request, done, this.cliOptions.getTimeoutMs());
    }

    @Override
    public Future<Message> removeLearners(final Endpoint endpoint, final CliRequests.RemoveLearnersRequest request,
                                          final RpcResponseClosure<CliRequests.LearnersOpResponse> done) {
        return invokeWithDone(endpoint, request, done, this.cliOptions.getTimeoutMs());
    }

    @Override
    public Future<Message> resetLearners(final Endpoint endpoint, final CliRequests.ResetLearnersRequest request,
                                         final RpcResponseClosure<CliRequests.LearnersOpResponse> done) {
        return invokeWithDone(endpoint, request, done, this.cliOptions.getTimeoutMs());
    }

    @Override
    public Future<Message> getLeader(final Endpoint endpoint, final CliRequests.GetLeaderRequest request,
                                     final RpcResponseClosure<CliRequests.GetLeaderResponse> done) {
        return invokeWithDone(endpoint, request, done, this.cliOptions.getTimeoutMs());
    }

    @Override
    public Future<Message> transferLeader(final Endpoint endpoint, final CliRequests.TransferLeaderRequest request,
                                          final RpcResponseClosure<RpcRequests.ErrorResponse> done) {
        return invokeWithDone(endpoint, request, done, this.cliOptions.getTimeoutMs());
    }

    @Override
    public Future<Message> getPeers(final Endpoint endpoint, final CliRequests.GetPeersRequest request,
                                    final RpcResponseClosure<CliRequests.GetPeersResponse> done) {
        return invokeWithDone(endpoint, request, done, this.cliOptions.getTimeoutMs());
    }
}
