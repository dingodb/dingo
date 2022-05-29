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

package io.dingodb.raft.rpc.dingo;

import com.google.protobuf.Message;
import io.dingodb.net.RaftTag;
import io.dingodb.raft.rpc.CliClientService;
import io.dingodb.raft.rpc.CliRequests;
import io.dingodb.raft.rpc.RpcRequests;
import io.dingodb.raft.rpc.RpcResponseClosure;
import io.dingodb.raft.util.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;

public class DingoCliClientServiceImpl extends AbstractClientService implements CliClientService {

    protected static final Logger LOG = LoggerFactory.getLogger(DingoCliClientServiceImpl.class);

    @Override
    public Future<Message> addPeer(Endpoint endpoint, CliRequests.AddPeerRequest request, RpcResponseClosure<CliRequests.AddPeerResponse> done) {
        return invokeWithDone(endpoint, RaftTag.ADDPEER_REQUEST, request, done, CliRequests.AddPeerResponse::parseFrom);
    }

    @Override
    public Future<Message> removePeer(Endpoint endpoint, CliRequests.RemovePeerRequest request, RpcResponseClosure<CliRequests.RemovePeerResponse> done) {
        return invokeWithDone(endpoint, RaftTag.REMOVEPEER_REQUEST, request, done, CliRequests.RemovePeerResponse::parseFrom);
    }

    @Override
    public Future<Message> resetPeer(Endpoint endpoint, CliRequests.ResetPeerRequest request, RpcResponseClosure<RpcRequests.ErrorResponse> done) {
        return invokeWithDone(endpoint, RaftTag.RESETPEER_REQUEST, request, done, RpcRequests.ErrorResponse::parseFrom);
    }

    @Override
    public Future<Message> snapshot(Endpoint endpoint, CliRequests.SnapshotRequest request, RpcResponseClosure<RpcRequests.ErrorResponse> done) {
        return invokeWithDone(endpoint, RaftTag.SNAPSHOT_REQUEST, request, done, RpcRequests.ErrorResponse::parseFrom);
    }

    @Override
    public Future<Message> changePeers(Endpoint endpoint, CliRequests.ChangePeersRequest request, RpcResponseClosure<CliRequests.ChangePeersResponse> done) {
        return invokeWithDone(endpoint, RaftTag.CHANGEPEERS_REQUEST, request, done, CliRequests.ChangePeersResponse::parseFrom);
    }

    @Override
    public Future<Message> addLearners(Endpoint endpoint, CliRequests.AddLearnersRequest request, RpcResponseClosure<CliRequests.LearnersOpResponse> done) {
        return invokeWithDone(endpoint, RaftTag.ADDLEARNERS_REQUEST, request, done, CliRequests.LearnersOpResponse::parseFrom);
    }

    @Override
    public Future<Message> removeLearners(Endpoint endpoint, CliRequests.RemoveLearnersRequest request, RpcResponseClosure<CliRequests.LearnersOpResponse> done) {
        return invokeWithDone(endpoint, RaftTag.REMOVELEARNERS_REQUEST, request, done, CliRequests.LearnersOpResponse::parseFrom);
    }

    @Override
    public Future<Message> resetLearners(Endpoint endpoint, CliRequests.ResetLearnersRequest request, RpcResponseClosure<CliRequests.LearnersOpResponse> done) {
        return invokeWithDone(endpoint, RaftTag.RESETLEARNERS_REQUEST, request, done, CliRequests.LearnersOpResponse::parseFrom);
    }

    @Override
    public Future<Message> getLeader(Endpoint endpoint, CliRequests.GetLeaderRequest request, RpcResponseClosure<CliRequests.GetLeaderResponse> done) {
        return invokeWithDone(endpoint, RaftTag.GETLEADER_REQUEST, request, done, CliRequests.GetLeaderResponse::parseFrom);
    }

    @Override
    public Future<Message> transferLeader(Endpoint endpoint, CliRequests.TransferLeaderRequest request, RpcResponseClosure<RpcRequests.ErrorResponse> done) {
        return invokeWithDone(endpoint, RaftTag.TRANSFERLEADER_REQUEST, request, done, RpcRequests.ErrorResponse::parseFrom);
    }

    @Override
    public Future<Message> getPeers(Endpoint endpoint, CliRequests.GetPeersRequest request, RpcResponseClosure<CliRequests.GetPeersResponse> done) {
        return invokeWithDone(endpoint, RaftTag.GETPEERS_REQUEST, request, done, CliRequests.GetPeersResponse::parseFrom);
    }
}
