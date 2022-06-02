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
import io.dingodb.raft.option.RpcOptions;
import io.dingodb.raft.rpc.RaftClientService;
import io.dingodb.raft.rpc.RpcRequests;
import io.dingodb.raft.rpc.RpcResponseClosure;
import io.dingodb.raft.util.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;

public class DingoRaftRpcClientService extends AbstractClientService implements RaftClientService {

    protected static final Logger LOG = LoggerFactory.getLogger(DingoRaftRpcClientService.class);

    @Override
    public boolean init(RpcOptions opts) {
        return true;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public Future<Message> preVote(Endpoint endpoint, RpcRequests.RequestVoteRequest request, RpcResponseClosure<RpcRequests.RequestVoteResponse> done) {
        return invokeWithDone(endpoint, Tags.REQUESTVOTE_REQUEST, request, done, RpcRequests.RequestVoteResponse::parseFrom);
    }

    @Override
    public Future<Message> requestVote(Endpoint endpoint, RpcRequests.RequestVoteRequest request, RpcResponseClosure<RpcRequests.RequestVoteResponse> done) {
        return invokeWithDone(endpoint, Tags.REQUESTVOTE_REQUEST, request, done, RpcRequests.RequestVoteResponse::parseFrom);
    }

    @Override
    public Future<Message> appendEntries(Endpoint endpoint, RpcRequests.AppendEntriesRequest request, int timeoutMs, RpcResponseClosure<RpcRequests.AppendEntriesResponse> done) {
        return invokeWithDone(endpoint, Tags.APPENDENTRIES_REQUEST, request, done, RpcRequests.AppendEntriesResponse::parseFrom);
    }

    @Override
    public Future<Message> installSnapshot(Endpoint endpoint, RpcRequests.InstallSnapshotRequest request, RpcResponseClosure<RpcRequests.InstallSnapshotResponse> done) {
        return invokeWithDone(endpoint, Tags.INSTALLSNAPSHOT_REQUEST, request, done, RpcRequests.InstallSnapshotResponse::parseFrom);
    }

    @Override
    public Future<Message> getFile(Endpoint endpoint, RpcRequests.GetFileRequest request, int timeoutMs, RpcResponseClosure<RpcRequests.GetFileResponse> done) {
        return invokeWithDone(endpoint, Tags.GETFILE_REQUEST, request, done, RpcRequests.GetFileResponse::parseFrom);
    }

    @Override
    public Future<Message> timeoutNow(Endpoint endpoint, RpcRequests.TimeoutNowRequest request, int timeoutMs, RpcResponseClosure<RpcRequests.TimeoutNowResponse> done) {
        return invokeWithDone(endpoint, Tags.TIMEOUTNOW_REQUEST, request, done, RpcRequests.TimeoutNowResponse::parseFrom);
    }

    @Override
    public Future<Message> readIndex(Endpoint endpoint, RpcRequests.ReadIndexRequest request, int timeoutMs, RpcResponseClosure<RpcRequests.ReadIndexResponse> done) {
        return invokeWithDone(endpoint, Tags.READINDEX_REQUEST, request, done, RpcRequests.ReadIndexResponse::parseFrom);
    }
}
