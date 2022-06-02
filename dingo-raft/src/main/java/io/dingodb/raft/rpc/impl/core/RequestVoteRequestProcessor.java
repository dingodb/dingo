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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.dingodb.raft.rpc.RaftServerService;
import io.dingodb.raft.rpc.RpcRequestClosure;
import io.dingodb.raft.rpc.RpcRequests;
import io.dingodb.raft.rpc.dingo.Tags;

import java.util.concurrent.Executor;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class RequestVoteRequestProcessor extends NodeRequestProcessor<RpcRequests.RequestVoteRequest> {
    public RequestVoteRequestProcessor(Executor executor) {
        super(executor, RpcRequests.RequestVoteResponse.getDefaultInstance());
    }

    @Override
    public RpcRequests.RequestVoteRequest parse(byte[] request) {
        try {
            return RpcRequests.RequestVoteRequest.parseFrom(request);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String getPeerId(final RpcRequests.RequestVoteRequest request) {
        return request.getPeerId();
    }

    @Override
    protected String getGroupId(final RpcRequests.RequestVoteRequest request) {
        return request.getGroupId();
    }

    @Override
    public Message processRequest0(final RaftServerService service, final RpcRequests.RequestVoteRequest request,
                                   final RpcRequestClosure done) {
        if (request.getPreVote()) {
            return service.handlePreVoteRequest(request);
        } else {
            return service.handleRequestVoteRequest(request);
        }
    }

    @Override
    public String interest() {
        return RpcRequests.RequestVoteRequest.class.getName();
    }

    @Override
    public String getRequestTag() {
        return Tags.REQUESTVOTE_REQUEST;
    }

    @Override
    public String getResponseTag() {
        return Tags.REQUESTVOTE_RESPONSE;
    }
}
