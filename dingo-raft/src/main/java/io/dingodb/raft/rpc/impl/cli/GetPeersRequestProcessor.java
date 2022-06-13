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
import io.dingodb.raft.entity.PeerId;
import io.dingodb.raft.rpc.CliRequests;
import io.dingodb.raft.rpc.RpcRequestClosure;

import java.util.List;
import java.util.concurrent.Executor;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class GetPeersRequestProcessor extends BaseCliRequestProcessor<CliRequests.GetPeersRequest> {
    public GetPeersRequestProcessor(final Executor executor) {
        super(executor, CliRequests.GetPeersResponse.getDefaultInstance());
    }

    @Override
    protected String getPeerId(final CliRequests.GetPeersRequest request) {
        return request.getLeaderId();
    }

    @Override
    protected String getGroupId(final CliRequests.GetPeersRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final CliRequests.GetPeersRequest request,
                                      final RpcRequestClosure done) {
        final List<PeerId> peers;
        final List<PeerId> learners;
        if (request.hasOnlyAlive() && request.getOnlyAlive()) {
            peers = ctx.node.listAlivePeers();
            learners = ctx.node.listAliveLearners();
        } else {
            peers = ctx.node.listPeers();
            learners = ctx.node.listLearners();
        }
        final CliRequests.GetPeersResponse.Builder builder = CliRequests.GetPeersResponse.newBuilder();
        for (final PeerId peerId : peers) {
            builder.addPeers(peerId.toString());
        }
        for (final PeerId peerId : learners) {
            builder.addLearners(peerId.toString());
        }
        return builder.build();
    }

    @Override
    public String interest() {
        return CliRequests.GetPeersRequest.class.getName();
    }
}
