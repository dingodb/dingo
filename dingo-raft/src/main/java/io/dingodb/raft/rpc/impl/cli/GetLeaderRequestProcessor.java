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
import io.dingodb.raft.Node;
import io.dingodb.raft.NodeManager;
import io.dingodb.raft.Status;
import io.dingodb.raft.entity.PeerId;
import io.dingodb.raft.error.RaftError;
import io.dingodb.raft.rpc.CliRequests;
import io.dingodb.raft.rpc.RpcRequestClosure;
import io.dingodb.raft.util.RpcFactoryHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class GetLeaderRequestProcessor extends BaseCliRequestProcessor<CliRequests.GetLeaderRequest> {
    public GetLeaderRequestProcessor(Executor executor) {
        super(executor, CliRequests.GetLeaderResponse.getDefaultInstance());
    }

    @Override
    protected String getPeerId(final CliRequests.GetLeaderRequest request) {
        return request.getPeerId();
    }

    @Override
    protected String getGroupId(final CliRequests.GetLeaderRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final CliRequests.GetLeaderRequest request,
                                      final RpcRequestClosure done) {
        // ignore
        return null;
    }

    @Override
    public Message processRequest(final CliRequests.GetLeaderRequest request, final RpcRequestClosure done) {
        List<Node> nodes = new ArrayList<>();
        final String groupId = getGroupId(request);
        if (request.hasPeerId()) {
            final String peerIdStr = getPeerId(request);
            final PeerId peer = new PeerId();
            if (peer.parse(peerIdStr)) {
                final Status st = new Status();
                nodes.add(getNode(groupId, peer, st));
                if (!st.isOk()) {
                    return RpcFactoryHelper //
                        .responseFactory() //
                        .newResponse(defaultResp(), st);
                }
            } else {
                return RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(defaultResp(), RaftError.EINVAL, "Fail to parse peer id %s", peerIdStr);
            }
        } else {
            nodes = NodeManager.getInstance().getNodesByGroupId(groupId);
        }
        if (nodes == null || nodes.isEmpty()) {
            return RpcFactoryHelper //
                .responseFactory() //
                .newResponse(defaultResp(), RaftError.ENOENT, "No nodes in group %s", groupId);
        }
        for (final Node node : nodes) {
            final PeerId leader = node.getLeaderId();
            if (leader != null && !leader.isEmpty()) {
                return CliRequests.GetLeaderResponse.newBuilder().setLeaderId(leader.toString()).build();
            }
        }
        return RpcFactoryHelper //
            .responseFactory() //
            .newResponse(defaultResp(), RaftError.EAGAIN, "Unknown leader");
    }

    @Override
    public String interest() {
        return CliRequests.GetLeaderRequest.class.getName();
    }

}
