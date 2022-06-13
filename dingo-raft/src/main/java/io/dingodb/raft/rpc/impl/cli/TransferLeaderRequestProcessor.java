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
import io.dingodb.raft.Status;
import io.dingodb.raft.entity.PeerId;
import io.dingodb.raft.error.RaftError;
import io.dingodb.raft.rpc.CliRequests;
import io.dingodb.raft.rpc.RpcRequestClosure;
import io.dingodb.raft.rpc.RpcRequests;
import io.dingodb.raft.util.RpcFactoryHelper;

import java.util.concurrent.Executor;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class TransferLeaderRequestProcessor extends BaseCliRequestProcessor<CliRequests.TransferLeaderRequest> {
    public TransferLeaderRequestProcessor(Executor executor) {
        super(executor, RpcRequests.ErrorResponse.getDefaultInstance());
    }

    @Override
    protected String getPeerId(final CliRequests.TransferLeaderRequest request) {
        return request.getLeaderId();
    }

    @Override
    protected String getGroupId(final CliRequests.TransferLeaderRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final CliRequests.TransferLeaderRequest request,
                                      final RpcRequestClosure done) {
        final PeerId peer = new PeerId();
        if (request.hasPeerId() && !peer.parse(request.getPeerId())) {
            return RpcFactoryHelper //
                .responseFactory() //
                .newResponse(defaultResp(), RaftError.EINVAL, "Fail to parse peer id %s", request.getPeerId());
        }
        LOG.info("Receive TransferLeaderRequest to {} from {}, newLeader will be {}.", ctx.node.getNodeId(), done
            .getRpcCtx().getRemoteAddress(), peer);
        final Status st = ctx.node.transferLeadershipTo(peer);
        return RpcFactoryHelper //
            .responseFactory() //
            .newResponse(defaultResp(), st);
    }

    @Override
    public String interest() {
        return CliRequests.TransferLeaderRequest.class.getName();
    }

}
