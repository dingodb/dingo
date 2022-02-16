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
import io.dingodb.raft.conf.Configuration;
import io.dingodb.raft.entity.PeerId;
import io.dingodb.raft.error.RaftError;
import io.dingodb.raft.rpc.CliRequests;
import io.dingodb.raft.rpc.RpcRequestClosure;
import io.dingodb.raft.rpc.RpcRequests;
import io.dingodb.raft.util.RpcFactoryHelper;

import java.util.concurrent.Executor;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class ResetPeerRequestProcessor extends BaseCliRequestProcessor<CliRequests.ResetPeerRequest> {
    public ResetPeerRequestProcessor(Executor executor) {
        super(executor, RpcRequests.ErrorResponse.getDefaultInstance());
    }

    @Override
    protected String getPeerId(final CliRequests.ResetPeerRequest request) {
        return request.getPeerId();
    }

    @Override
    protected String getGroupId(final CliRequests.ResetPeerRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final CliRequests.ResetPeerRequest request,
                                      final RpcRequestClosure done) {
        final Configuration newConf = new Configuration();
        for (final String peerIdStr : request.getNewPeersList()) {
            final PeerId peer = new PeerId();
            if (peer.parse(peerIdStr)) {
                newConf.addPeer(peer);
            } else {
                return RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(defaultResp(), RaftError.EINVAL, "Fail to parse peer id %s", peerIdStr);
            }
        }
        LOG.info("Receive ResetPeerRequest to {} from {}, new conf is {}", ctx.node.getNodeId(), done.getRpcCtx()
            .getRemoteAddress(), newConf);
        final Status st = ctx.node.resetPeers(newConf);
        return RpcFactoryHelper //
            .responseFactory() //
            .newResponse(defaultResp(), st);
    }

    @Override
    public String interest() {
        return CliRequests.ResetPeerRequest.class.getName();
    }

}
