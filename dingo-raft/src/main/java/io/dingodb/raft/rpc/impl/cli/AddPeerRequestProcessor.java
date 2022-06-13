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
import io.dingodb.raft.error.RaftError;
import io.dingodb.raft.rpc.CliRequests;
import io.dingodb.raft.rpc.RpcRequestClosure;
import io.dingodb.raft.util.RpcFactoryHelper;

import java.util.List;
import java.util.concurrent.Executor;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class AddPeerRequestProcessor extends BaseCliRequestProcessor<CliRequests.AddPeerRequest> {
    public AddPeerRequestProcessor(Executor executor) {
        super(executor, CliRequests.AddPeerResponse.getDefaultInstance());
    }

    @Override
    protected String getPeerId(final CliRequests.AddPeerRequest request) {
        return request.getLeaderId();
    }

    @Override
    protected String getGroupId(final CliRequests.AddPeerRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final CliRequests.AddPeerRequest request, final RpcRequestClosure done) {
        final List<PeerId> oldPeers = ctx.node.listPeers();
        final String addingPeerIdStr = request.getPeerId();
        final PeerId addingPeer = new PeerId();
        if (addingPeer.parse(addingPeerIdStr)) {
            LOG.info("Receive AddPeerRequest to {} from {}, adding {}", ctx.node.getNodeId(), done.getRpcCtx()
                .getRemoteAddress(), addingPeerIdStr);
            ctx.node.addPeer(addingPeer, status -> {
                if (!status.isOk()) {
                    done.run(status);
                } else {
                    final CliRequests.AddPeerResponse.Builder rb = CliRequests.AddPeerResponse.newBuilder();
                    boolean alreadyExists = false;
                    for (final PeerId oldPeer : oldPeers) {
                        rb.addOldPeers(oldPeer.toString());
                        rb.addNewPeers(oldPeer.toString());
                        if (oldPeer.equals(addingPeer)) {
                            alreadyExists = true;
                        }
                    }
                    if (!alreadyExists) {
                        rb.addNewPeers(addingPeerIdStr);
                    }
                    done.sendResponse(rb.build());
                }
            });
        } else {
            return RpcFactoryHelper //
                .responseFactory() //
                .newResponse(defaultResp(), RaftError.EINVAL, "Fail to parse peer id %s", addingPeerIdStr);
        }

        return null;
    }

    @Override
    public String interest() {
        return CliRequests.AddPeerRequest.class.getName();
    }

}
