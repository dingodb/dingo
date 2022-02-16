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
import io.dingodb.raft.conf.Configuration;
import io.dingodb.raft.entity.PeerId;
import io.dingodb.raft.error.RaftError;
import io.dingodb.raft.rpc.CliRequests;
import io.dingodb.raft.rpc.RpcRequestClosure;
import io.dingodb.raft.util.RpcFactoryHelper;

import java.util.List;
import java.util.concurrent.Executor;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class ChangePeersRequestProcessor extends BaseCliRequestProcessor<CliRequests.ChangePeersRequest> {
    public ChangePeersRequestProcessor(Executor executor) {
        super(executor, CliRequests.ChangePeersResponse.getDefaultInstance());
    }

    @Override
    protected String getPeerId(final CliRequests.ChangePeersRequest request) {
        return request.getLeaderId();
    }

    @Override
    protected String getGroupId(final CliRequests.ChangePeersRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final CliRequests.ChangePeersRequest request, final RpcRequestClosure done) {
        final List<PeerId> oldConf = ctx.node.listPeers();

        final Configuration conf = new Configuration();
        for (final String peerIdStr : request.getNewPeersList()) {
            final PeerId peer = new PeerId();
            if (peer.parse(peerIdStr)) {
                conf.addPeer(peer);
            } else {
                return RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(defaultResp(), RaftError.EINVAL, "Fail to parse peer id %s", peerIdStr);
            }
        }
        LOG.info("Receive ChangePeersRequest to {} from {}, new conf is {}", ctx.node.getNodeId(), done.getRpcCtx()
            .getRemoteAddress(), conf);
        ctx.node.changePeers(conf, status -> {
            if (!status.isOk()) {
                done.run(status);
            } else {
                CliRequests.ChangePeersResponse.Builder rb = CliRequests.ChangePeersResponse.newBuilder();
                for (final PeerId peer : oldConf) {
                    rb.addOldPeers(peer.toString());
                }
                for (final PeerId peer : conf) {
                    rb.addNewPeers(peer.toString());
                }
                done.sendResponse(rb.build());
            }
        });
        return null;
    }

    @Override
    public String interest() {
        return CliRequests.ChangePeersRequest.class.getName();
    }
}
