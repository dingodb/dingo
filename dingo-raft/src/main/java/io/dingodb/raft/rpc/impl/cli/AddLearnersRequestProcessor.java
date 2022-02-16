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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class AddLearnersRequestProcessor extends BaseCliRequestProcessor<CliRequests.AddLearnersRequest> {
    public AddLearnersRequestProcessor(final Executor executor) {
        super(executor, CliRequests.LearnersOpResponse.getDefaultInstance());
    }

    @Override
    protected String getPeerId(final CliRequests.AddLearnersRequest request) {
        return request.getLeaderId();
    }

    @Override
    protected String getGroupId(final CliRequests.AddLearnersRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final CliRequests.AddLearnersRequest request,
                                      final RpcRequestClosure done) {
        final List<PeerId> oldLearners = ctx.node.listLearners();
        final List<PeerId> addingLearners = new ArrayList<>(request.getLearnersCount());

        for (final String peerStr : request.getLearnersList()) {
            final PeerId peer = new PeerId();
            if (!peer.parse(peerStr)) {
                return RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(defaultResp(), RaftError.EINVAL, "Fail to parse peer id %s", peerStr);
            }
            addingLearners.add(peer);
        }

        LOG.info("Receive AddLearnersRequest to {} from {}, adding {}.", ctx.node.getNodeId(),
            done.getRpcCtx().getRemoteAddress(), addingLearners);
        ctx.node.addLearners(addingLearners, status -> {
            if (!status.isOk()) {
                done.run(status);
            } else {
                final CliRequests.LearnersOpResponse.Builder rb = CliRequests.LearnersOpResponse.newBuilder();

                for (final PeerId peer : oldLearners) {
                    rb.addOldLearners(peer.toString());
                    rb.addNewLearners(peer.toString());
                }

                for (final PeerId peer : addingLearners) {
                    if (!oldLearners.contains(peer)) {
                        rb.addNewLearners(peer.toString());
                    }
                }

                done.sendResponse(rb.build());
            }
        });

        return null;
    }

    @Override
    public String interest() {
        return CliRequests.AddLearnersRequest.class.getName();
    }

}
