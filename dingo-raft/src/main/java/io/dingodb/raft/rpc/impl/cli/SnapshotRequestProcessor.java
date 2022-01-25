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
import io.dingodb.raft.rpc.CliRequests;
import io.dingodb.raft.rpc.RpcRequestClosure;
import io.dingodb.raft.rpc.RpcRequests;

import java.util.concurrent.Executor;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class SnapshotRequestProcessor extends BaseCliRequestProcessor<CliRequests.SnapshotRequest> {
    public SnapshotRequestProcessor(Executor executor) {
        super(executor, RpcRequests.ErrorResponse.getDefaultInstance());
    }

    @Override
    protected String getPeerId(final CliRequests.SnapshotRequest request) {
        return request.getPeerId();
    }

    @Override
    protected String getGroupId(final CliRequests.SnapshotRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final CliRequests.SnapshotRequest request,
                                      final RpcRequestClosure done) {
        LOG.info("Receive SnapshotRequest to {} from {}", ctx.node.getNodeId(), request.getPeerId());
        ctx.node.snapshot(done);
        return null;
    }

    @Override
    public String interest() {
        return CliRequests.SnapshotRequest.class.getName();
    }

}
