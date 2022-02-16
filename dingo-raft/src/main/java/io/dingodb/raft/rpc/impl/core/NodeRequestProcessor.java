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

import com.google.protobuf.Message;
import io.dingodb.raft.Node;
import io.dingodb.raft.NodeManager;
import io.dingodb.raft.entity.PeerId;
import io.dingodb.raft.error.RaftError;
import io.dingodb.raft.rpc.RaftServerService;
import io.dingodb.raft.rpc.RpcRequestClosure;
import io.dingodb.raft.rpc.RpcRequestProcessor;
import io.dingodb.raft.util.RpcFactoryHelper;

import java.util.concurrent.Executor;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public abstract class NodeRequestProcessor<T extends Message> extends RpcRequestProcessor<T> {
    public NodeRequestProcessor(Executor executor, Message defaultResp) {
        super(executor, defaultResp);
    }

    protected abstract Message processRequest0(final RaftServerService serviceService, final T request,
                                               final RpcRequestClosure done);

    protected abstract String getPeerId(final T request);

    protected abstract String getGroupId(final T request);

    @Override
    public Message processRequest(final T request, final RpcRequestClosure done) {
        final PeerId peer = new PeerId();
        final String peerIdStr = getPeerId(request);
        if (peer.parse(peerIdStr)) {
            final String groupId = getGroupId(request);
            final Node node = NodeManager.getInstance().get(groupId, peer);
            if (node != null) {
                return processRequest0((RaftServerService) node, request, done);
            } else {
                return RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(defaultResp(), RaftError.ENOENT, "Peer id not found: %s, group: %s", peerIdStr,
                        groupId);
            }
        } else {
            return RpcFactoryHelper //
                .responseFactory() //
                .newResponse(defaultResp(), RaftError.EINVAL, "Fail to parse peerId: %s", peerIdStr);
        }
    }
}
