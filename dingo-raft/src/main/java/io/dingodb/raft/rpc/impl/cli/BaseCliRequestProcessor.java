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
import io.dingodb.raft.rpc.RpcRequestClosure;
import io.dingodb.raft.rpc.RpcRequestProcessor;
import io.dingodb.raft.entity.PeerId;
import io.dingodb.raft.error.RaftError;
import io.dingodb.raft.util.RpcFactoryHelper;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executor;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public abstract class BaseCliRequestProcessor<T extends Message> extends RpcRequestProcessor<T> {
    protected static final Logger LOG = LoggerFactory.getLogger(BaseCliRequestProcessor.class);

    public BaseCliRequestProcessor(Executor executor, Message defaultResp) {
        super(executor, defaultResp);
    }

    /**
     * Returns the peerId that will be find in node manager.
     */
    protected abstract String getPeerId(T request);

    /**
     * Returns the raft group id
     */
    protected abstract String getGroupId(T request);

    /**
     * Process the request with CliRequestContext
     */
    protected abstract Message processRequest0(CliRequestContext ctx, T request, RpcRequestClosure done);

    /**
     * Cli request context
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-09 11:55:45 AM
     */
    public static class CliRequestContext {

        /**
         * The found node.
         */
        public final Node node;
        /**
         * The peerId in returns by {@link BaseCliRequestProcessor#getPeerId(Message)}, null if absent.
         */
        public final PeerId peerId;
        /**
         * The groupId in request.
         */
        public final String groupId;

        public CliRequestContext(Node ndoe, String groupId, PeerId peerId) {
            super();
            this.node = ndoe;
            this.peerId = peerId;
            this.groupId = groupId;
        }

    }

    @Override
    public Message processRequest(T request, RpcRequestClosure done) {

        String groupId = getGroupId(request);
        String peerIdStr = getPeerId(request);
        PeerId peerId = null;

        if (!StringUtils.isBlank(peerIdStr)) {
            peerId = new PeerId();
            if (!peerId.parse(peerIdStr)) {
                return RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(defaultResp(), RaftError.EINVAL, "Fail to parse peer: %s", peerIdStr);
            }
        }

        Status st = new Status();
        Node node = getNode(groupId, peerId, st);
        if (!st.isOk()) {
            return RpcFactoryHelper //
                .responseFactory() //
                .newResponse(defaultResp(), st.getCode(), st.getErrorMsg());
        } else {
            return processRequest0(new CliRequestContext(node, groupId, peerId), request, done);
        }
    }

    protected Node getNode(String groupId, PeerId peerId, Status st) {
        Node node = null;

        if (peerId != null) {
            node = NodeManager.getInstance().get(groupId, peerId);
            if (node == null) {
                st.setError(RaftError.ENOENT, "Fail to find node %s in group %s", peerId, groupId);
            }
        } else {
            List<Node> nodes = NodeManager.getInstance().getNodesByGroupId(groupId);
            if (nodes == null || nodes.isEmpty()) {
                st.setError(RaftError.ENOENT, "Empty nodes in group %s", groupId);
            } else if (nodes.size() > 1) {
                st.setError(RaftError.EINVAL, "Peer must be specified since there're %d nodes in group %s",
                    nodes.size(), groupId);
            } else {
                node = nodes.get(0);
            }

        }
        if (node != null && node.getOptions().isDisableCli()) {
            st.setError(RaftError.EACCES, "Cli service is not allowed to access node %s", node.getNodeId());
        }
        return node;
    }
}
