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

package io.dingodb.raft.core;

import io.dingodb.raft.Node;
import io.dingodb.raft.Status;
import io.dingodb.raft.entity.PeerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DingoReplicatorStateListenerImpl implements Replicator.ReplicatorStateListener {

    private static final Logger LOG = LoggerFactory.getLogger(DingoReplicatorStateListenerImpl.class);

    private Node node;

    public DingoReplicatorStateListenerImpl(Node node) {
        this.node = node;
    }

    @Override
    public void onCreated(PeerId peer) {

    }

    @Override
    public void onError(PeerId peer, Status status) {
        LOG.info("OnError : {}, {}", peer, status);
        switch (status.getRaftError()) {
            case ETIMEDOUT:
                node.restartReplicator(peer);
                break;
            case ENOENT:
                node.restartReplicator(peer);
                break;
            default:
        }

    }

    @Override
    public void onDestroyed(PeerId peer) {

    }
}
