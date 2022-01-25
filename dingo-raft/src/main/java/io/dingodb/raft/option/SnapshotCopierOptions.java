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

package io.dingodb.raft.option;

import io.dingodb.raft.core.Scheduler;
import io.dingodb.raft.rpc.RaftClientService;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class SnapshotCopierOptions {
    private RaftClientService raftClientService;
    private Scheduler timerManager;
    private RaftOptions raftOptions;
    private NodeOptions nodeOptions;

    public SnapshotCopierOptions() {
        super();
    }

    public SnapshotCopierOptions(RaftClientService raftClientService, Scheduler timerManager, RaftOptions raftOptions,
                                 NodeOptions nodeOptions) {
        super();
        this.raftClientService = raftClientService;
        this.timerManager = timerManager;
        this.raftOptions = raftOptions;
        this.nodeOptions = nodeOptions;
    }

    public NodeOptions getNodeOptions() {
        return this.nodeOptions;
    }

    public void setNodeOptions(NodeOptions nodeOptions) {
        this.nodeOptions = nodeOptions;
    }

    public RaftClientService getRaftClientService() {
        return this.raftClientService;
    }

    public void setRaftClientService(RaftClientService raftClientService) {
        this.raftClientService = raftClientService;
    }

    public Scheduler getTimerManager() {
        return this.timerManager;
    }

    public void setTimerManager(Scheduler timerManager) {
        this.timerManager = timerManager;
    }

    public RaftOptions getRaftOptions() {
        return this.raftOptions;
    }

    public void setRaftOptions(RaftOptions raftOptions) {
        this.raftOptions = raftOptions;
    }
}
