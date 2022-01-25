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

import io.dingodb.raft.JRaftServiceFactory;
import io.dingodb.raft.StateMachine;
import io.dingodb.raft.conf.Configuration;
import io.dingodb.raft.util.JRaftServiceLoader;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class BootstrapOptions {
    public static final JRaftServiceFactory defaultServiceFactory = JRaftServiceLoader.load(JRaftServiceFactory.class) //
                                                                      .first();

    // Containing the initial member of this raft group
    // Default: empty conf
    private Configuration groupConf;

    // The index of the last index which the dumping snapshot contains
    // Default: 0
    private long lastLogIndex = 0L;

    // The specific StateMachine which is going to dump the first snapshot
    // If last_log_index isn't 0, fsm must be a valid instance.
    // Default: NULL
    private StateMachine fsm;

    // Describe a specific LogStorage in format ${type}://${parameters}
    private String logUri;

    // Describe a specific RaftMetaStorage in format ${type}://${parameters}
    private String raftMetaUri;

    // Describe a specific SnapshotStorage in format ${type}://${parameters}
    private String snapshotUri;

    // Whether to enable metrics for node.
    private boolean enableMetrics = false;

    /**
     * Custom service factory.
     */
    private JRaftServiceFactory serviceFactory = defaultServiceFactory;

    public JRaftServiceFactory getServiceFactory() {
        return serviceFactory;
    }

    public void setServiceFactory(JRaftServiceFactory serviceFactory) {
        this.serviceFactory = serviceFactory;
    }

    public void setEnableMetrics(boolean enableMetrics) {
        this.enableMetrics = enableMetrics;
    }

    public boolean isEnableMetrics() {
        return enableMetrics;
    }

    public Configuration getGroupConf() {
        return this.groupConf;
    }

    public void setGroupConf(Configuration groupConf) {
        this.groupConf = groupConf;
    }

    public long getLastLogIndex() {
        return this.lastLogIndex;
    }

    public void setLastLogIndex(long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    public StateMachine getFsm() {
        return this.fsm;
    }

    public void setFsm(StateMachine fsm) {
        this.fsm = fsm;
    }

    public String getLogUri() {
        return this.logUri;
    }

    public void setLogUri(String logUri) {
        this.logUri = logUri;
    }

    public String getRaftMetaUri() {
        return this.raftMetaUri;
    }

    public void setRaftMetaUri(String raftMetaUri) {
        this.raftMetaUri = raftMetaUri;
    }

    public String getSnapshotUri() {
        return this.snapshotUri;
    }

    public void setSnapshotUri(String snapshotUri) {
        this.snapshotUri = snapshotUri;
    }
}
