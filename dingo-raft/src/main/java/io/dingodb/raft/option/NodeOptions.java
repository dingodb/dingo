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

import com.alipay.remoting.util.StringUtils;
import io.dingodb.raft.JRaftServiceFactory;
import io.dingodb.raft.StateMachine;
import io.dingodb.raft.conf.Configuration;
import io.dingodb.raft.core.ElectionPriority;
import io.dingodb.raft.storage.SnapshotThrottle;
import io.dingodb.raft.util.Copiable;
import io.dingodb.raft.util.JRaftServiceLoader;
import io.dingodb.raft.util.Utils;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class NodeOptions extends RpcOptions implements Copiable<NodeOptions> {
    public static final JRaftServiceFactory defaultServiceFactory = JRaftServiceLoader.load(JRaftServiceFactory.class) //
                                                                       .first();

    // A follower would become a candidate if it doesn't receive any message
    // from the leader in |election_timeout_ms| milliseconds
    // Default: 5000 (5s)
    private int electionTimeoutMs = 5000;                                         // follower to candidate timeout

    // One node's local priority value would be set to | electionPriority |
    // value when it starts up.If this value is set to 0,the node will never be a leader.
    // If this node doesn't support priority election,then set this value to -1.
    // Default: -1
    private int electionPriority = ElectionPriority.Disabled;

    // If next leader is not elected until next election timeout, it exponentially
    // decay its local target priority, for example target_priority = target_priority - gap
    // Default: 10
    private int decayPriorityGap = 10;

    // Leader lease time's ratio of electionTimeoutMs,
    // To minimize the effects of clock drift, we should make that:
    // clockDrift + leaderLeaseTimeoutMs < electionTimeout
    // Default: 90, Max: 100
    private int leaderLeaseTimeRatio = 90;

    // A snapshot saving would be triggered every |snapshot_interval_s| seconds
    // if this was reset as a positive number
    // If |snapshot_interval_s| <= 0, the time based snapshot would be disabled.
    //
    // Default: 3600 (1 hour)
    private int snapshotIntervalSecs = 3600;

    // A snapshot saving would be triggered every |snapshot_interval_s| seconds,
    // and at this moment when state machine's lastAppliedIndex value
    // minus lastSnapshotId value is greater than snapshotLogIndexMargin value,
    // the snapshot action will be done really.
    // If |snapshotLogIndexMargin| <= 0, the distance based snapshot would be disable.
    //
    // Default: 0
    private int snapshotLogIndexMargin = 0;

    // We will regard a adding peer as caught up if the margin between the
    // last_log_index of this peer and the last_log_index of leader is less than
    // |catchup_margin|
    //
    // Default: 1000
    private int catchupMargin = 1000;

    // If node is starting from a empty environment (both LogStorage and
    // SnapshotStorage are empty), it would use |initial_conf| as the
    // configuration of the group, otherwise it would load configuration from
    // the existing environment.
    //
    // Default: A empty group
    private Configuration initialConf = new Configuration();

    // The specific StateMachine implemented your business logic, which must be
    // a valid instance.
    private StateMachine fsm;

    // Describe a specific LogStorage in format ${type}://${parameters}
    private String logUri;

    // Describe a specific RaftMetaStorage in format ${type}://${parameters}
    private String raftMetaUri;

    // Describe a specific SnapshotStorage in format ${type}://${parameters}
    private String snapshotUri;

    // If enable, we will filter duplicate files before copy remote snapshot,
    // to avoid useless transmission. Two files in local and remote are duplicate,
    // only if they has the same filename and the same checksum (stored in file meta).
    // Default: false
    private boolean filterBeforeCopyRemote = false;

    // If non-null, we will pass this throughput_snapshot_throttle to SnapshotExecutor
    // Default: NULL
    //    scoped_refptr<SnapshotThrottle>* snapshot_throttle;

    // If true, RPCs through raft_cli will be denied.
    // Default: false
    private boolean disableCli = false;

    /**
     * Whether use global timer pool, if true, the {@code timerPoolSize} will be invalid.
     */
    private boolean sharedTimerPool = false;

    /**
     * storage options about raft on rocksdb.
     */
    private RaftLogStorageOptions raftLogStorageOptions  = new RaftLogStorageOptions();

    /**
     * Timer manager thread pool size.
     */
    private int timerPoolSize = Utils.cpus() * 3 > 20 ? 20 : Utils.cpus() * 3;

    /**
     * CLI service request RPC executor pool size, use default executor if -1.
     */
    private int cliRpcThreadPoolSize = Utils.cpus();

    /**
     * RAFT request RPC executor pool size, use default executor if -1.
     */
    private int raftRpcThreadPoolSize = Utils.cpus() * 6;

    /**
     * Whether to enable metrics for node.
     */
    private boolean enableMetrics = false;

    /**
     *  If non-null, we will pass this SnapshotThrottle to SnapshotExecutor.
     * Default: NULL.
     */
    private SnapshotThrottle snapshotThrottle;

    /**
     * Whether use global election timer.
     */
    private boolean sharedElectionTimer = false;

    /**
     * Whether use global vote timer.
     */
    private boolean sharedVoteTimer = false;

    /**
     * Whether use global step down timer.
     */
    private boolean sharedStepDownTimer = false;

    /**
     * Whether use global snapshot timer.
     */
    private boolean sharedSnapshotTimer = false;

    private int serverExchangePort = 19191;

    // Default: 15min
    private int unfreezingSnapshotIntervalSecs = 15 * 60;

    /**
     * Custom service factory.
     */
    private JRaftServiceFactory serviceFactory = defaultServiceFactory;

    public JRaftServiceFactory getServiceFactory() {
        return this.serviceFactory;
    }

    public void setServiceFactory(final JRaftServiceFactory serviceFactory) {
        this.serviceFactory = serviceFactory;
    }

    public SnapshotThrottle getSnapshotThrottle() {
        return this.snapshotThrottle;
    }

    public void setSnapshotThrottle(final SnapshotThrottle snapshotThrottle) {
        this.snapshotThrottle = snapshotThrottle;
    }

    public void setEnableMetrics(final boolean enableMetrics) {
        this.enableMetrics = enableMetrics;
    }

    /**
     * Raft options
     */
    private RaftOptions raftOptions = new RaftOptions();

    public int getCliRpcThreadPoolSize() {
        return this.cliRpcThreadPoolSize;
    }

    public void setCliRpcThreadPoolSize(final int cliRpcThreadPoolSize) {
        this.cliRpcThreadPoolSize = cliRpcThreadPoolSize;
    }

    public boolean isEnableMetrics() {
        return this.enableMetrics;
    }

    public int getRaftRpcThreadPoolSize() {
        return this.raftRpcThreadPoolSize;
    }

    public void setRaftRpcThreadPoolSize(final int raftRpcThreadPoolSize) {
        this.raftRpcThreadPoolSize = raftRpcThreadPoolSize;
    }

    public boolean isSharedTimerPool() {
        return sharedTimerPool;
    }

    public void setSharedTimerPool(boolean sharedTimerPool) {
        this.sharedTimerPool = sharedTimerPool;
    }

    public int getTimerPoolSize() {
        return this.timerPoolSize;
    }

    public void setTimerPoolSize(final int timerPoolSize) {
        this.timerPoolSize = timerPoolSize;
    }

    public RaftOptions getRaftOptions() {
        return this.raftOptions;
    }

    public void setRaftOptions(final RaftOptions raftOptions) {
        this.raftOptions = raftOptions;
    }

    public void validate() {
        if (StringUtils.isBlank(this.logUri)) {
            throw new IllegalArgumentException("Blank logUri");
        }
        if (StringUtils.isBlank(this.raftMetaUri)) {
            throw new IllegalArgumentException("Blank raftMetaUri");
        }
        if (this.fsm == null) {
            throw new IllegalArgumentException("Null stateMachine");
        }
    }

    public int getElectionPriority() {
        return electionPriority;
    }

    public void setElectionPriority(int electionPriority) {
        this.electionPriority = electionPriority;
    }

    public int getDecayPriorityGap() {
        return decayPriorityGap;
    }

    public void setDecayPriorityGap(int decayPriorityGap) {
        this.decayPriorityGap = decayPriorityGap;
    }

    public int getElectionTimeoutMs() {
        return this.electionTimeoutMs;
    }

    public void setElectionTimeoutMs(final int electionTimeoutMs) {
        this.electionTimeoutMs = electionTimeoutMs;
    }

    public int getLeaderLeaseTimeRatio() {
        return this.leaderLeaseTimeRatio;
    }

    public void setLeaderLeaseTimeRatio(final int leaderLeaseTimeRatio) {
        if (leaderLeaseTimeRatio <= 0 || leaderLeaseTimeRatio > 100) {
            throw new IllegalArgumentException("leaderLeaseTimeRatio: " + leaderLeaseTimeRatio
                                               + " (expected: 0 < leaderLeaseTimeRatio <= 100)");
        }
        this.leaderLeaseTimeRatio = leaderLeaseTimeRatio;
    }

    public int getLeaderLeaseTimeoutMs() {
        return this.electionTimeoutMs * this.leaderLeaseTimeRatio / 100;
    }

    public int getSnapshotIntervalSecs() {
        return this.snapshotIntervalSecs;
    }

    public void setSnapshotIntervalSecs(final int snapshotIntervalSecs) {
        this.snapshotIntervalSecs = snapshotIntervalSecs;
    }

    public int getSnapshotLogIndexMargin() {
        return snapshotLogIndexMargin;
    }

    public void setSnapshotLogIndexMargin(int snapshotLogIndexMargin) {
        this.snapshotLogIndexMargin = snapshotLogIndexMargin;
    }

    public int getCatchupMargin() {
        return this.catchupMargin;
    }

    public void setCatchupMargin(final int catchupMargin) {
        this.catchupMargin = catchupMargin;
    }

    public Configuration getInitialConf() {
        return this.initialConf;
    }

    public void setInitialConf(final Configuration initialConf) {
        this.initialConf = initialConf;
    }

    public StateMachine getFsm() {
        return this.fsm;
    }

    public void setFsm(final StateMachine fsm) {
        this.fsm = fsm;
    }

    public String getLogUri() {
        return this.logUri;
    }

    public void setLogUri(final String logUri) {
        this.logUri = logUri;
    }

    public String getRaftMetaUri() {
        return this.raftMetaUri;
    }

    public void setRaftMetaUri(final String raftMetaUri) {
        this.raftMetaUri = raftMetaUri;
    }

    public String getSnapshotUri() {
        return this.snapshotUri;
    }

    public void setSnapshotUri(final String snapshotUri) {
        this.snapshotUri = snapshotUri;
    }

    public boolean isFilterBeforeCopyRemote() {
        return this.filterBeforeCopyRemote;
    }

    public void setFilterBeforeCopyRemote(final boolean filterBeforeCopyRemote) {
        this.filterBeforeCopyRemote = filterBeforeCopyRemote;
    }

    public boolean isDisableCli() {
        return this.disableCli;
    }

    public void setDisableCli(final boolean disableCli) {
        this.disableCli = disableCli;
    }

    public boolean isSharedElectionTimer() {
        return sharedElectionTimer;
    }

    public void setSharedElectionTimer(boolean sharedElectionTimer) {
        this.sharedElectionTimer = sharedElectionTimer;
    }

    public boolean isSharedVoteTimer() {
        return sharedVoteTimer;
    }

    public void setSharedVoteTimer(boolean sharedVoteTimer) {
        this.sharedVoteTimer = sharedVoteTimer;
    }

    public boolean isSharedStepDownTimer() {
        return sharedStepDownTimer;
    }

    public void setSharedStepDownTimer(boolean sharedStepDownTimer) {
        this.sharedStepDownTimer = sharedStepDownTimer;
    }

    public boolean isSharedSnapshotTimer() {
        return sharedSnapshotTimer;
    }

    public void setSharedSnapshotTimer(boolean sharedSnapshotTimer) {
        this.sharedSnapshotTimer = sharedSnapshotTimer;
    }

    public RaftLogStorageOptions getRaftLogStorageOptions() {
        return raftLogStorageOptions;
    }

    public void setRaftLogStorageOptions(RaftLogStorageOptions raftLogStorageOptions) {
        this.raftLogStorageOptions = raftLogStorageOptions;
    }

    public void setServerExchangePort(int port) {
        this.serverExchangePort = port;
    }

    public int getServerExchangePort() {
        return this.serverExchangePort;
    }

    public void setUnfreezingSnapshotIntervalSecs(int unfreezingSnapshotIntervalSecs) {
        this.unfreezingSnapshotIntervalSecs = unfreezingSnapshotIntervalSecs;
    }

    public int getUnfreezingSnapshotIntervalSecs() {
        return this.unfreezingSnapshotIntervalSecs;
    }

    @Override
    public NodeOptions copy() {
        final NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setElectionTimeoutMs(this.electionTimeoutMs);
        nodeOptions.setElectionPriority(this.electionPriority);
        nodeOptions.setDecayPriorityGap(this.decayPriorityGap);
        nodeOptions.setSnapshotIntervalSecs(this.snapshotIntervalSecs);
        nodeOptions.setSnapshotLogIndexMargin(this.snapshotLogIndexMargin);
        nodeOptions.setCatchupMargin(this.catchupMargin);
        nodeOptions.setFilterBeforeCopyRemote(this.filterBeforeCopyRemote);
        nodeOptions.setDisableCli(this.disableCli);
        nodeOptions.setSharedTimerPool(this.sharedTimerPool);
        nodeOptions.setTimerPoolSize(this.timerPoolSize);
        nodeOptions.setCliRpcThreadPoolSize(this.cliRpcThreadPoolSize);
        nodeOptions.setRaftRpcThreadPoolSize(this.raftRpcThreadPoolSize);
        nodeOptions.setEnableMetrics(this.enableMetrics);
        nodeOptions.setRaftOptions(this.raftOptions == null ? new RaftOptions() : this.raftOptions.copy());
        nodeOptions.setSharedElectionTimer(this.sharedElectionTimer);
        nodeOptions.setSharedVoteTimer(this.sharedVoteTimer);
        nodeOptions.setSharedStepDownTimer(this.sharedStepDownTimer);
        nodeOptions.setSharedSnapshotTimer(this.sharedSnapshotTimer);
        nodeOptions.setRaftLogStorageOptions(this.raftLogStorageOptions);
        nodeOptions.setServerExchangePort(this.serverExchangePort);
        nodeOptions.setUnfreezingSnapshotIntervalSecs(this.unfreezingSnapshotIntervalSecs);
        return nodeOptions;
    }

    @Override
    public String toString() {
        return "NodeOptions{" + "electionTimeoutMs=" + electionTimeoutMs + ", electionPriority=" + electionPriority
               + ", decayPriorityGap=" + decayPriorityGap + ", leaderLeaseTimeRatio=" + leaderLeaseTimeRatio
               + ", snapshotIntervalSecs=" + snapshotIntervalSecs + ", snapshotLogIndexMargin="
               + snapshotLogIndexMargin + ", catchupMargin=" + catchupMargin + ", initialConf=" + initialConf
               + ", fsm=" + fsm + ", logUri='" + logUri + '\'' + ", raftMetaUri='" + raftMetaUri + '\''
               + ", snapshotUri='" + snapshotUri + '\'' + ", filterBeforeCopyRemote=" + filterBeforeCopyRemote
               + ", disableCli=" + disableCli + ", sharedTimerPool=" + sharedTimerPool + ", timerPoolSize="
               + timerPoolSize + ", cliRpcThreadPoolSize=" + cliRpcThreadPoolSize + ", raftRpcThreadPoolSize="
               + raftRpcThreadPoolSize + ", enableMetrics=" + enableMetrics + ", snapshotThrottle=" + snapshotThrottle
               + ", sharedElectionTimer=" + sharedElectionTimer + ", sharedVoteTimer=" + sharedVoteTimer
               + ", sharedStepDownTimer=" + sharedStepDownTimer + ", sharedSnapshotTimer=" + sharedSnapshotTimer
               + ", serviceFactory=" + serviceFactory + ", raftOptions=" + raftOptions
               + ", raftLogStorageOptions:" + this.raftLogStorageOptions
            + ", serverExchangePort:" + this.serverExchangePort
            + ", unfreezingSnapshotIntervalSecs:" + this.unfreezingSnapshotIntervalSecs
            + "} " + super.toString();
    }
}
