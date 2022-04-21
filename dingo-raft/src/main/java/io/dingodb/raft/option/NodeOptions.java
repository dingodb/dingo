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
import io.dingodb.raft.storage.LogStorage;
import io.dingodb.raft.storage.SnapshotThrottle;
import io.dingodb.raft.util.Copiable;
import io.dingodb.raft.util.JRaftServiceLoader;
import io.dingodb.raft.util.Utils;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class NodeOptions extends RpcOptions implements Copiable<NodeOptions> {
    public static final JRaftServiceFactory defaultServiceFactory = JRaftServiceLoader.load(JRaftServiceFactory.class)
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
    private LogStorage logStorage;

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

    /**
     * Apply task in blocking or non-blocking mode, ApplyTaskMode.NonBlocking by default.
     */
    private ApplyTaskMode                   applyTaskMode          = ApplyTaskMode.NonBlocking;

    /**
     * Raft options.
     */
    private RaftOptions raftOptions = new RaftOptions();

    public void validate() {
        if (this.logStorage == null) {
            throw new IllegalArgumentException("Null logStorage");
        }
        if (StringUtils.isBlank(this.raftMetaUri)) {
            throw new IllegalArgumentException("Blank raftMetaUri");
        }
        if (this.fsm == null) {
            throw new IllegalArgumentException("Null stateMachine");
        }
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

    @Override
    public NodeOptions copy() {
        final NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setLogStorage(this.getLogStorage());
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
        nodeOptions.setServerExchangePort(this.serverExchangePort);
        nodeOptions.setUnfreezingSnapshotIntervalSecs(this.unfreezingSnapshotIntervalSecs);
        return nodeOptions;
    }
}
