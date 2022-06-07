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

import io.dingodb.raft.ReplicatorGroup;
import io.dingodb.raft.Status;
import io.dingodb.raft.closure.CatchUpClosure;
import io.dingodb.raft.conf.ConfigurationEntry;
import io.dingodb.raft.entity.NodeId;
import io.dingodb.raft.entity.PeerId;
import io.dingodb.raft.error.RaftError;
import io.dingodb.raft.option.RaftOptions;
import io.dingodb.raft.option.ReplicatorGroupOptions;
import io.dingodb.raft.option.ReplicatorOptions;
import io.dingodb.raft.rpc.RaftClientService;
import io.dingodb.raft.rpc.RpcRequests;
import io.dingodb.raft.rpc.RpcResponseClosure;
import io.dingodb.raft.util.Describer;
import io.dingodb.raft.util.OnlyForTest;
import io.dingodb.raft.util.Requires;
import io.dingodb.raft.util.ThreadId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class ReplicatorGroupImpl implements ReplicatorGroup {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicatorGroupImpl.class);

    // <peerId, replicatorId>
    private final ConcurrentMap<PeerId, ThreadId> replicatorMap = new ConcurrentHashMap<>();
    /** common replicator options */
    private ReplicatorOptions commonOptions;
    private int dynamicTimeoutMs = -1;
    private int electionTimeoutMs  = -1;
    private RaftOptions raftOptions;
    private final Map<PeerId, ReplicatorType> failureReplicators = new ConcurrentHashMap<>();

    @Override
    public boolean init(final NodeId nodeId, final ReplicatorGroupOptions opts) {
        this.dynamicTimeoutMs = opts.getHeartbeatTimeoutMs();
        this.electionTimeoutMs = opts.getElectionTimeoutMs();
        this.raftOptions = opts.getRaftOptions();
        this.commonOptions = new ReplicatorOptions();
        this.commonOptions.setDynamicHeartBeatTimeoutMs(this.dynamicTimeoutMs);
        this.commonOptions.setElectionTimeoutMs(this.electionTimeoutMs);
        this.commonOptions.setRaftRpcService(opts.getRaftRpcClientService());
        this.commonOptions.setLogManager(opts.getLogManager());
        this.commonOptions.setBallotBox(opts.getBallotBox());
        this.commonOptions.setNode(opts.getNode());
        this.commonOptions.setTerm(0);
        this.commonOptions.setGroupId(nodeId.getGroupId());
        this.commonOptions.setServerId(nodeId.getPeerId());
        this.commonOptions.setSnapshotStorage(opts.getSnapshotStorage());
        this.commonOptions.setTimerManager(opts.getTimerManager());
        return true;
    }

    @OnlyForTest
    ConcurrentMap<PeerId, ThreadId> getReplicatorMap() {
        return this.replicatorMap;
    }

    @OnlyForTest
    Map<PeerId, ReplicatorType> getFailureReplicators() {
        return this.failureReplicators;
    }

    @Override
    public void sendHeartbeat(final PeerId peer, final RpcResponseClosure<RpcRequests.AppendEntriesResponse> closure) {
        final ThreadId rid = this.replicatorMap.get(peer);
        if (rid == null) {
            LOG.debug("Peer {} is not connected", peer);
            if (closure != null) {
                closure.run(new Status(RaftError.EHOSTDOWN, "Peer %s is not connected", peer));
            }
            return;
        }
        Replicator.sendHeartbeat(rid, closure);
    }

    @Override
    public ThreadId getReplicator(final PeerId peer) {
        return this.replicatorMap.get(peer);
    }

    @Override
    public boolean addReplicator(final PeerId peer, final ReplicatorType replicatorType, final boolean sync) {
        Requires.requireTrue(this.commonOptions.getTerm() != 0);
        this.failureReplicators.remove(peer);
        if (this.replicatorMap.containsKey(peer)) {
            return true;
        }
        final ReplicatorOptions opts = this.commonOptions == null ? new ReplicatorOptions() : this.commonOptions.copy();
        opts.setReplicatorType(replicatorType);
        opts.setPeerId(peer);
        if (!sync) {
            final RaftClientService client = opts.getRaftRpcService();
            if (client != null && !client.checkConnection(peer.getEndpoint(), true)) {
                LOG.error("Fail to check replicator connection to peer={}, replicatorType={}.", peer, replicatorType);
                this.failureReplicators.put(peer, replicatorType);
                return false;
            }
        }
        final ThreadId rid = Replicator.start(opts, this.raftOptions);
        if (rid == null) {
            LOG.error("Fail to start replicator to peer={}, replicatorType={}.", peer, replicatorType);
            this.failureReplicators.put(peer, replicatorType);
            return false;
        }
        return this.replicatorMap.put(peer, rid) == null;
    }

    @Override
    public void clearFailureReplicators() {
        this.failureReplicators.clear();
    }

    @Override
    public boolean waitCaughtUp(final PeerId peer, final long maxMargin, final long dueTime, final CatchUpClosure done) {
        final ThreadId rid = this.replicatorMap.get(peer);
        if (rid == null) {
            return false;
        }

        Replicator.waitForCaughtUp(rid, maxMargin, dueTime, done);
        return true;
    }

    @Override
    public long getLastRpcSendTimestamp(final PeerId peer) {
        final ThreadId rid = this.replicatorMap.get(peer);
        if (rid == null) {
            return 0L;
        }
        return Replicator.getLastRpcSendTimestamp(rid);
    }

    @Override
    public boolean stopAll() {
        final List<ThreadId> rids = new ArrayList<>(this.replicatorMap.values());
        this.replicatorMap.clear();
        this.failureReplicators.clear();
        for (final ThreadId rid : rids) {
            Replicator.stop(rid);
        }
        return true;
    }

    @Override
    public void checkReplicator(final PeerId peer, final boolean lockNode) {
        final ThreadId rid = this.replicatorMap.get(peer);
        if (rid == null) {
            // Create replicator if it's not found for leader.
            final NodeImpl node = this.commonOptions.getNode();
            if (lockNode) {
                node.writeLock.lock();
            }
            try {
                if (node.isLeader()) {
                    final ReplicatorType rType = this.failureReplicators.get(peer);
                    if (rType != null && addReplicator(peer, rType, false)) {
                        this.failureReplicators.remove(peer, rType);
                    }
                }
            } finally {
                if (lockNode) {
                    node.writeLock.unlock();
                }
            }
        }
    }

    @Override
    public boolean stopReplicator(final PeerId peer) {
        LOG.info("Stop replicator to {}.", peer);
        this.failureReplicators.remove(peer);
        final ThreadId rid = this.replicatorMap.remove(peer);
        if (rid == null) {
            return false;
        }
        // Calling ReplicatorId.stop might lead to calling stopReplicator again,
        // erase entry first to avoid race condition
        return Replicator.stop(rid);
    }

    @Override
    public boolean resetTerm(final long newTerm) {
        if (newTerm <= this.commonOptions.getTerm()) {
            return false;
        }
        this.commonOptions.setTerm(newTerm);
        return true;
    }

    @Override
    public boolean resetHeartbeatInterval(final int newIntervalMs) {
        this.dynamicTimeoutMs = newIntervalMs;
        return true;
    }

    @Override
    public boolean resetElectionTimeoutInterval(final int newIntervalMs) {
        this.electionTimeoutMs = newIntervalMs;
        return true;
    }

    @Override
    public boolean contains(final PeerId peer) {
        return this.replicatorMap.containsKey(peer);
    }

    @Override
    public boolean transferLeadershipTo(final PeerId peer, final long logIndex) {
        final ThreadId rid = this.replicatorMap.get(peer);
        return rid != null && Replicator.transferLeadership(rid, logIndex);
    }

    @Override
    public boolean stopTransferLeadership(final PeerId peer) {
        final ThreadId rid = this.replicatorMap.get(peer);
        return rid != null && Replicator.stopTransferLeadership(rid);
    }

    @Override
    public ThreadId stopAllAndFindTheNextCandidate(final ConfigurationEntry conf) {
        ThreadId candidate = null;
        final PeerId candidateId = findTheNextCandidate(conf);
        if (candidateId != null) {
            candidate = this.replicatorMap.get(candidateId);
        } else {
            LOG.info("Fail to find the next candidate.");
        }
        for (final ThreadId r : this.replicatorMap.values()) {
            if (r != candidate) {
                Replicator.stop(r);
            }
        }
        this.replicatorMap.clear();
        this.failureReplicators.clear();
        return candidate;
    }

    @Override
    public PeerId findTheNextCandidate(final ConfigurationEntry conf) {
        PeerId peerId = null;
        int priority = Integer.MIN_VALUE;
        long maxIndex = -1L;
        for (final Map.Entry<PeerId, ThreadId> entry : this.replicatorMap.entrySet()) {
            if (!conf.contains(entry.getKey())) {
                continue;
            }
            final int nextPriority = entry.getKey().getPriority();
            if (nextPriority == ElectionPriority.NotElected) {
                continue;
            }
            final long nextIndex = Replicator.getNextIndex(entry.getValue());
            if (nextIndex > maxIndex) {
                maxIndex = nextIndex;
                peerId = entry.getKey();
                priority = peerId.getPriority();
            } else if (nextIndex == maxIndex && nextPriority > priority) {
                peerId = entry.getKey();
                priority = peerId.getPriority();
            }
        }

        if (maxIndex == -1L) {
            return null;
        } else {
            return peerId;
        }
    }

    @Override
    public List<ThreadId> listReplicators() {
        return new ArrayList<>(this.replicatorMap.values());
    }

    @Override
    public void describe(final Describer.Printer out) {
        out.print("  replicators: ") //
            .println(this.replicatorMap.values());
        out.print("  failureReplicators: ") //
            .println(this.failureReplicators);
    }

    @Override
    public void failReplicator(PeerId peerId) {
        ThreadId id = this.replicatorMap.remove(peerId);
        if (id != null) {
            this.failureReplicators.put(peerId, ((Replicator) id.getData()).getOpts().getReplicatorType());
        };
    }
}
