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

package io.dingodb.store.raft;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.store.Part;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.raft.Closure;
import io.dingodb.raft.Node;
import io.dingodb.raft.Status;
import io.dingodb.raft.entity.LocalFileMetaOutter;
import io.dingodb.raft.entity.PeerId;
import io.dingodb.raft.kv.storage.ByteArrayEntry;
import io.dingodb.raft.kv.storage.DefaultRaftRawKVStoreStateMachine;
import io.dingodb.raft.kv.storage.RaftRawKVOperation;
import io.dingodb.raft.kv.storage.RaftRawKVStore;
import io.dingodb.raft.kv.storage.SeekableIterator;
import io.dingodb.raft.storage.snapshot.SnapshotReader;
import io.dingodb.raft.storage.snapshot.SnapshotWriter;
import io.dingodb.raft.util.NamedThreadFactory;
import io.dingodb.raft.util.timer.HashedWheelTimer;
import io.dingodb.raft.util.timer.Timeout;
import io.dingodb.server.api.ReportApi;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import io.dingodb.server.protocol.meta.TablePartStats;
import io.dingodb.server.protocol.meta.TablePartStats.ApproximateStats;
import io.dingodb.store.raft.config.StoreConfiguration;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.dingodb.raft.kv.Constants.SNAPSHOT_ZIP;
import static io.dingodb.raft.kv.storage.RaftRawKVOperation.Op.SNAPSHOT_LOAD;
import static io.dingodb.raft.kv.storage.RaftRawKVOperation.Op.SNAPSHOT_SAVE;
import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.STATS_IDENTIFIER;

@Slf4j
public class PartStateMachine extends DefaultRaftRawKVStoreStateMachine {

    public static final String TIMER_THREAD_NAME = "ServiceStats-timer";

    public final Integer approximateCount = StoreConfiguration.approximateCount();
    private final CommonId id;

    private final Node node;

    private Part part;
    private HashedWheelTimer timer;
    private ReportApi reportApi;
    private long lastTime;

    private volatile boolean available = false;
    private volatile boolean enable = true;
    private List<Runnable> availableListener = new CopyOnWriteArrayList<>();

    public PartStateMachine(CommonId id, RaftRawKVStore store, Part part) {
        super(id.toString(), store);
        this.id = id;
        this.node = store.getNode();
        this.part = part;
    }

    public boolean isAvailable() {
        return available;
    }

    public boolean isEnable() {
        return enable;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
    }

    public void listenAvailable(Runnable listener) {
        try {
            if (available) {
                listener.run();
            }
        } catch (Exception e) {
            log.error("Run available listener error.", e);
        }
        this.availableListener.add(listener);
    }

    public void resetPart(Part part) {
        this.part = part;
        if (node.isLeader()) {
            Map<Location, PeerId> peers = node.listPeers().stream()
                .collect(Collectors.toMap(PeerId::toLocation, Function.identity()));
            if (part.getLeader() != null &&
                (!part.getLeader().getHost().equals(DingoConfiguration.host())
                    || part.getLeader().getRaftPort() != DingoConfiguration.raftPort())) {
                log.info("Transfer leader to [{}].", part.getLeader().getUrl());
                node.transferLeadershipTo(PeerId.of(part.getLeader()));
                return;
            }
            if (part.getReplicates().size() != node.listPeers().size()) {
                for (Location replicateLoc : part.getReplicates()) {
                    if (!raftLocationContains(peers.keySet(), replicateLoc)) {
                        addReplica(PeerId.of(replicateLoc));
                    }
                }
                for (Location peerLoc : peers.keySet()) {
                    if (!raftLocationContains(part.getReplicates(), peerLoc)) {
                        removeReplica(peers.get(peerLoc));
                    }
                }
            }
        }
    }

    public void addReplica(PeerId peerId) {
        log.info("Add peer [{}] to [{}].", peerId, id);
        Executors.submit("add-peer", () -> node.addPeer(
            peerId,
            status -> log.info("Add peer [{}] to [{}] -> [{}].", peerId, id, status)
        ));
    }

    public void removeReplica(PeerId peerId) {
        log.info("Remove peer [{}] from [{}].", peerId, id);
        Executors.submit("remove-peer", () -> node.removePeer(
            peerId,
            status -> log.info("remove peer [{}] to [{}] -> [{}].", peerId, id, status)
        ));
    }

    @Override
    protected void onApplyOperation(RaftRawKVOperation operation) {
        switch (operation.getOp()) {
            case PUT:
                break;
            case PUT_LIST:
                break;
            case DELETE:
                break;
            case DELETE_LIST:
                break;
            case DELETE_RANGE:
                break;
            default:
        }
    }

    @Override
    public RaftRawKVOperation snapshotSaveOperation(SnapshotWriter writer, Closure done) {
        return RaftRawKVOperation.builder()
            .key(part.getStart())
            .extKey(part.getEnd())
            .ext1(writer.getPath())
            .op(SNAPSHOT_SAVE)
            .build();
    }

    @Override
    public RaftRawKVOperation snapshotLoadOperation(final SnapshotReader reader) {
        return RaftRawKVOperation.builder()
            .key(part.getStart())
            .extKey(part.getEnd())
            .ext1(reader.getPath())
            .ext2(((LocalFileMetaOutter.LocalFileMeta) reader.getFileMeta(SNAPSHOT_ZIP)).getChecksum())
            .op(SNAPSHOT_LOAD)
            .build();
    }

    @Override
    public void onLeaderStart(long term) {
        super.onLeaderStart(term);
        if (StoreConfiguration.collectStatsInterval() < 0) {
            available = true;
            availableListener.forEach(listen -> Executors.submit(id + " available", listen));
            return;
        }
        if (this.timer != null) {
            this.timer.stop();
            this.timer = null;
        }

        this.timer = new HashedWheelTimer(
            new NamedThreadFactory(TIMER_THREAD_NAME, true),
            50,
            TimeUnit.MILLISECONDS,
            4096
        );

        this.reportApi = ServiceLoader.load(NetServiceProvider.class).iterator().next().get().apiRegistry()
            .proxy(ReportApi.class, CoordinatorConnector.defaultConnector());
        this.timer.start();
        this.timer.newTimeout(this::sendStats, 1, TimeUnit.SECONDS);
    }

    @Override
    public void onLeaderStop(Status status) {
        if (this.timer != null) {
            this.timer.stop();
            this.timer = null;
        }
        available = false;
        availableListener.forEach(listen -> Executors.submit(id + " available", listen));
    }

    //todo refactor send stats ?
    private void sendStats(Timeout timeout) throws Exception {
        try {
            SeekableIterator<byte[], ByteArrayEntry> iterator = store.scan(part.getStart(), part.getEnd()).join();
            List<ApproximateStats> approximateStats = new ArrayList<>();
            long count = 0;
            long size = 0;
            byte[] startKey = null;
            byte[] endKey = null;
            while (iterator.hasNext()) {
                count++;
                ByteArrayEntry entry = iterator.next();
                size += entry.getKey().length;
                size += entry.getValue().length;
                if (startKey == null) {
                    startKey = entry.getKey();
                }
                endKey = entry.getKey();
                if (count >= approximateCount) {
                    approximateStats.add(new ApproximateStats(startKey, entry.getKey(), count, size));
                    count = 0;
                    size = 0;
                    startKey = null;
                }
            }
            if (count > 0) {
                approximateStats.add(new ApproximateStats(startKey, endKey, count, size));
            }
            TablePartStats stats = TablePartStats.builder()
                .id(new CommonId(ID_TYPE.stats, STATS_IDENTIFIER.part, id.domain(), id.seqContent()))
                .leader(DingoConfiguration.instance().getServerId())
                .tablePart(id)
                .table(part.getInstanceId())
                .approximateStats(approximateStats)
                .time(System.currentTimeMillis())
                .alive(node.listAlivePeers().stream().map(PeerId::toLocation).collect(Collectors.toList()))
                .build();
            if (available != (available = reportApi.report(stats))) {
                availableListener.forEach(listen -> Executors.submit(id + " available", listen));
            }
        } catch (Exception e) {
            log.error("Report stats error, id: {}", id, e);
        } finally {
            if (node.isLeader()) {
                this.timer.newTimeout(this::sendStats, StoreConfiguration.collectStatsInterval(), TimeUnit.SECONDS);
            }
        }

    }

    private boolean raftLocationContains(Collection<Location> list, Location location) {
        for (Location loc : list) {
            if (loc.getHost().equals(location.getHost()) && loc.getRaftPort() == location.getRaftPort()) {
                return true;
            }
        }
        return false;
    }

}
