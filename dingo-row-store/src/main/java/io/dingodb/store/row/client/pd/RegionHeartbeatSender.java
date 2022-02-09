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

package io.dingodb.store.row.client.pd;

import com.codahale.metrics.Counter;
import io.dingodb.raft.Lifecycle;
import io.dingodb.raft.util.Endpoint;
import io.dingodb.raft.util.NamedThreadFactory;
import io.dingodb.raft.util.timer.HashedWheelTimer;
import io.dingodb.raft.util.timer.Timeout;
import io.dingodb.raft.util.timer.TimerTask;
import io.dingodb.store.row.ApproximateKVStats;
import io.dingodb.store.row.RegionEngine;
import io.dingodb.store.row.client.failover.FailoverClosure;
import io.dingodb.store.row.client.failover.impl.FailoverClosureImpl;
import io.dingodb.store.row.cmd.pd.BaseRequest;
import io.dingodb.store.row.cmd.pd.RegionHeartbeatRequest;
import io.dingodb.store.row.errors.Errors;
import io.dingodb.store.row.metadata.Instruction;
import io.dingodb.store.row.metadata.Peer;
import io.dingodb.store.row.metadata.Region;
import io.dingodb.store.row.metadata.RegionStats;
import io.dingodb.store.row.metadata.TimeInterval;
import io.dingodb.store.row.metrics.KVMetricNames;
import io.dingodb.store.row.metrics.KVMetrics;
import io.dingodb.store.row.options.HeartbeatOptions;
import io.dingodb.store.row.storage.BaseRawKVStore;
import io.dingodb.store.row.util.StackTraceUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RegionHeartbeatSender implements Lifecycle<HeartbeatOptions> {

    public static final String TIMER_THREAD_NAME = "heartbeat-timer";

    private final Region region;
    private final RegionEngine regionEngine;
    private final PlacementDriverClient pdClient;
    private final String storeId;
    private final Endpoint selfEndpoint;

    private long interval;

    private InstructionProcessor instructionProcessor;
    private HashedWheelTimer heartbeatTimer;

    private boolean started;

    public RegionHeartbeatSender(RegionEngine regionEngine) {
        this.regionEngine = regionEngine;
        this.region = regionEngine.getRegion();
        this.storeId = regionEngine.getStoreEngine().getStoreId();
        this.selfEndpoint = regionEngine.getStoreEngine().getSelfEndpoint();
        this.pdClient = regionEngine.getStoreEngine().getPlacementDriverClient();
    }

    @Override
    public synchronized boolean init(final HeartbeatOptions opts) {
        if (this.started) {
            log.info("[RegionHeartbeatSender] already started.");
            return true;
        }
        this.instructionProcessor = new InstructionProcessor(this.regionEngine.getStoreEngine());
        this.interval = opts.getRegionHeartbeatIntervalSeconds();
        if (interval <= 0) {
            throw new IllegalArgumentException("Region heartbeat interval seconds must > 0, " + interval);
        }

        log.info("[RegionHeartbeatSender] init successfully, options: {}.", opts);
        return this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        if (this.heartbeatTimer != null) {
            this.heartbeatTimer.stop();
        }
    }

    public synchronized void start() {
        if (heartbeatTimer == null) {
            this.heartbeatTimer = new HashedWheelTimer(
                new NamedThreadFactory(TIMER_THREAD_NAME, true),
                50,
                TimeUnit.MILLISECONDS,
                4096
            );
        }
        this.heartbeatTimer.start();
        final RegionHeartbeatTask regionHeartbeatTask = new RegionHeartbeatTask(-1);
        this.heartbeatTimer.newTimeout(regionHeartbeatTask, 0, TimeUnit.SECONDS);
    }

    private void sendRegionHeartbeat(
        final long lastTime
    ) throws Exception {
        final long now = System.currentTimeMillis();

        final RegionHeartbeatRequest request = new RegionHeartbeatRequest();
        request.setClusterId(this.regionEngine.getStoreEngine().getClusterId());
        request.setLeastKeysOnSplit(this.regionEngine.getStoreEngine().getStoreOpts().getLeastKeysOnSplit());
        request.setRegion(region);
        request.setRegionStats(collectRegionStats(new TimeInterval(lastTime, now)));
        request.setSelfEndpoint(selfEndpoint);

        CompletableFuture<List<Instruction>> future = new CompletableFuture<>();
        callAsyncWithRpc(future, request, 3, null);
        future.whenCompleteAsync(this::processHeartbeatResponse);
        future.thenRunAsync(() -> heartbeatTimer.newTimeout(new RegionHeartbeatTask(now), interval, TimeUnit.SECONDS));
    }

    private void processHeartbeatResponse(List<Instruction> instructions, Throwable err) {
        if (err != null) {
            log.error("Send region heartbeat error, id: [{}]", region.getId(), err);
            return;
        }
        if (instructions != null && !instructions.isEmpty()) {
            instructionProcessor.process(instructions);
        }
    }

    private void callAsyncWithRpc(
        final CompletableFuture<?> future,
        final BaseRequest request,
        final int retriesLeft,
        final Errors lastErr
    ) {
        FailoverClosure<?> closure = new FailoverClosureImpl<>(
            future,
            retriesLeft,
            err -> {
                pdClient.getPdLeader(true, 3000);
                callAsyncWithRpc(future, request, retriesLeft - 1, err);
            }
        );

        pdClient.getPdRpcService().callPdServerWithRpc(request, closure, lastErr);
    }

    private final class RegionHeartbeatTask implements TimerTask {

        private final long    lastTime;

        private RegionHeartbeatTask(long lastTime) {
            this.lastTime = lastTime;
        }

        @Override
        public void run(final Timeout timeout) throws Exception {
            try {
                sendRegionHeartbeat(this.lastTime);
            } catch (final Throwable t) {
                log.error("Caught a error on sending [RegionHeartbeat]: {}.", StackTraceUtil.stackTrace(t));
            }
        }

    }

    public RegionStats collectRegionStats(final TimeInterval timeInterval) {
        final RegionStats stats = new RegionStats();
        stats.setRegionId(region.getId());
        // Leader Peer sending the heartbeat
        stats.setLeader(new Peer(region.getId(), this.storeId, this.regionEngine.getStoreEngine().getSelfEndpoint()));
        // Leader considers that these peers are down
        // TODO
        // Pending peers are the peers that the leader can't consider as working followers
        // TODO
        // Bytes written for the region during this period
        stats.setBytesWritten(bytesWritten());
        // Bytes read for the region during this period
        stats.setBytesRead(bytesRead());
        // Keys written for the region during this period
        stats.setKeysWritten(keysWritten());
        // Keys read for the region during this period
        stats.setKeysRead(keysRead());
        // Approximate region size
        // TODO very important
        // Approximate number of keys
        ApproximateKVStats rangeStats = store().getApproximateKVStatsInRange(
            region.getStartKey(),
            region.getEndKey()
        );
        stats.setApproximateKeys(rangeStats.getKeysCnt());
        stats.setApproximateSize(rangeStats.getSizeInBytes());
        // Actually reported time interval
        stats.setInterval(timeInterval);
        if (log.isDebugEnabled()) {
            log.info("Collect [RegionStats]: {}.", stats);
        }
        return stats;
    }

    private BaseRawKVStore<?> store() {
        return regionEngine.getStoreEngine().getRawKVStore();
    }

    private long counterMetrics(String name) {
        final Counter counter = KVMetrics.counter(name, String.valueOf(region.getId()));
        final long value = counter.getCount();
        counter.dec(value);
        return value;
    }

    private long bytesWritten() {
        return counterMetrics(KVMetricNames.REGION_BYTES_WRITTEN);
    }

    private long bytesRead() {
        return counterMetrics(KVMetricNames.REGION_BYTES_READ);
    }

    private long keysWritten() {
        return counterMetrics(KVMetricNames.REGION_KEYS_WRITTEN);
    }

    private long keysRead() {
        return counterMetrics(KVMetricNames.REGION_KEYS_READ);
    }
}
