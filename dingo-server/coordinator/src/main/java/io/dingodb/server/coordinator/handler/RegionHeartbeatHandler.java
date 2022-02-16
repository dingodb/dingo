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

package io.dingodb.server.coordinator.handler;

import io.dingodb.common.util.Optional;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.MessageListener;
import io.dingodb.raft.rpc.RpcContext;
import io.dingodb.raft.rpc.RpcProcessor;
import io.dingodb.raft.util.Endpoint;
import io.dingodb.server.coordinator.GeneralId;
import io.dingodb.server.coordinator.config.CoordinatorConfiguration;
import io.dingodb.server.coordinator.meta.ClusterStatsManager;
import io.dingodb.server.coordinator.meta.RowStoreMetaAdaptor;
import io.dingodb.store.row.cmd.pd.RegionHeartbeatRequest;
import io.dingodb.store.row.cmd.pd.RegionHeartbeatResponse;
import io.dingodb.store.row.errors.Errors;
import io.dingodb.store.row.metadata.Instruction;
import io.dingodb.store.row.metadata.Peer;
import io.dingodb.store.row.metadata.Region;
import io.dingodb.store.row.metadata.RegionStats;
import io.dingodb.store.row.metadata.StoreStats;
import io.dingodb.store.row.util.Lists;
import io.dingodb.store.row.util.Pair;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class RegionHeartbeatHandler implements MessageListener, RpcProcessor<RegionHeartbeatRequest> {

    public static final Class<RegionHeartbeatRequest> REQ_CLASS = RegionHeartbeatRequest.class;

    private final RowStoreMetaAdaptor rowStoreMetaAdaptor;
    //private final boolean autoBalanceSplit = CoordinatorConfiguration.instance().autoSchedule();
    private boolean autoBalanceSplit;
    private ClusterStatsManager clusterStatsManager = ClusterStatsManager.getInstance(0);

    public RegionHeartbeatHandler(RowStoreMetaAdaptor rowStoreMetaAdaptor, boolean autoBalanceSplit) {
        this.rowStoreMetaAdaptor = rowStoreMetaAdaptor;
        this.autoBalanceSplit = autoBalanceSplit;
    }

    @Override
    public void onMessage(Message message, Channel channel) {

    }

    @Override
    public void handleRequest(RpcContext rpcCtx, RegionHeartbeatRequest request) {
        RegionHeartbeatResponse response = new RegionHeartbeatResponse();
        if (rowStoreMetaAdaptor.available()) {
            response.setValue(new ArrayList<>());
            rowStoreMetaAdaptor.saveRegionHeartbeat(request.getRegion(), request.getRegionStats());
            if (autoBalanceSplit) {
                Optional.ofNullable(balance(request.getRegion(), request.getRegionStats()))
                    .ifAbsentSet(() -> split(request))
                    .ifPresent(response.getValue()::add);

            }
        } else {
            response.setError(Errors.NOT_LEADER);
        }
        rpcCtx.sendResponse(response);
    }

    @Override
    public String interest() {
        return REQ_CLASS.getName();
    }

    private Instruction balance(Region region, RegionStats regionStats) {
        final long clusterId = 0;
        final String storeId = regionStats.getLeader().getStoreId();
        final ClusterStatsManager clusterStatsManager = ClusterStatsManager.getInstance(clusterId);

        clusterStatsManager.addOrUpdateLeader(storeId, region.getId());

        // check if the modelWorker
        final Pair<Set<String>, Integer> modelWorkers = clusterStatsManager.findModelWorkerStores(1);
        final Set<String> modelWorkerStoreIds = modelWorkers.getKey();
        final int modelWorkerLeaders = modelWorkers.getValue();
        if (!modelWorkerStoreIds.contains(storeId)) {
            return null;
        }

        log.info("[Cluster] model worker stores is: {}, it has {} leaders.", modelWorkerStoreIds, modelWorkerLeaders);


        final List<Peer> peers = region.getPeers();
        if (peers == null) {
            return null;
        }
        final List<Endpoint> endpoints = Lists.transform(peers, Peer::getEndpoint);
        final Map<String, Endpoint> storeIds = rowStoreMetaAdaptor.storeLocation();
        // find lazyWorkers
        final List<Pair<String, Integer>> lazyWorkers = clusterStatsManager.findLazyWorkerStores(storeIds.keySet());

        if (lazyWorkers.isEmpty()) {
            return null;
        }

        for (int i = lazyWorkers.size() - 1; i >= 0; i--) {
            final Pair<String, Integer> worker = lazyWorkers.get(i);
            if (modelWorkerLeaders - worker.getValue() <= 1) { // no need to transfer
                lazyWorkers.remove(i);
            }
        }

        if (lazyWorkers.isEmpty()) {
            return null;
        }
        final Pair<String, Integer> laziestWorker = tryToFindLaziestWorker(lazyWorkers);
        if (laziestWorker == null) {
            return null;
        }

        final String lazyWorkerStoreId = laziestWorker.getKey();
        log.info("[Cluster: {}], lazy worker store is: {}, it has {} leaders.", clusterId, lazyWorkerStoreId,
            laziestWorker.getValue());
        final Instruction.TransferLeader transferLeader = new Instruction.TransferLeader();
        transferLeader.setMoveToStoreId(lazyWorkerStoreId);
        transferLeader.setMoveToEndpoint(storeIds.get(lazyWorkerStoreId));
        final Instruction instruction = new Instruction();
        instruction.setRegion(region.copy());
        instruction.setTransferLeader(transferLeader);
        log.info("[Cluster: {}], send 'instruction.transferLeader': {} to region: {}.", clusterId, instruction, region);
        return instruction;
    }

    private Pair<String, Integer> tryToFindLaziestWorker(final List<Pair<String, Integer>> lazyWorkers) {
        final List<Pair<Pair<String, Integer>, StoreStats>> storeStatsList = Lists.newArrayList();
        for (final Pair<String, Integer> worker : lazyWorkers) {
            final StoreStats stats = rowStoreMetaAdaptor.storeStats(GeneralId.fromStr(worker.getKey()));
            if (stats != null) {
                // TODO check timeInterval
                storeStatsList.add(Pair.of(worker, stats));
            }
        }
        if (storeStatsList.isEmpty()) {
            return null;
        }
        if (storeStatsList.size() == 1) {
            return storeStatsList.get(0).getKey();
        }
        final Pair<Pair<String, Integer>, StoreStats> min = Collections.min(storeStatsList, (o1, o2) -> {
            final StoreStats s1 = o1.getValue();
            final StoreStats s2 = o2.getValue();
            int val = Boolean.compare(s1.isBusy(), s2.isBusy());
            if (val != 0) {
                return val;
            }
            val = Integer.compare(s1.getRegionCount(), s2.getRegionCount());
            if (val != 0) {
                return val;
            }
            val = Long.compare(s1.getBytesWritten(), s2.getBytesWritten());
            if (val != 0) {
                return val;
            }
            val = Long.compare(s1.getBytesRead(), s2.getBytesRead());
            if (val != 0) {
                return val;
            }
            val = Long.compare(s1.getKeysWritten(), s2.getKeysWritten());
            if (val != 0) {
                return val;
            }
            val = Long.compare(s1.getKeysRead(), s2.getKeysRead());
            if (val != 0) {
                return val;
            }
            return Long.compare(-s1.getAvailable(), -s2.getAvailable());
        });
        return min.getKey();
    }

    private Instruction split(RegionHeartbeatRequest request) {
        Region region = request.getRegion();
        RegionStats regionStats = request.getRegionStats();
        clusterStatsManager.addOrUpdateRegionStats(region, regionStats);
        final Set<String> stores = rowStoreMetaAdaptor.storeLocation().keySet();
        if (stores.isEmpty()) {
            return null;
        }

        if (clusterStatsManager.regionSize() >= stores.size()) {
            // one store one region is perfect
            return null;
        }
        final Pair<Region, RegionStats> modelWorker = clusterStatsManager.findModelWorkerRegion();
        if (!isSplitNeeded(request, modelWorker)) {
            return null;
        }

        final String newRegionId = rowStoreMetaAdaptor.newRegionId().seqNo().toString();
        final Instruction.RangeSplit rangeSplit = new Instruction.RangeSplit();
        rangeSplit.setNewRegionId(newRegionId);
        final Instruction instruction = new Instruction();
        instruction.setRegion(modelWorker.getKey().copy());
        instruction.setRangeSplit(rangeSplit);
        return instruction;
    }

    private boolean isSplitNeeded(RegionHeartbeatRequest request, Pair<Region, RegionStats> modelWorker) {
        if (modelWorker == null || request.getLeastKeysOnSplit() > modelWorker.getValue().getApproximateKeys()) {
            return false;
        }
        return modelWorker.getKey().equals(request.getRegion());
    }
}
