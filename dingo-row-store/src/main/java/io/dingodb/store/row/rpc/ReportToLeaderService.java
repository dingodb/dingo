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

package io.dingodb.store.row.rpc;

import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.store.row.RegionEngine;
import io.dingodb.store.row.StoreEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ServiceLoader;

public class ReportToLeaderService implements ReportToLeaderApi {
    private static final Logger LOG = LoggerFactory.getLogger(ReportToLeaderService.class);
    private static final ReportToLeaderService INSTANCE = new ReportToLeaderService();
    private static StoreEngine storeEngine;

    private final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    public static ReportToLeaderService instance() {
        return INSTANCE;
    }

    public void init(StoreEngine engine) {
        netService.apiRegistry().register(ReportToLeaderApi.class, this);
        storeEngine = engine;
    }

    private class RegionStatus {
        public RegionEngine regionEngine;
        public ApiStatus status;
    }

    @Override
    public ApiStatus freezeSnapshotResult(@Nonnull final String regionId, final boolean freezeResult,
                                          final String errMsg) {
        final RegionStatus rs = getAndCheckRegion(regionId);
        if (rs.status != ApiStatus.OK) {
            LOG.error("freezeSnapshotResult, regionId: {}, freezeResult: {}, status: {}.",
                regionId, freezeResult, rs.status);
            return rs.status;
        }

        int peerCount = rs.regionEngine.getRegion().getPeerCount();
        LOG.info("freezeSnapshotResult, regionId: {}, freezeResult: {}, status: {}, peer count: {}.",
            regionId, freezeResult, rs.status, peerCount);
        rs.regionEngine.getNode().freezeSnapshotReply(peerCount, freezeResult, errMsg);
        return rs.status;
    }

    @Override
    public ApiStatus snapshotMd5Result(@Nonnull final String regionId, final long kVcount, final String md5Str,
                                       final long lastAppliedIndex) {
        final RegionStatus rs = getAndCheckRegion(regionId);
        if (rs.status != ApiStatus.OK) {
            LOG.error("snapshotMd5Result, regionId: {}, kVcount: {}, md5Str: {}, status: {}.",
                regionId, kVcount, md5Str, rs.status);
            return rs.status;
        }

        LOG.info("snapshotMd5Result, regionId: {}, kVcount: {}, md5Str: {}, status: {}.",
            regionId, kVcount, md5Str, rs.status);

        int peerCount = rs.regionEngine.getRegion().getPeerCount();
        rs.regionEngine.getNode().snapshotMd5Reply(peerCount, kVcount, md5Str, lastAppliedIndex);
        return rs.status;
    }

    private RegionStatus getAndCheckRegion(@Nonnull String regionId) {
        RegionStatus rs = new RegionStatus();
        rs.status = ApiStatus.OK;
        RegionEngine regionEngine = storeEngine.getRegionEngine(regionId);
        if (regionEngine == null) {
            rs.status = ApiStatus.REGION_NOT_FOUND;
            return rs;
        }

        rs.regionEngine = regionEngine;
        if(!regionEngine.isLeader()) {
            rs.status = ApiStatus.NOT_LEADRE;
        }

        return rs;
    }
}
