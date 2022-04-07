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
import io.dingodb.raft.util.Pair;
import io.dingodb.store.row.RegionEngine;
import io.dingodb.store.row.StoreEngine;
import io.dingodb.store.row.util.RegionHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ServiceLoader;

public class CompareRegionService implements CompareRegionApi{
    private static final Logger LOG = LoggerFactory.getLogger(CompareRegionService.class);
    private static final CompareRegionService INSTANCE = new CompareRegionService();
    private static StoreEngine storeEngine;

    private final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    public static CompareRegionService instance() {
        return INSTANCE;
    }

    public void init(StoreEngine engine) {
        netService.apiRegistry().register(CompareRegionApi.class, this);
        storeEngine = engine;
    }

    @Override
    public ApiGetLeaderReply getLeader(@Nonnull final String regionId) {
        LOG.info("CompareRegionService getLeader, regionId: {}.", regionId);
        ApiGetLeaderReply reply = new ApiGetLeaderReply(ApiStatus.OK);
        RegionEngine regionEngine = storeEngine.getRegionEngine(regionId);
        if (regionEngine == null) {
            reply.setStatus(ApiStatus.REGION_NOT_FOUND);
            return reply;
        }
        reply.setIp(regionEngine.getLeaderId().getIp());
        reply.setPort(regionEngine.getNode().getOptions().getServerExchangePort());
        LOG.info("CompareRegionService getLeader, result leader ip: {}, port: {}.",
            reply.getIp(), reply.getPort());

        return reply;
    }

    @Override
    public ApiStatus startCompare(@Nonnull String regionId) {
        LOG.info("CompareRegionService compare, regionId: {}.", regionId);
        ApiStatus status = ApiStatus.OK;
        RegionEngine regionEngine = storeEngine.getRegionEngine(regionId);
        if (regionEngine == null) {
            status = ApiStatus.REGION_NOT_FOUND;
            LOG.error("CompareRegionService compare, regionId: {}, status: {}.", regionId, status);
            return status;
        }

        if(!regionEngine.isLeader()) {
            status = ApiStatus.NOT_LEADRE;
            LOG.error("CompareRegionService compare, regionId: {}, status: {}.", regionId, status);
            return status;
        }

        if (!RegionHelper.isMultiGroup(regionEngine.getRegion())) {
            status = ApiStatus.NOT_MUTLTI_GROUP;
            LOG.error("CompareRegionService compare, regionId: {}, status: {}.", regionId, status);
            return status;
        }

        if (regionEngine.getNode().isComparing()) {
            status = ApiStatus.COMPARING_BY_ANOTHER;
            LOG.error("CompareRegionService compare, regionId: {}, status: {}.", regionId, status);
            return status;
        }
        regionEngine.getNode().startCompare();
        return status;
    }

    @Override
    public ApiCompareReply getResult(@Nonnull String regionId) {
        ApiCompareReply reply = new ApiCompareReply(ApiStatus.OK);
        RegionEngine regionEngine = storeEngine.getRegionEngine(regionId);
        if (regionEngine == null) {
            reply.setStatus(ApiStatus.REGION_NOT_FOUND);
            LOG.error("CompareRegionService getResult, regionId: {}, status: {}.", regionId, reply.getStatus());
            return reply;
        }

        if(!regionEngine.isLeader()) {
            reply.setStatus(ApiStatus.NOT_LEADRE);
            LOG.error("CompareRegionService getResult, regionId: {}, status: {}.", regionId, reply.getStatus());
            return reply;
        }

        Pair<Integer, String> ret = regionEngine.getNode().getCompareResult();
        if (ret.getKey() == 0) {
            LOG.info("CompareRegionService getResult, regionId: {}, compare result is the same, {}.",
                regionId, ret.getValue());
        } else if (ret.getKey() == -3) {
            reply.setStatus(ApiStatus.COMPARING);
            LOG.info("CompareRegionService getResult, regionId: {}, compare result is comparing, {}.",
                regionId, ret.getValue());
        } else {
            LOG.info("CompareRegionService getResult, regionId: {}, compare result is different, {}.",
                regionId, ret.getValue());
        }
        reply.setCompareResult(ret.getKey());
        reply.setMsg(ret.getValue());
        return reply;
    }
}
