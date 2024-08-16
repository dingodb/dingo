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

package io.dingodb.store.service;

import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.StoreService;
import io.dingodb.sdk.service.entity.coordinator.ScanRegionInfo;
import io.dingodb.store.proxy.meta.ScanRegionWithPartId;
import io.dingodb.store.proxy.service.TransactionStoreInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;

public class MetaKvTxn extends CommitBase {
    Function<String, byte[]> getMetaRegionKey;
    Function<String, byte[]> getMetaRegionEndKey;
    public MetaKvTxn(StoreService storeService,
                     CommonId partId,
                     Function<String, byte[]> getMetaRegionKey,
                     Function<String, byte[]> getMetaRegionEndKey
    ) {
        super(storeService, partId);
        this.getMetaRegionKey = getMetaRegionKey;
        this.getMetaRegionEndKey = getMetaRegionEndKey;
    }

    @Override
    public TransactionStoreInstance refreshRegion(byte[] key) {
        CommonId regionIdNew = refreshRegionId(getMetaRegionKey.apply(null), getMetaRegionEndKey.apply(null), key);
        StoreService serviceNew = Services.storeRegionService(coordinators, regionIdNew.seq, 60);
        return new TransactionStoreInstance(serviceNew, null, partId);
    }

    public static CommonId refreshRegionId(byte[] startKey, byte[] endKey, byte[] key) {
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution
            = loadDistribution(startKey, endKey);
        String strategy = DingoPartitionServiceProvider.RANGE_FUNC_NAME;

        return PartitionService.getService(
                strategy)
            .calcPartId(key, rangeDistribution);
    }

    private static NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> loadDistribution(
        byte[] startKey, byte[] endKey
    ) {
        io.dingodb.meta.InfoSchemaService infoSchemaService = io.dingodb.store.service.InfoSchemaService.ROOT;
        List<Object> regionList = infoSchemaService
            .scanRegions(startKey, endKey);
        List<ScanRegionWithPartId> rangeDistributionList = new ArrayList<>();
        regionList
            .forEach(object -> {
                ScanRegionInfo scanRegionInfo = (ScanRegionInfo) object;
                rangeDistributionList.add(
                    new ScanRegionWithPartId(scanRegionInfo, 0)
                );
            });
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> result = new TreeMap<>();

        rangeDistributionList.forEach(scanRegionWithPartId -> {
            ScanRegionInfo scanRegionInfo = scanRegionWithPartId.getScanRegionInfo();
            byte[] startInner = scanRegionInfo.getRange().getStartKey();
            byte[] endInner = scanRegionInfo.getRange().getEndKey();
            RangeDistribution distribution = RangeDistribution.builder()
                .id(new CommonId(CommonId.CommonType.DISTRIBUTION, scanRegionWithPartId.getPartId(), scanRegionInfo.getRegionId()))
                .startKey(startInner)
                .endKey(endInner)
                .build();
            result.put(new ByteArrayUtils.ComparableByteArray(distribution.getStartKey(), 1), distribution);
        });
        return result;
    }
}
