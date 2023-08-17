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

package io.dingodb.client.common;

import io.dingodb.common.ConsistentHashing;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.index.Index;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.common.utils.ByteArrayUtils;
import io.dingodb.sdk.service.meta.AutoIncrementService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static io.dingodb.common.util.ByteArrayUtils.SKIP_LONG_POS;

@Slf4j
@AllArgsConstructor
public class IndexInfo {

    public final String schemaName;
    public final String indexName;
    public final DingoCommonId indexId;

    public final Index index;
    public final KeyValueCodec codec;
    public final AutoIncrementService autoIncrementService;

    public final NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution;

    public DingoCommonId calcRegionId(byte[] key) {
        String strategy = index.getIndexPartition().getFuncName().toUpperCase();
        DingoCommonId commonId = null;
        switch (strategy) {
            case "RANGE":
                // skip the first 8 bytes when comparing byte[] (id)
                commonId = rangeDistribution.floorEntry(new ByteArrayUtils.ComparableByteArray(key, SKIP_LONG_POS)).getValue().getId();
                break;
            case "HASH":
                ConsistentHashing<Long> hashRing = new ConsistentHashing<>(3);
                NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> partRanges = new TreeMap<>();
                for (Map.Entry<ByteArrayUtils.ComparableByteArray, RangeDistribution> entry : rangeDistribution.entrySet()) {
                    RangeDistribution value = entry.getValue();
                    log.trace("entityId:" + value.getId().entityId() + ",parentId:" + value.getId().parentId());
                    hashRing.addNode(value.getId().parentId());
                }
                Long selectNode = hashRing.getNode(key);
                for (Map.Entry<ByteArrayUtils.ComparableByteArray, RangeDistribution> entry : rangeDistribution.entrySet()) {
                    ByteArrayUtils.ComparableByteArray keyBytes = entry.getKey();
                    RangeDistribution value = entry.getValue();
                    if (value.getId().parentId() == selectNode.longValue()) {
                        partRanges.put(keyBytes, value);
                    }
                }
                commonId = partRanges.floorEntry(new ByteArrayUtils.ComparableByteArray(key, SKIP_LONG_POS)).getValue().getId();
                break;
            default:
                throw new IllegalStateException("Unsupported " + strategy);
        }
        return commonId;
    }
}
