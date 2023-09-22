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

package io.dingodb.partition.base;

import io.dingodb.codec.CodecService;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.RangeUtils;
import io.dingodb.partition.PartitionService;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

@Slf4j
public class HashRangePartitionService implements PartitionService {

    @Override
    public int getPartNum(NavigableMap<ComparableByteArray, RangeDistribution> ranges) {
        return ranges.size();
    }

    @Override
    public CommonId calcPartId(byte[] key, NavigableMap<ComparableByteArray, RangeDistribution> ranges) {
        ConsistentHashing<Long> hashRing = new ConsistentHashing<>(3);
        NavigableMap<ComparableByteArray, RangeDistribution> partRanges = new TreeMap<>();
        for (Map.Entry<ComparableByteArray, RangeDistribution> entry : ranges.entrySet()) {
            RangeDistribution value = entry.getValue();
            log.trace("id:" + value.getId().domain);
            hashRing.addNode(value.getId().domain);
        }
        Long selectNode = hashRing.getNode(key);
        for (Map.Entry<ComparableByteArray, RangeDistribution> entry : ranges.entrySet()) {
            ComparableByteArray rangeKeyBytes = entry.getKey();
            RangeDistribution rangeValue = entry.getValue();
            if (rangeValue.getId().domain == selectNode.longValue()) {
                partRanges.put(rangeKeyBytes, rangeValue);
            }
        }

        CodecService.getDefault().setId(key, new CommonId(CommonId.CommonType.PARTITION, 0, selectNode));
        return partRanges.floorEntry(new ComparableByteArray(key)).getValue().id();
    }

    @Override
    public NavigableSet<RangeDistribution> calcPartitionRange(
        byte [] startKey,
        byte [] endKey,
        boolean withStart,
        boolean withEnd,
        NavigableMap<ComparableByteArray, RangeDistribution> ranges
    ) {

        Map<Long, NavigableMap<ComparableByteArray, RangeDistribution>> map = new HashMap<>();
        for (Map.Entry<ComparableByteArray, RangeDistribution> entry : ranges.entrySet()) {
            ComparableByteArray key = entry.getKey();
            RangeDistribution value = entry.getValue();
            Long domain = value.getId().domain;
            log.trace("id:" + domain);
            map.computeIfAbsent(domain, k -> new TreeMap<>()).put(key, value);
        }
        NavigableSet<RangeDistribution> distributions = new TreeSet<>(RangeUtils.rangeComparator(0));
        for (Map.Entry<Long, NavigableMap<ComparableByteArray, RangeDistribution>> entry : map.entrySet()) {
            NavigableMap<ComparableByteArray, RangeDistribution> subMap = entry.getValue();
            byte [] newStartKey;
            byte [] newEndKey;
            boolean newWithStart;
            boolean newWithEnd;
            if (startKey == null) {
                newStartKey = subMap.firstEntry().getValue().getStartKey();
                newWithStart = true;
            } else {
                newStartKey = startKey;
                newWithStart = withStart;
            }
            if (endKey == null) {
                newEndKey = subMap.lastEntry().getValue().getEndKey();
                newWithEnd = true;
            } else {
                newEndKey = endKey;
                newWithEnd = withEnd;
            }
            RangeDistribution range = RangeDistribution.builder()
                .startKey(newStartKey)
                .endKey(newEndKey)
                .withStart(newWithStart)
                .withEnd(newWithEnd)
                .build();
            NavigableSet<RangeDistribution> subRanges = RangeUtils.getSubRangeDistribution(subMap.values(), range, 8);
            subRanges.descendingSet().stream().skip(1).forEach(rd -> {
                if (Arrays.equals(rd.getEndKey(), subMap.lastEntry().getValue().getEndKey())) {
                    rd.setWithEnd(true);
                }
            });
            distributions.addAll(subRanges);
        }
        return  distributions;
    }

}
