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

package io.dingodb.exec.partition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.CommonId;
import io.dingodb.common.ConsistentHashing;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.RangeUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

@JsonPropertyOrder({"definition", "ranges"})
@JsonTypeName("HashRange")
@Slf4j
public class HashRangeStrategy extends PartitionStrategy<CommonId, byte[]> {

    @JsonProperty("definition")
    private final TableDefinition definition;

    @JsonProperty("ranges")
    @JsonSerialize(keyUsing = ComparableByteArray.JacksonKeySerializer.class)
    @JsonDeserialize(keyUsing = ComparableByteArray.JacksonKeyDeserializer.class)
    private final NavigableMap<ComparableByteArray, RangeDistribution> ranges;

    @JsonCreator
    public HashRangeStrategy(
        @JsonProperty("definition") @NonNull TableDefinition definition,
        @JsonProperty("ranges") NavigableMap<ComparableByteArray, RangeDistribution> ranges
    ) {
        this.ranges = ranges;
        this.definition = definition;
    }

    @Override
    public int getPartNum() {
        return ranges.size();
    }

    @Override
    public CommonId calcPartId(byte @NonNull [] keyBytes) {
        ConsistentHashing<Long> hashRing = new ConsistentHashing<>(3);
        NavigableMap<ComparableByteArray, RangeDistribution> partRanges = new TreeMap<>();
        for (Map.Entry<ComparableByteArray, RangeDistribution> entry : ranges.entrySet()) {
            RangeDistribution value = entry.getValue();
            log.trace("id:" + value.getId().domain);
            hashRing.addNode(value.getId().domain);
        }
        Long selectNode = hashRing.getNode(keyBytes);
        for (Map.Entry<ComparableByteArray, RangeDistribution> entry : ranges.entrySet()) {
            ComparableByteArray key = entry.getKey();
            RangeDistribution value = entry.getValue();
            if (value.getId().domain == selectNode.longValue()) {
                partRanges.put(key, value);
            }
        }
        return partRanges.floorEntry(new ComparableByteArray(keyBytes)).getValue().id();
    }

    @Override
    public NavigableSet<RangeDistribution> calcPartitionRange(
        byte [] startKey,
        byte [] endKey,
        boolean withStart,
        boolean withEnd
    ) {

        Map<Long, NavigableMap<ComparableByteArray, RangeDistribution>> map = new HashMap<>();
        for (Map.Entry<ComparableByteArray, RangeDistribution> entry : ranges.entrySet()) {
            ComparableByteArray key = entry.getKey();
            RangeDistribution value = entry.getValue();
            Long domain = value.getId().domain;
            log.trace("id:" + domain);
            map.computeIfAbsent(domain, k -> new TreeMap<>()).put(key, value);
        }
        NavigableSet<RangeDistribution> distributions = new TreeSet<>(RangeUtils.rangeComparator());
        for (Map.Entry<Long, NavigableMap<ComparableByteArray, RangeDistribution>> entry : map.entrySet()) {
            NavigableMap<ComparableByteArray, RangeDistribution> subMap = entry.getValue();
            startKey = Optional.ofNullable(startKey).orElseGet(() -> subMap.firstEntry().getValue().getStartKey());
            withStart = true;
            endKey = Optional.ofNullable(endKey).orElseGet(() -> subMap.lastEntry().getValue().getEndKey());
            withEnd = true;

            RangeDistribution range = RangeDistribution.builder()
                .startKey(startKey)
                .endKey(endKey)
                .withStart(withStart)
                .withEnd(withEnd)
                .build();
            NavigableSet<RangeDistribution> ranges = RangeUtils.getSubRangeDistribution(subMap.values(), range, 0);
            ranges.descendingSet().stream().skip(1).forEach(rd -> {
                if (Arrays.equals(rd.getEndKey(), ranges.last().getEndKey())) {
                    rd.setWithEnd(true);
                }
            });
            distributions.addAll(ranges);
        }
        return  distributions;
    }

    @Override
    public Map<byte[], byte[]> calcPartitionPrefixRange(
        byte @NonNull [] startKey,
        byte @NonNull [] endKey,
        boolean includeEnd,
        boolean prefixRange
    ) {
        NavigableSet<ComparableByteArray> ranges = this.ranges.navigableKeySet();
        Map<byte[], byte[]> keyMap = new TreeMap<>(ByteArrayUtils::compare);

        SortedSet<ComparableByteArray> subSet = ranges.subSet(
            ranges.floor(new ComparableByteArray(startKey)), true,
            new ComparableByteArray(endKey, prefixRange), includeEnd
        );

        byte[] start = startKey;
        byte[] end;
        Iterator<ComparableByteArray> iterator = subSet.iterator();
        while (iterator.hasNext()) {
            ComparableByteArray sKey = iterator.next();

            if (start == null) {
                start = sKey.getBytes();
            }
            if (iterator.hasNext()) {
                end = null;
            } else {
                end = endKey;
            }

            keyMap.put(start, end);
            start = null;
        }

        return keyMap;
    }

    @Override
    public Map<byte[], byte[]> calcPartitionByPrefix(byte @NonNull [] prefix) {
        Map<byte[], byte[]> keyMap = new TreeMap<>(ByteArrayUtils::compare);
        LinkedHashSet<ComparableByteArray> keySet = new LinkedHashSet<>();

        // Identify left boundary
        boolean isLeft = true;
        ComparableByteArray previousKey = null;
        // Traverse to find partitions
        for (ComparableByteArray key : ranges.navigableKeySet()) {
            if (isLeft) {
                if (ByteArrayUtils.lessThanOrEqual(prefix, key.getBytes())) {
                    if (previousKey != null) {
                        keySet.add(previousKey);
                    } else {
                        keySet.add(key);
                    }
                    isLeft = false;
                }
            } else {
                if (ByteArrayUtils.greatThanOrEqual(prefix, key.getBytes())) {
                    keySet.add(key);
                }
            }
            previousKey = key;
        }

        // If there is no satisfied partition, take the last partition
        if (keySet.size() == 0) {
            keyMap.put(ranges.lastKey().getBytes(), null);
            return keyMap;
        }

        // Traverse all keys, add them to the map, and return
        LinkedHashSet<ComparableByteArray> subSet = keySet;
        Iterator<ComparableByteArray> iterator = subSet.iterator();
        while (iterator.hasNext()) {
            ComparableByteArray sKey = iterator.next();
            keyMap.put(sKey.getBytes(), null);
        }

        return keyMap;
    }

}
