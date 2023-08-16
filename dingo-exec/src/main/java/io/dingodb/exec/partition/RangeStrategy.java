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
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.RangeUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeMap;

@JsonPropertyOrder({"definition", "ranges"})
@JsonTypeName("RangeHash")
public class RangeStrategy extends PartitionStrategy<CommonId, byte[]> {

    @JsonProperty("definition")
    private final TableDefinition definition;

    @JsonProperty("ranges")
    @JsonSerialize(keyUsing = ComparableByteArray.JacksonKeySerializer.class)
    @JsonDeserialize(keyUsing = ComparableByteArray.JacksonKeyDeserializer.class)
    private final NavigableMap<ComparableByteArray, RangeDistribution> ranges;

    @JsonCreator
    public RangeStrategy(
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
        return ranges.floorEntry(new ComparableByteArray(keyBytes)).getValue().id();
    }

    @Override
    public NavigableSet<RangeDistribution> calcPartitionRange(
        byte [] startKey,
        byte [] endKey,
        boolean withStart,
        boolean withEnd
    ) {
        if (startKey == null) {
            startKey = ranges.firstEntry().getValue().getStartKey();
            withStart = true;
        }
        if (endKey == null) {
            endKey = ranges.lastEntry().getValue().getEndKey();
            withEnd = true;
        }
        RangeDistribution range = RangeDistribution.builder()
            .startKey(startKey)
            .endKey(endKey)
            .withStart(withStart)
            .withEnd(withEnd)
            .build();
        NavigableSet<RangeDistribution> ranges = RangeUtils.getSubRangeDistribution(this.ranges.values(), range, 0);
        ranges.descendingSet().stream().skip(1).forEach(rd -> {
            if (Arrays.equals(rd.getEndKey(), ranges.last().getEndKey())) {
                rd.setWithEnd(true);
            }
        });
        return ranges;
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
