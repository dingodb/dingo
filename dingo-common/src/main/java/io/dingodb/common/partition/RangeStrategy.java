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

package io.dingodb.common.partition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.CommonId;
import io.dingodb.common.codec.DingoKeyValueCodec;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeMap;

@JsonPropertyOrder({"definition", "ranges"})
@JsonTypeName("RangeHash")
public class RangeStrategy extends PartitionStrategy<CommonId> {

    @JsonProperty("definition")
    private final TableDefinition definition;

    @JsonProperty("ranges")
    private final NavigableMap<ComparableByteArray, Distribution> ranges;

    private final transient DingoKeyValueCodec codec;

    @JsonCreator
    public RangeStrategy(
        @JsonProperty("definition") @NonNull TableDefinition definition,
        @JsonProperty("ranges") NavigableMap<ComparableByteArray, Distribution> ranges
    ) {
        this.ranges = ranges;
        this.definition = definition;
        this.codec = definition.createCodec();
    }

    @Override
    public int getPartNum() {
        return ranges.size();
    }

    @Override
    public CommonId calcPartId(Object @NonNull [] keyTuple) {
        try {
            return calcPartId(codec.encodeKey(keyTuple));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CommonId calcPartId(byte @NonNull [] keyBytes) {
        return ranges.floorEntry(new ComparableByteArray(keyBytes)).getValue().id();
    }

    @Override
    public Map<byte[], byte[]> calcPartitionRange(
        byte @NonNull [] startKey,
        byte @NonNull [] endKey,
        boolean includeEnd
    ) {
        NavigableSet<ComparableByteArray> ranges = this.ranges.navigableKeySet();
        Map<byte[], byte[]> keyMap = new TreeMap<>(ByteArrayUtils::compare);
        LinkedHashSet<ComparableByteArray> keySet = new LinkedHashSet<>();
        // Support > < condition when deleting
        if (startKey == null) {
            for (ComparableByteArray key : ranges) {
                if (ByteArrayUtils.lessThanOrEqual(key.getBytes(), endKey)) {
                    keySet.add(key);
                }
            }
        } else if (endKey == null) {
            for (ComparableByteArray key : ranges) {
                if (ByteArrayUtils.greatThanOrEqual(key.getBytes(), startKey)) {
                    keySet.add(key);
                }
            }
        } else {
            for (ComparableByteArray key : ranges) {
                if (ByteArrayUtils.greatThanOrEqual(key.getBytes(), startKey)
                    && ByteArrayUtils.lessThanOrEqual(key.getBytes(), endKey)) {
                    keySet.add(key);
                }
            }
        }
        LinkedHashSet<ComparableByteArray> subSet = keySet;

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
