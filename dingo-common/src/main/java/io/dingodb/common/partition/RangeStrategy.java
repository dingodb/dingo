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
import io.dingodb.common.codec.Codec;
import io.dingodb.common.codec.DingoCodec;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.converter.DingoConverter;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeMap;

@JsonPropertyOrder({"definition", "ranges"})
@JsonTypeName("RangeHash")
public class RangeStrategy extends PartitionStrategy<ComparableByteArray> {

    @JsonProperty("definition")
    private final TableDefinition definition;

    @JsonProperty("ranges")
    private final NavigableSet<ComparableByteArray> ranges;

    private final transient Codec codec;
    private final transient DingoType keySchema;

    @JsonCreator
    public RangeStrategy(
        @JsonProperty("definition") @NonNull TableDefinition definition,
        @JsonProperty("ranges") NavigableSet<ComparableByteArray> ranges
    ) {
        this.ranges = ranges;
        this.definition = definition;
        this.keySchema = definition.getDingoType(true);
        this.codec = new DingoCodec(definition.getDingoSchemaOfKey(), null, true);
    }

    @Override
    public int getPartNum() {
        return ranges.size();
    }

    @Override
    public ComparableByteArray calcPartId(Object @NonNull [] keyTuple) {
        try {
            return calcPartId(codec.encodeKey((Object[]) keySchema.convertTo(keyTuple, DingoConverter.INSTANCE)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ComparableByteArray calcPartId(byte @NonNull [] keyBytes) {
        return ranges.floor(new ComparableByteArray(keyBytes));
    }

    @Override
    public Map<byte[], byte[]> calcPartitionRange(
        byte @NonNull [] startKey,
        byte @NonNull [] endKey,
        boolean includeEnd
    ) {
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
        Map<byte[], byte[]> keyMap = new TreeMap<>(ByteArrayUtils::compare);

        SortedSet<ComparableByteArray> subSet = ranges.subSet(
            ranges.floor(new ComparableByteArray(startKey)), true,
            new ComparableByteArray(endKey, true), includeEnd
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
}
