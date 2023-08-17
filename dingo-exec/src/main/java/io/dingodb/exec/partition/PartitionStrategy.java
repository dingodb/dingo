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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.dingodb.common.partition.RangeDistribution;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.function.Function;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type"
)
@JsonSubTypes({
    @JsonSubTypes.Type(RangeStrategy.class),
    @JsonSubTypes.Type(HashRangeStrategy.class),
})
public abstract class PartitionStrategy<I, K> {

    public abstract int getPartNum();

    public abstract I calcPartId(final @NonNull K key);

    public I calcPartId(final Object @NonNull [] key, @NonNull Function<Object[], K> getKeyFunction) {
        return calcPartId(getKeyFunction.apply(key));
    }

    public abstract NavigableSet<RangeDistribution> calcPartitionRange(
        final byte [] startKey,
        final byte [] endKey,
        boolean withStart,
        boolean withEnd
    );

    public abstract Map<byte[], byte[]> calcPartitionPrefixRange(
        final byte @NonNull [] startKey,
        final byte @NonNull [] endKey,
        boolean includeEnd,
        boolean prefixRange
    );

    public abstract Map<byte[], byte[]> calcPartitionByPrefix(final byte @NonNull [] prefix);

    public Map<I, List<K>> partKeys(final @NonNull Collection<K> keys) {
        Map<I, List<K>> map = new LinkedHashMap<>(getPartNum());
        for (K key : keys) {
            I partId = calcPartId(key);
            map.putIfAbsent(partId, new LinkedList<>());
            map.get(partId).add(key);
        }
        return map;
    }

    public Map<I, List<Object[]>> partTuples(
        final @NonNull Collection<Object[]> tuples,
        @NonNull Function<Object[], K> getKeyFunction
    ) {
        Map<I, List<Object[]>> map = new LinkedHashMap<>(getPartNum());
        for (Object[] tuple : tuples) {
            I partId = calcPartId(getKeyFunction.apply(tuple));
            map.putIfAbsent(partId, new LinkedList<>());
            map.get(partId).add(tuple);
        }
        return map;
    }
}
