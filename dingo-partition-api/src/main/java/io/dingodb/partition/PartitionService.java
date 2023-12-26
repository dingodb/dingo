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

package io.dingodb.partition;

import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.function.Function;

public interface PartitionService {
    static PartitionService getService(String funcName) {
        return DingoPartitionServiceProvider.getDefault().getService(funcName);
    }

    int getPartNum(NavigableMap<ComparableByteArray, RangeDistribution> ranges);

    CommonId calcPartId(byte[] key, NavigableMap<ComparableByteArray, RangeDistribution> ranges);

    default CommonId calcPartId(final Object @NonNull [] key, @NonNull Function<Object[], byte[]> getKeyFunction,
                                NavigableMap<ComparableByteArray, RangeDistribution> ranges) {
        return calcPartId(getKeyFunction.apply(key), ranges);
    }

    NavigableSet<RangeDistribution> calcPartitionRange(
        final byte[] startKey,
        final byte[] endKey,
        boolean withStart,
        boolean withEnd,
        NavigableMap<ComparableByteArray, RangeDistribution> ranges
    );

    default Map<CommonId, List<Object[]>> partTuples(
        final @NonNull Collection<Object[]> tuples,
        @NonNull Function<Object[], byte[]> getKeyFunction,
        NavigableMap<ComparableByteArray, RangeDistribution> ranges
    ) {
        Map<CommonId, List<Object[]>> map = new LinkedHashMap<>(getPartNum(ranges));
        for (Object[] tuple : tuples) {
            CommonId partId = calcPartId(getKeyFunction.apply(tuple), ranges);
            map.putIfAbsent(partId, new LinkedList<>());
            map.get(partId).add(tuple);
        }
        return map;
    }

    default Map<CommonId, List<byte[]>> partKeys(
        final @NonNull List<byte[]> keys,
        NavigableMap<ComparableByteArray, RangeDistribution> ranges
    ) {
        Map<CommonId, List<byte[]>> map = new LinkedHashMap<>(getPartNum(ranges));
        for (byte[] key: keys) {
            CommonId partId = calcPartId(key, ranges);
            map.putIfAbsent(partId, new LinkedList<>());
            map.get(partId).add(key);
        }
        return map;
    }
}
