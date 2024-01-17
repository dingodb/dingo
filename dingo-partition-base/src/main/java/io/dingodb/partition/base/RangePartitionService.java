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

import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.common.util.RangeUtils;
import io.dingodb.partition.PartitionService;

import java.util.Arrays;
import java.util.NavigableMap;
import java.util.NavigableSet;

public class RangePartitionService implements PartitionService {

    @Override
    public int getPartNum(NavigableMap<ComparableByteArray, RangeDistribution> ranges) {
        return ranges.size();
    }

    @Override
    public CommonId calcPartId(byte [] key, NavigableMap<ComparableByteArray, RangeDistribution> ranges) {
        return ranges.floorEntry(new ComparableByteArray(key, 1)).getValue().id();
    }

    @Override
    public NavigableSet<RangeDistribution> calcPartitionRange(
        byte [] startKey,
        byte [] endKey,
        boolean withStart,
        boolean withEnd,
        NavigableMap<ComparableByteArray, RangeDistribution> ranges
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
        NavigableSet<RangeDistribution> subRanges = RangeUtils.getSubRangeDistribution(ranges.values(), range, 1);
        subRanges.descendingSet().stream().skip(1).forEach(rd -> {
            if (Arrays.equals(rd.getEndKey(), ranges.firstKey().getBytes())) {
                rd.setWithEnd(true);
            }
        });
        return subRanges;
    }

}
