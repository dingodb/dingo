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

package io.dingodb.common.util;

import io.dingodb.common.partition.RangeDistribution;

import java.util.Collection;
import java.util.Comparator;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.dingodb.common.util.ByteArrayUtils.compareWithoutLen;
import static io.dingodb.common.util.ByteArrayUtils.greatThan;

public class RangeUtils {

    public static Comparator<RangeDistribution> rangeComparator() {
        return (r1, r2) -> ByteArrayUtils.compare(r1.getStartKey(), r2.getStartKey());
    }

    public static NavigableSet<RangeDistribution> getSubRangeDistribution(
        Collection<RangeDistribution> src, RangeDistribution range
    ) {
        NavigableSet<RangeDistribution> rangeSet = new TreeSet<>(rangeComparator());
        rangeSet.addAll(src);
        byte[] rangeStart = range.getStartKey();
        byte[] rangeEnd = range.getEndKey();
        NavigableSet<RangeDistribution> subRanges = new TreeSet<>(rangeComparator());
        Predicate<byte[]> filter = (k) ->
            greatThan(rangeEnd, k) || (compareWithoutLen(rangeEnd, k) == 0 && range.isWithEnd())
        ;
        Function<RangeDistribution, byte[]> keyGetter = RangeDistribution::getStartKey;

        for (RangeDistribution rd : rangeSet.descendingSet()) {
            if (filter.test(keyGetter.apply(rd))) {
                if (subRanges.isEmpty()) {
                    filter = k ->
                        !(greatThan(rangeStart, k) || compareWithoutLen(rangeStart, k) == 0 && !range.isWithStart())
                    ;
                    keyGetter = RangeDistribution::getEndKey;
                }
                subRanges.add(new RangeDistribution(
                    rd.getId(),
                    rd.getStartKey(), rd.getEndKey(), rd.isWithStart(), rd.isWithEnd()
                ));
            }
        }

        if (!subRanges.isEmpty()) {
            subRanges.first().setStartKey(rangeStart);
            subRanges.first().setWithStart(range.isWithStart());

            subRanges.last().setEndKey(rangeEnd);
            subRanges.last().setWithEnd(range.isWithEnd());
        }

        return subRanges;
    }

}
