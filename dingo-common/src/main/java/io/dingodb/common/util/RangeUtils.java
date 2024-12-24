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

import static io.dingodb.common.util.ByteArrayUtils.SKIP_LONG_POS;
import static io.dingodb.common.util.ByteArrayUtils.compareWithoutLen;
import static io.dingodb.common.util.ByteArrayUtils.equal;
import static io.dingodb.common.util.ByteArrayUtils.greatThan;

public class RangeUtils {

    public static Comparator<RangeDistribution> rangeComparator() {
        return (r1, r2) -> ByteArrayUtils.compare(r1.getStartKey(), r2.getStartKey(), SKIP_LONG_POS);
    }

    public static Comparator<RangeDistribution> rangeComparator(int pos) {
        return (r1, r2) -> ByteArrayUtils.compare(r1.getStartKey(), r2.getStartKey(), pos);
    }

    public static NavigableSet<RangeDistribution> getSubRangeDistribution(
        Collection<RangeDistribution> src, RangeDistribution range
    ) {
        return getSubRangeDistribution(src, range, SKIP_LONG_POS);
    }

    public static NavigableSet<RangeDistribution> getSubRangeDistribution(
        Collection<RangeDistribution> src, RangeDistribution range, int pos
    ) {
        NavigableSet<RangeDistribution> rangeSet = new TreeSet<>(rangeComparator(pos));
        rangeSet.addAll(src);
        byte[] rangeStart = range.getStartKey();
        byte[] rangeEnd = range.getEndKey();
        NavigableSet<RangeDistribution> subRanges = new TreeSet<>(rangeComparator(pos));
        Predicate<byte[]> filter = __ -> checkEndIn(rangeEnd, __, range.isWithEnd(), pos);
        Function<RangeDistribution, byte[]> keyGetter = RangeDistribution::getStartKey;

        for (RangeDistribution rd : rangeSet.descendingSet()) {
            if (filter.test(keyGetter.apply(rd))) {
                if (subRanges.isEmpty()) {
                    filter = __ -> checkStartIn(rangeStart, __, range.isWithStart(), pos);
                }
                keyGetter = __ -> rd.getStartKey();
                subRanges.add(
                    RangeDistribution.builder()
                        .id(rd.getId())
                        .startKey(rd.getStartKey())
                        .endKey(rd.getEndKey())
                        .withStart(rd.isWithStart())
                        .withEnd(rd.isWithEnd())
                        .build()
                );
            }
        }

        if (!subRanges.isEmpty()) {
            subRanges.first().setStartKey(rangeStart);
            subRanges.first().setWithStart(range.isWithStart());

            if (subRanges.last().getEndKey().length == rangeEnd.length
                && !greatThan(subRanges.last().getEndKey(), rangeEnd, pos)) {
                // case1: create table t(id int,age int,primary key(id)); select * from tab
                // rangeEnd: 116 0 0 0 0 0 0 0 0  subRange.last.endKey: 116 0 0 0 0 0 0 0 0
                // When the specific range of the region is not specified, startKey and endKey are both empty,
                // and the specific encoding is t-partId (8). In this case, withStart=true, withEnd = true
                // case2: create table t(id int,age int,primary key(id)) partition by range values(0),(20),(30)
                // partition 4 (Infinity, 0), (0, 20), (20,30), (30, Infinity) -> 4 regions
                // range example: 74000000000000eaad80000fa0() - 74000000000000eaae()
                //  alter table add distribution by values(10)
                // select * from t where id<=10 (or id < 15)
                // case3: dependency case 2. select * from t where id<=15

                // Add restrictive conditions:
                // If subRanges.last.end key has a specific specified range except for the first digit
                // and partId (the first 9 digits), and this range is the endKey of a certain region,
                // it is not allowed to set withEnd=true
                // case 4: dependency case2. alter table t add partition [pName] values less than (10)
                // partition 5 (Infinity, 0), (0, 10),(10,20), (20,30), (30, Infinity)
                if (rangeEnd.length > 9 && equal(subRanges.last().getEndKey(), rangeEnd)) {
                    subRanges.last().setEndKey(rangeEnd);
                    subRanges.last().setWithEnd(range.isWithEnd());
                } else {
                    subRanges.last().setWithEnd(true);
                }
            } else {
                subRanges.last().setEndKey(rangeEnd);
                subRanges.last().setWithEnd(range.isWithEnd());
            }
        }

        return subRanges;
    }

    private static boolean checkStartIn(byte[] rangeStart, byte[] regionEnd, boolean withStart, int pos) {
        return compareWithoutLen(rangeStart, regionEnd, pos) < 0
            || (withStart && rangeStart.length != regionEnd.length && compareWithoutLen(rangeStart, regionEnd, pos) == 0);
    }

    private static boolean checkEndIn(byte[] rangeEnd, byte[] regionStart, boolean withEnd, int pos) {
        return greatThan(rangeEnd, regionStart, pos) || (compareWithoutLen(rangeEnd, regionStart, pos) == 0 && withEnd);
    }

}
