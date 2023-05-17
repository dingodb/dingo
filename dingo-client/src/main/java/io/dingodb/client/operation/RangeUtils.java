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

package io.dingodb.client.operation;

import io.dingodb.client.common.RouteTable;
import io.dingodb.client.operation.impl.Operation;
import io.dingodb.sdk.common.Range;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.client.operation.impl.OpRange;
import io.dingodb.sdk.common.utils.ByteArrayUtils;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.dingodb.sdk.common.utils.Any.wrap;
import static io.dingodb.sdk.common.utils.ByteArrayUtils.compareWithoutLen;
import static io.dingodb.sdk.common.utils.ByteArrayUtils.greatThan;

public class RangeUtils {

    public static boolean rangeCheck(List<Object> startKey, List<Object> endKey, OpRange range) {
        return compareWithoutLen(range.getStartKey(), range.getEndKey()) > 0
            || (Arrays.equals(range.getStartKey(), range.getEndKey())) && (!range.withEnd || !range.withStart)
            || (!range.withStart && startKey.size() == 0)
            || (!range.withEnd && endKey.size() == 0);
    }

    public static Comparator<Operation.Task> getComparator() {
        return (e1, e2) -> ByteArrayUtils.compare(e1.<OpRange>parameters().getStartKey(), e2.<OpRange>parameters().getStartKey());
    }

    public static NavigableSet<Operation.Task> getSubTasks(RouteTable routeTable, OpRange range) {
        NavigableSet<Operation.Task> subTasks = new TreeSet<>(getComparator());
        Predicate<byte[]> filter = (k) -> greatThan(range.getEndKey(), k) || (compareWithoutLen(range.getEndKey(), k) == 0 && range.withEnd);
        Function<Range, byte[]> keyGetter = Range::getStartKey;
        for (RangeDistribution rd : routeTable.getRangeDistribution().descendingMap().values()) {
            if (filter.test(keyGetter.apply(rd.getRange()))) {
                if (subTasks.isEmpty()) {
                    filter = k -> !(greatThan(range.getStartKey(), k) || compareWithoutLen(range.getStartKey(), k) == 0 && !range.withStart);
                    keyGetter = Range::getEndKey;
                }
                subTasks.add(new Operation.Task(
                    rd.getId(),
                    wrap(new OpRange(rd.getRange().getStartKey(), rd.getRange().getEndKey(), true, false))
                ));
            }
        }

        return subTasks;
    }
}
