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
import io.dingodb.client.operation.impl.OpKeyRange;
import io.dingodb.client.operation.impl.OpRange;
import io.dingodb.client.operation.impl.Operation;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.SDKCommonId;
import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.ByteArrayUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static io.dingodb.client.utils.OperationUtils.mapKeyPrefix;
import static io.dingodb.sdk.common.utils.Any.wrap;
import static io.dingodb.sdk.common.utils.ByteArrayUtils.lessThanOrEqual;

public class RangeUtils {

    public static boolean validateKeyRange(OpKeyRange keyRange) {
        return (!keyRange.getStart().userKey.isEmpty() || keyRange.withStart)
            && (!keyRange.getEnd().userKey.isEmpty() || keyRange.withEnd);
    }

    public static boolean validateOpRange(OpRange range) {
        return lessThanOrEqual(range.getStartKey(), range.getEndKey());
    }

    public static OpRange convert(KeyValueCodec codec, Table table, OpKeyRange keyRange) throws IOException {
        Object[] startKey = mapKeyPrefix(table, keyRange.start);
        Object[] endKey = mapKeyPrefix(table, keyRange.end);
        return new OpRange(
            codec.encodeKeyPrefix(startKey, keyRange.start.userKey.size()),
            codec.encodeKeyPrefix(endKey, keyRange.end.userKey.size()),
            keyRange.withStart,
            keyRange.withEnd
        );
    }

    public static Comparator<Operation.Task> getComparator() {
        return (e1, e2) -> ByteArrayUtils.compare(e1.<OpRange>parameters().getStartKey(), e2.<OpRange>parameters().getStartKey());
    }

    public static NavigableSet<Operation.Task> getSubTasks(RouteTable routeTable, OpRange range) {
        Collection<RangeDistribution> src = routeTable.rangeDistribution.values().stream()
            .map(RangeUtils::mapping)
            .collect(Collectors.toSet());
        RangeDistribution rangeDistribution = new RangeDistribution(
            mapping(routeTable.tableId), range.getStartKey(), range.getEndKey(), range.withStart, range.withEnd);

        return io.dingodb.common.util.RangeUtils.getSubRangeDistribution(src, rangeDistribution).stream()
            .map(rd -> new Operation.Task(
                mapping(rd.id()),
                wrap(new OpRange(rd.getStartKey(), rd.getEndKey(), rd.isWithStart(), rd.isWithEnd()))
            ))
            .collect(Collectors.toCollection(() -> new TreeSet<>(getComparator())));
    }

    public static CommonId mapping(DingoCommonId commonId) {
        return new CommonId(
            CommonId.CommonType.of(commonId.type().ordinal()),
            (int) commonId.parentId(),
            (int) commonId.entityId());
    }

    public static DingoCommonId mapping(CommonId commonId) {
        return new SDKCommonId(DingoCommonId.Type.values()[commonId.type.code], commonId.domain, commonId.seq);
    }

    public static RangeDistribution mapping(io.dingodb.sdk.common.table.RangeDistribution rangeDistribution) {
        return new RangeDistribution(
            mapping(rangeDistribution.getId()),
            rangeDistribution.getRange().getStartKey(),
            rangeDistribution.getRange().getEndKey()
        );
    }
}
