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

import io.dingodb.client.common.TableInfo;
import io.dingodb.client.operation.impl.KeyRangeCoprocessor;
import io.dingodb.client.operation.impl.OpKeyRange;
import io.dingodb.client.operation.impl.OpRange;
import io.dingodb.client.operation.impl.OpRangeCoprocessor;
import io.dingodb.client.operation.impl.Operation;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.util.Optional;
import io.dingodb.partition.base.ConsistentHashing;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.SDKCommonId;
import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.common.partition.Partition;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.ByteArrayUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static io.dingodb.client.utils.OperationUtils.mapKeyPrefix;
import static io.dingodb.common.util.ByteArrayUtils.SKIP_LONG_POS;
import static io.dingodb.sdk.common.utils.Any.wrap;
import static io.dingodb.sdk.common.utils.ByteArrayUtils.lessThanOrEqual;

@Slf4j
public class RangeUtils {

    public static final String RANGE_FUNC_NAME = "RANGE";
    public static final String HASH_FUNC_NAME = "HASH";

    public static DingoCommonId getDingoCommonId(
        byte[] key, String strategy,
        NavigableMap<ByteArrayUtils.ComparableByteArray, io.dingodb.sdk.common.table.RangeDistribution> distribution
    ) {
        DingoCommonId commonId;
        switch (strategy) {
            case RANGE_FUNC_NAME:
                // skip the first 8 bytes when comparing byte[] (id)
                commonId = distribution.floorEntry(
                    new ByteArrayUtils.ComparableByteArray(key, SKIP_LONG_POS)).getValue().getId();
                break;
            case HASH_FUNC_NAME:
                ConsistentHashing<Long> hashRing = new ConsistentHashing<>(3);
                NavigableMap<ByteArrayUtils.ComparableByteArray,
                    io.dingodb.sdk.common.table.RangeDistribution> partRanges = new TreeMap<>();
                for (Map.Entry<ByteArrayUtils.ComparableByteArray,
                    io.dingodb.sdk.common.table.RangeDistribution> entry : distribution.entrySet()) {
                    io.dingodb.sdk.common.table.RangeDistribution value = entry.getValue();
                    log.trace("entityId:" + value.getId().entityId() + ",parentId:" + value.getId().parentId());
                    hashRing.addNode(value.getId().parentId());
                }
                Long selectNode = hashRing.getNode(key);
                for (Map.Entry<ByteArrayUtils.ComparableByteArray,
                    io.dingodb.sdk.common.table.RangeDistribution> entry : distribution.entrySet()) {
                    ByteArrayUtils.ComparableByteArray keyBytes = entry.getKey();
                    io.dingodb.sdk.common.table.RangeDistribution value = entry.getValue();
                    if (value.getId().parentId() == selectNode.longValue()) {
                        partRanges.put(keyBytes, value);
                    }
                }
                commonId = partRanges.floorEntry(
                    new ByteArrayUtils.ComparableByteArray(key, SKIP_LONG_POS)).getValue().getId();
                break;
            default:
                throw new IllegalStateException("Unsupported " + strategy);
        }
        return commonId;
    }

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
        return getComparator(SKIP_LONG_POS);
    }

    public static Comparator<Operation.Task> getComparator(int pos) {
        return (e1, e2) -> ByteArrayUtils.compare(
            e1.<OpRange>parameters().getStartKey(),
            e2.<OpRange>parameters().getStartKey(),
            pos
        );
    }

    public static NavigableSet<Operation.Task> getSubTasks(TableInfo tableInfo, OpRange range) {
        return getSubTasks(tableInfo, range, null);
    }

    public static NavigableSet<Operation.Task> getSubTasks(
        TableInfo tableInfo,
        OpRange range,
        Coprocessor coprocessor
    ) {
        Collection<RangeDistribution> src = tableInfo.rangeDistribution.values().stream()
            .map(RangeUtils::mapping)
            .collect(Collectors.toSet());
        RangeDistribution rangeDistribution = RangeDistribution.builder()
            .id(mapping(tableInfo.tableId))
            .startKey(range.getStartKey())
            .endKey(range.getEndKey())
            .withStart(range.withStart)
            .withEnd(range.withEnd)
            .build();

        NavigableSet<RangeDistribution> distributions;
        final int pos;
        if (Optional.of(tableInfo.definition.getPartition())
            .map(Partition::getFuncName)
            .filter(f -> !f.equalsIgnoreCase(HASH_FUNC_NAME))
            .isPresent()
        ) {
            pos = SKIP_LONG_POS;
            distributions = io.dingodb.common.util.RangeUtils.getSubRangeDistribution(src, rangeDistribution);
            if (!distributions.isEmpty()) {
                RangeDistribution last = distributions.last();
                last.setEndKey(tableInfo.codec.resetPrefix(last.getEndKey(), last.getId().domain));
            }
        } else {
            pos = 0;
            distributions = new TreeSet<>(io.dingodb.common.util.RangeUtils.rangeComparator(pos));
            Map<Long, List<RangeDistribution>> groupedMap = src.stream()
                .collect(Collectors.groupingBy(rd -> rd.getId().domain));
            for (Map.Entry<Long, List<RangeDistribution>> entry : groupedMap.entrySet()) {
                NavigableSet<RangeDistribution> distribution =
                    io.dingodb.common.util.RangeUtils.getSubRangeDistribution(entry.getValue(), rangeDistribution);
                distribution.stream()
                    .peek($ -> $.setStartKey(tableInfo.codec.resetPrefix($.getStartKey(), entry.getKey())))
                    .forEach($ -> $.setEndKey(tableInfo.codec.resetPrefix($.getEndKey(), entry.getKey())));
                if (!distribution.isEmpty()) {
                    RangeDistribution last = distribution.last();
                    last.setEndKey(tableInfo.codec.resetPrefix(last.getEndKey(), entry.getKey()));
                }
                distributions.addAll(distribution);
            }
        }

        if (coprocessor == null) {
            return distributions.stream()
                .map(rd -> new Operation.Task(
                    mapping(rd.id()),
                    wrap(new OpRange(rd.getStartKey(), rd.getEndKey(), rd.isWithStart(), rd.isWithEnd()))
                ))
                .collect(Collectors.toCollection(() -> new TreeSet<>(getComparator(pos))));
        } else {
            return distributions.stream()
                .map(rd -> new Operation.Task(
                    mapping(rd.id()),
                    wrap(new OpRangeCoprocessor(
                        rd.getStartKey(),
                        rd.getEndKey(),
                        rd.isWithStart(),
                        rd.isWithEnd(),
                        coprocessor))
                ))
                .collect(Collectors.toCollection(() -> new TreeSet<>(getComparator(pos))));
        }
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
        return RangeDistribution.builder()
            .id(mapping(rangeDistribution.getId()))
            .startKey(rangeDistribution.getRange().getStartKey())
            .endKey(rangeDistribution.getRange().getEndKey())
            .build();
    }

    public static Coprocessor.AggregationOperator mapping(KeyRangeCoprocessor.Aggregation aggregation, Table table) {
        return new Coprocessor.AggregationOperator(aggregation.operation, table.getColumnIndex(aggregation.columnName));
    }

    public static ColumnDefinition mapping(Column column) {
        return ColumnDefinition.getInstance(
            column.getName(),
            column.getType().equals("STRING") ? "VARCHAR" : column.getType(),
            column.getElementType(),
            column.getPrecision(),
            column.getScale(),
            column.isNullable(),
            column.getPrimary(),
            column.getDefaultValue(),
            column.isAutoIncrement(),
            column.getState());
    }

    public static Column mapping(ColumnDefinition definition) {
        return io.dingodb.sdk.common.table.ColumnDefinition.builder()
            .name(definition.getName())
            .type(definition.getTypeName())
            .elementType(definition.getElementType())
            .precision(definition.getPrecision())
            .scale(definition.getScale())
            .nullable(definition.isNullable())
            .primary(definition.getPrimary())
            .defaultValue(definition.getDefaultValue())
            .build();
    }
}
