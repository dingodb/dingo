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

package io.dingodb.store.common;

import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.Range;
import io.dingodb.sdk.common.RangeWithOptions;
import io.dingodb.sdk.common.SDKCommonId;
import io.dingodb.sdk.common.partition.Partition;
import io.dingodb.sdk.common.partition.PartitionDetail;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.store.api.StoreInstance;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public final class Mapping {

    private Mapping() {
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
            .isAutoIncrement(definition.isAutoIncrement())
            .state(definition.getState())
            .createVersion(definition.getCreateVersion())
            .updateVersion(definition.getUpdateVersion())
            .deleteVersion(definition.getDeleteVersion())
            .build();
    }

    public static TableDefinition mapping(Table table) {
        Properties properties = new Properties();
        Map<String, String> map = table.getProperties();
        if (map != null) {
            properties.putAll(map);
        }

        return new TableDefinition(
            table.getName(),
            table.getColumns().stream().map(Mapping::mapping).collect(Collectors.toList()),
            null,
            table.getVersion(),
            table.getTtl(),
            mapping(table.getPartition()),
            table.getEngine(),
            properties,
            table.getAutoIncrement(),
            table.getReplica(),
            table.getCreateSql());
    }

    public static io.dingodb.store.common.TableDefinition mapping(TableDefinition tableDefinition) {
        return new io.dingodb.store.common.TableDefinition(tableDefinition);
    }

    public static PartitionDefinition mapping(Partition partition) {
        if (partition == null) {
            return null;
        }
        return new PartitionDefinition(
            partition.getFuncName(),
            partition.getCols(),
            partition.getDetails().stream().map(Mapping::mapping).collect(Collectors.toList()));
    }

    public static RangeDistribution mapping(
        io.dingodb.sdk.common.table.RangeDistribution rangeDistribution,
        KeyValueCodec codec, boolean isOriginalKey
    ) {
        try {
            byte[] startKey = rangeDistribution.getRange().getStartKey();
            byte[] endKey = rangeDistribution.getRange().getEndKey();
            Object[] start = cleanOperand(
                codec.decodeKeyPrefix(isOriginalKey ? Arrays.copyOf(startKey, startKey.length) : startKey)
            );
            Object[] end = cleanOperand(
                codec.decodeKeyPrefix(isOriginalKey ? Arrays.copyOf(endKey, endKey.length) : endKey)
            );

            return RangeDistribution.builder()
                .id(mapping(rangeDistribution.getId()))
                .startKey(startKey)
                .endKey(endKey)
                .start(start)
                .end(end)
                .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static io.dingodb.common.partition.PartitionDetailDefinition mapping(PartitionDetail partitionDetail) {
        Object[] operand = partitionDetail.getOperand();
        operand = cleanOperand(operand);
        return new io.dingodb.common.partition.PartitionDetailDefinition(
            partitionDetail.getPartName(),
            partitionDetail.getOperator(),
            operand);
    }

    @NonNull
    private static Object[] cleanOperand(Object[] operand) {
        if (operand.length > 0) {
            for (int i = (operand.length - 1); i >= 0; --i) {
                if (operand[i] != null) {
                    operand = Arrays.copyOf(operand, i + 1);
                }
            }
        }
        if (operand.length > 0 && operand[operand.length - 1] == null) {
            operand = new Object[0];
        }
        return operand;
    }

    public static PartitionDetailDefinition mapping(io.dingodb.common.partition.PartitionDetailDefinition partitionDetail) {
        return new PartitionDetailDefinition(partitionDetail);
    }

    public static ColumnDefinition mapping(Column column) {
        return ColumnDefinition.builder()
            .name(column.getName())
            .type(column.getType())
            .elementType(column.getElementType())
            .precision(column.getPrecision())
            .scale(column.getScale())
            .nullable(column.isNullable())
            .primary(column.getPrimary())
            .defaultValue(column.getDefaultValue())
            .autoIncrement(column.isAutoIncrement())
            .state(column.getState())
            .createVersion(column.getCreateVersion())
            .updateVersion(column.getUpdateVersion())
            .deleteVersion(column.getDeleteVersion())
            .build();
    }

    public static DingoCommonId mapping(CommonId commonId) {
        //return new io.dingodb.server.executor.common.DingoCommonId(commonId);
        return new SDKCommonId(DingoCommonId.Type.values()[commonId.type.code], commonId.domain, commonId.seq);
    }

    public static CommonId mapping(DingoCommonId commonId) {
        return new CommonId(
            CommonId.CommonType.of(commonId.type().ordinal()),
            (int) commonId.parentId(),
            (int) commonId.entityId());
    }

    public static io.dingodb.sdk.common.KeyValue mapping(KeyValue keyValue) {
        return new io.dingodb.sdk.common.KeyValue(keyValue.getKey(), keyValue.getValue());
    }

    public static KeyValue mapping(io.dingodb.sdk.common.KeyValue keyValue) {
        return new KeyValue(keyValue.getKey(), keyValue.getValue());
    }

    public static RangeWithOptions mapping(StoreInstance.Range range) {
        return new RangeWithOptions(new Range(range.start, range.end), range.withStart, range.withEnd);
    }

}
