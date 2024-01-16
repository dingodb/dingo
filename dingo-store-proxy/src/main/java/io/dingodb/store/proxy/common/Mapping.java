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

package io.dingodb.store.proxy.common;

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

import java.util.Arrays;
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
            .state(definition.getState())
            .build();
    }

    public static Column mapping(io.dingodb.meta.entity.Column definition) {
        return io.dingodb.sdk.common.table.ColumnDefinition.builder()
            .name(definition.getName())
            .type(definition.getSqlTypeName())
            .elementType(definition.getElementTypeName())
            .precision(definition.getPrecision())
            .scale(definition.getScale())
            .nullable(definition.isNullable())
            .primary(definition.getPrimaryKeyIndex())
            .defaultValue(definition.getDefaultValueExpr())
            .state(definition.getState())
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
            table.getVersion(),
            table.getTtl(),
            mapping(table.getPartition()),
            table.getEngine(),
            properties,
            table.getAutoIncrement(),
            table.getReplica(),
            table.getCreateSql(),
            table.getComment(),
            table.getCharset(),
            table.getCollate(),
            table.getTableType(),
            table.getRowFormat(),
            table.getCreateTime(),
            table.getUpdateTime());
    }

    public static io.dingodb.store.proxy.common.TableDefinition mapping(TableDefinition tableDefinition) {
        return new io.dingodb.store.proxy.common.TableDefinition(tableDefinition);
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
        byte[] startKey = rangeDistribution.getRange().getStartKey();
        byte[] endKey = rangeDistribution.getRange().getEndKey();
        return RangeDistribution.builder()
            .id(mapping(rangeDistribution.getId()))
            .startKey(startKey)
            .endKey(endKey)
            .start(codec.decodeKeyPrefix(isOriginalKey ? Arrays.copyOf(startKey, startKey.length) : startKey))
            .end(codec.decodeKeyPrefix(isOriginalKey ? Arrays.copyOf(endKey, endKey.length) : endKey))
            .build();
    }

    public static io.dingodb.common.partition.PartitionDetailDefinition mapping(PartitionDetail partitionDetail) {
        return new io.dingodb.common.partition.PartitionDetailDefinition(
            partitionDetail.getPartName(),
            partitionDetail.getOperator(),
            partitionDetail.getOperand());
    }


    public static RangeDistribution mapping(
        io.dingodb.sdk.common.table.RangeDistribution rangeDistribution,
        KeyValueCodec codec
    ) {
        byte[] startKey = rangeDistribution.getRange().getStartKey();
        byte[] endKey = rangeDistribution.getRange().getEndKey();
        return RangeDistribution.builder()
            .id(mapping(rangeDistribution.getId()))
            .startKey(startKey)
            .endKey(endKey)
            .start(codec.decodeKeyPrefix(startKey))
            .end(codec.decodeKeyPrefix(endKey))
            .build();
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
