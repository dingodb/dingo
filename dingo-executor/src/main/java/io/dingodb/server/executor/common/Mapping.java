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

package io.dingodb.server.executor.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.expr.json.runtime.Parser;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.SDKCommonId;
import io.dingodb.sdk.common.partition.Partition;
import io.dingodb.sdk.common.partition.PartitionDetail;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.Table;

import java.util.stream.Collectors;

public final class Mapping {

    private Mapping() {
    }

    public static Table mapping(io.dingodb.common.table.TableDefinition tableDefinition) {
        return new TableDefinition(tableDefinition);
    }

    public static io.dingodb.common.table.TableDefinition mapping(Table table) {
        return new io.dingodb.common.table.TableDefinition(
            table.getName(),
            table.getColumns().stream().map(Mapping::mapping).collect(Collectors.toList()),
            null,
            table.getVersion(),
            table.getTtl(),
            mapping(table.getPartDefinition()),
            table.getEngine(),
            null);
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
            column.getDefaultValue());
    }

    public static PartitionDefinition mapping(Partition partition) {
        if (partition == null) {
            return null;
        }
        return new PartitionDefinition(
            partition.strategy(),
            partition.cols(),
            partition.details().stream().map(Mapping::mapping).collect(Collectors.toList()));
    }

    public static RangeDistribution mapping(io.dingodb.sdk.common.table.RangeDistribution rangeDistribution) {
        return new RangeDistribution(
            mapping(rangeDistribution.getId()),
            rangeDistribution.getRange().getStartKey(),
            rangeDistribution.getRange().getEndKey()
        );
    }

    public static io.dingodb.common.partition.PartitionDetailDefinition mapping(PartitionDetail partitionDetail) {
        return new io.dingodb.common.partition.PartitionDetailDefinition(
            partitionDetail.getPartName(),
            partitionDetail.getOperator(),
            partitionDetail.getOperand());
    }

    public static CommonId mapping(DingoCommonId commonId) {
        return new CommonId(
            CommonId.CommonType.of(commonId.type().ordinal()),
            (int) commonId.parentId(),
            (int) commonId.entityId());
    }

    public static DingoCommonId mapping(CommonId commonId) {
        //return new io.dingodb.server.executor.common.DingoCommonId(commonId);
        return new SDKCommonId(DingoCommonId.Type.values()[commonId.type.code], commonId.domain, commonId.seq);
    }

    public static io.dingodb.sdk.common.KeyValue mapping(KeyValue keyValue) {
        return new io.dingodb.sdk.common.KeyValue(keyValue.getKey(), keyValue.getValue());
    }

    public static KeyValue mapping(io.dingodb.sdk.common.KeyValue keyValue) {
        return new KeyValue(keyValue.getKey(), keyValue.getValue());
    }

    public static DingoType mapping(io.dingodb.sdk.common.type.DingoType type) {
        //todo need optimize
        try {
            return Parser.JSON.parse(Parser.JSON.stringify(type), DingoType.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static io.dingodb.sdk.common.type.DingoType mapping(DingoType type) {
        //todo need optimize
        try {
            return Parser.JSON.parse(Parser.JSON.stringify(type), io.dingodb.sdk.common.type.DingoType.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
