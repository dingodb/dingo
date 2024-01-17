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

import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.converter.DataConverter;
import io.dingodb.common.type.converter.DingoConverter;
import io.dingodb.common.type.converter.StrParseConverter;
import io.dingodb.common.type.scalar.TimeType;

import java.sql.Time;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.common.util.Optional.mapOrNull;

public final class DefinitionUtils {

    private DefinitionUtils() {
    }

    public static void checkAndConvertRangePartition(
        List<String> keyNames,
        List<String> partitionBy,
        List<DingoType> keyTypes,
        List<Object[]> details
    ) {
        DataConverter fromConverter = StrParseConverter.INSTANCE;
        DataConverter toConverter = DingoConverter.INSTANCE;
        if (partitionBy == null || partitionBy.isEmpty()) {
            partitionBy = keyNames;
        } else {
            partitionBy = partitionBy.stream().map(String::toUpperCase).collect(Collectors.toList());
        }

        if (!keyNames.equals(partitionBy)) {
            throw new IllegalArgumentException(
                "Partition columns must be equals primary key columns, but " + partitionBy
            );
        }

        for (Object[] operand : details) {
            if (operand.length > keyNames.size()) {
                throw new IllegalArgumentException(
                    "Partition values count must be <= key columns count, but values count is " + operand.length
                );
            }
            for (int i = 0; i < operand.length; i++) {
                DingoType type = keyTypes.get(i);
                if (type instanceof TimeType) {
                   operand[i] = Time.valueOf(operand[i].toString()).getTime();
                } else {
                    operand[i] = mapOrNull(
                        operand[i],
                        v -> type.convertTo(type.convertFrom(v.toString(), fromConverter), toConverter)
                    );
                }
            }
        }
    }

    public static void checkAndConvertHashRangePartition(
        List<String> keyNames,
        List<String> partitionBy
    ) {
        if (partitionBy == null || partitionBy.isEmpty()) {
            partitionBy = keyNames;
        } else {
            partitionBy = partitionBy.stream().map(String::toUpperCase).collect(Collectors.toList());
        }

        if (!keyNames.equals(partitionBy)) {
            throw new IllegalArgumentException(
                "Partition columns must be equals primary key columns, but " + partitionBy
            );
        }
    }

    public static void checkAndConvertRangePartition(TableDefinition tableDefinition) {
        List<ColumnDefinition> keyColumns = tableDefinition.getKeyColumns();
        PartitionDefinition partDefinition = tableDefinition.getPartDefinition();
        keyColumns.sort(Comparator.comparingInt(ColumnDefinition::getPrimary));
        checkAndConvertRangePartition(
            keyColumns.stream().map(ColumnDefinition::getName).collect(Collectors.toList()),
            partDefinition.getColumns(),
            keyColumns.stream().map(ColumnDefinition::getType).collect(Collectors.toList()),
            partDefinition.getDetails().stream().map(PartitionDetailDefinition::getOperand).collect(Collectors.toList())
        );
    }

    public static void checkAndConvertHashRangePartition(TableDefinition tableDefinition) {
        List<ColumnDefinition> keyColumns = tableDefinition.getKeyColumns();
        PartitionDefinition partDefinition = tableDefinition.getPartDefinition();
        keyColumns.sort(Comparator.comparingInt(ColumnDefinition::getPrimary));
        checkAndConvertHashRangePartition(
            keyColumns.stream().map(ColumnDefinition::getName).collect(Collectors.toList()),
            partDefinition.getColumns()
        );
        List<PartitionDetailDefinition> details = partDefinition.getDetails();
        if(details.size() != 1) {
            throw new IllegalArgumentException(
                "Partition values count must be 1, but values count is " + details.size()
            );
        }
        PartitionDetailDefinition partitionDetailDefinition = details.get(0);
        Object[] operand = partitionDetailDefinition.getOperand();
        if (operand.length != 1) {
            throw new IllegalArgumentException(
                "Partition values count must be 1, but values count is " + operand.length
            );
        }
        Object ob = operand[0];
        DataConverter fromConverter = StrParseConverter.INSTANCE;
        DataConverter toConverter = DingoConverter.INSTANCE;
        DingoType type = DingoTypeFactory.INSTANCE.scalar("INT");
        ob = mapOrNull(
            ob,
            v -> type.convertTo(type.convertFrom(v.toString(), fromConverter), toConverter)
        );
        int hashNum = (int) ob;
        if(hashNum <= 0) {
            throw new IllegalArgumentException(
                "Partition values count must be non-negative number, but values count is " + hashNum
            );
        }
        List<PartitionDetailDefinition> newDetails = new ArrayList<>(hashNum);
        for(int i = 0; i < hashNum; i++) {
            newDetails.add(new PartitionDetailDefinition(null, null, new Object[0]));
        }
        partDefinition.setDetails(newDetails);
    }

    public static void checkAndConvertRangePartitionDetail(
        TableDefinition tableDefinition, PartitionDetailDefinition detail
    ) {
        List<ColumnDefinition> keyColumns = tableDefinition.getKeyColumns();
        keyColumns.sort(Comparator.comparingInt(ColumnDefinition::getPrimary));
        checkAndConvertRangePartition(
            keyColumns.stream().map(ColumnDefinition::getName).collect(Collectors.toList()),
            Collections.emptyList(),
            keyColumns.stream().map(ColumnDefinition::getType).collect(Collectors.toList()),
            Collections.singletonList(detail.getOperand())
        );
    }

}
