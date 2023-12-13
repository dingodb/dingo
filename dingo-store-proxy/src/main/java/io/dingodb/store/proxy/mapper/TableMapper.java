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

package io.dingodb.store.proxy.mapper;

import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.Column;
import io.dingodb.meta.Table;
import io.dingodb.sdk.common.serial.RecordEncoder;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.service.entity.common.Range;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.Partition;
import io.dingodb.sdk.service.entity.meta.PartitionRule;
import io.dingodb.sdk.service.entity.meta.PartitionStrategy;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
import io.dingodb.sdk.service.entity.meta.TableIdWithPartIds;
import io.dingodb.store.proxy.service.CodecService;
import lombok.SneakyThrows;
import org.mapstruct.Mapping;
import org.mapstruct.MappingTarget;
import org.mapstruct.Mappings;
import org.mapstruct.ap.internal.util.Strings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.partition.DingoPartitionServiceProvider.HASH_FUNC_NAME;
import static io.dingodb.partition.DingoPartitionServiceProvider.RANGE_FUNC_NAME;
import static io.dingodb.sdk.service.entity.meta.PartitionStrategy.PT_STRATEGY_HASH;
import static io.dingodb.sdk.service.entity.meta.PartitionStrategy.PT_STRATEGY_RANGE;
import static io.dingodb.store.proxy.mapper.Mapper.MAPPER;

public interface TableMapper {

    @SneakyThrows
    default PartitionStrategy toPartitionStrategy(String partitionStrategy) {
        return Optional.ofNullable(partitionStrategy)
            .filter(Strings::isNotEmpty)
            .filter(s -> s.equalsIgnoreCase(HASH_FUNC_NAME))
            .map(s -> PT_STRATEGY_HASH)
            .orElse(PT_STRATEGY_RANGE);
    }

    default String fromPartitionStrategy(PartitionStrategy partitionStrategy) {
        return partitionStrategy == PT_STRATEGY_HASH ? HASH_FUNC_NAME : RANGE_FUNC_NAME;
    }

    default DingoType typeFrom(io.dingodb.sdk.service.entity.meta.ColumnDefinition cd) {
        return DingoTypeFactory.INSTANCE.fromName(cd.getName(), cd.getElementType(), cd.isNullable());
    }

    @Mappings({
        @Mapping(source = "columnDefinition", target = "type"),
        @Mapping(source = "indexOfKey", target = "primaryKeyIndex"),
        @Mapping(source = "autoIncrement", target = "autoIncrement"),
        @Mapping(source = "defaultVal", target = "defaultValueExpr"),
    })
    Column columnFrom(io.dingodb.sdk.service.entity.meta.ColumnDefinition columnDefinition);

    List<Column> columnsFrom(List<io.dingodb.sdk.service.entity.meta.ColumnDefinition> columnDefinitions);

    @Mappings({
        @Mapping(source = "typeName", target = "sqlType"),
        @Mapping(source = "primary", target = "indexOfKey"),
        @Mapping(source = "autoIncrement", target = "isAutoIncrement"),
        @Mapping(source = "defaultValue", target = "defaultVal"),
    })
    io.dingodb.sdk.service.entity.meta.ColumnDefinition columnTo(ColumnDefinition tableDefinition);

    List<io.dingodb.sdk.service.entity.meta.ColumnDefinition> columnsTo(List<ColumnDefinition> tableDefinition);

    @Mappings({
        @Mapping(source = "sqlType", target = "type"),
        @Mapping(source = "indexOfKey", target = "primary"),
        @Mapping(source = "autoIncrement", target = "autoIncrement"),
        @Mapping(source = "defaultVal", target = "defaultValue"),
    })
    ColumnDefinition columnDefinitionFrom(io.dingodb.sdk.service.entity.meta.ColumnDefinition tableDefinition);

    List<ColumnDefinition> columnDefinitionFrom(
        List<io.dingodb.sdk.service.entity.meta.ColumnDefinition> columnDefinitions
    );

    default List<Partition> partitionsTo(
        List<PartitionDetailDefinition> details, List<DingoCommonId> partIds, RecordEncoder encoder
    ) {
        List<DingoCommonId> ids = new ArrayList<>(partIds);
        return details.stream()
            .map(PartitionDetailDefinition::getOperand)
            .map(key -> encoder.encodeKeyPrefix(key, key.length))
            .sorted(ByteArrayUtils::compare)
            .map(k -> Partition.builder()
                .range(Range.builder().startKey(realKey(k, ids.get(0))).endKey(nextKey(ids.get(0))).build())
                .id(ids.remove(0))
                .build()
            ).collect(Collectors.toList());
    }

    default PartitionRule partitionTo(
        PartitionDefinition source, List<DingoCommonId> partIds, RecordEncoder encoder
    ) {
        return PartitionRule.builder()
            .strategy(toPartitionStrategy(source.getFuncName()))
            .columns(source.getColumns())
            .partitions(partitionsTo(source.getDetails(), partIds, encoder))
            .build();
    }

    default Object[] operandFrom(Range key, KeyValueCodec codec) {
        return codec.decodeKeyPrefix(CodecService.INSTANCE.setId(key.getStartKey(), 0));
    }

    default io.dingodb.meta.Partition partitionFrom(Partition partition, KeyValueCodec codec) {
        return io.dingodb.meta.Partition.builder()
            .id(MAPPER.idFrom(partition.getId()))
            .operand(operandFrom(partition.getRange(), codec))
            .build();
    }

    default List<io.dingodb.meta.Partition> partitionFrom(List<Partition> partitions, KeyValueCodec codec) {
        return partitions.stream().map($ -> partitionFrom($, codec)).collect(Collectors.toList());
    }

    void tableFrom(
        io.dingodb.sdk.service.entity.meta.TableDefinition tableDefinition, @MappingTarget Table.TableBuilder builder
    );

    default Table tableFrom(
        io.dingodb.sdk.service.entity.meta.TableDefinitionWithId tableWithId,
        List<io.dingodb.sdk.service.entity.meta.TableDefinitionWithId> indexes
    ) {
        Table.TableBuilder builder = Table.builder();
        tableFrom(tableWithId.getTableDefinition(), builder);
        PartitionRule partitionRule = tableWithId.getTableDefinition().getTablePartition();
        builder.partitionStrategy(partitionRule.getStrategy().name());
        KeyValueCodec codec = CodecService.INSTANCE
            .createKeyValueCodec(columnDefinitionFrom(tableWithId.getTableDefinition().getColumns()));
        builder.partitions(partitionFrom(tableWithId.getTableDefinition().getTablePartition().getPartitions(), codec));
        builder.indexes(indexes.stream().map($ -> tableFrom($, Collections.emptyList())).collect(Collectors.toList()));
        return builder.build();
    }

    io.dingodb.sdk.service.entity.meta.TableDefinition tableTo(TableDefinition tableDefinition);

    default TableDefinitionWithId tableTo(
        TableIdWithPartIds ids, TableDefinition tableDefinition
    ) {
        RecordEncoder encoder = new RecordEncoder(
            1, CodecService.createSchemasForType(tableDefinition.getDingoType(), tableDefinition.getKeyMapping()), 0
        );
        io.dingodb.sdk.service.entity.meta.TableDefinition definition = tableTo(tableDefinition);
        definition.setTablePartition(partitionTo(tableDefinition.getPartDefinition(), ids.getPartIds(), encoder));
        return TableDefinitionWithId.builder().tableDefinition(definition).tableId(ids.getTableId()).build();
    }

    default byte[] realKey(byte[] key, DingoCommonId id) {
        return CodecService.INSTANCE.setId(key, MAPPER.idFrom(id));
    }

    default byte[] nextKey(DingoCommonId id) {
        DingoCommonId nextId = MAPPER.copyId(id);
        nextId.setEntityId(id.getEntityId() + 1);
        return CodecService.INSTANCE.setId(CodecService.INSTANCE.empty(), MAPPER.idFrom(nextId));
    }

}
