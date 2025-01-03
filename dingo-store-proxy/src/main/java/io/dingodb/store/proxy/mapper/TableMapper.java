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
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.TupleType;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.Table;
import io.dingodb.sdk.common.serial.RecordEncoder;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.service.entity.common.Range;
import io.dingodb.sdk.service.entity.common.SchemaState;
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
import org.mapstruct.Named;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.dingodb.partition.DingoPartitionServiceProvider.HASH_FUNC_NAME;
import static io.dingodb.partition.DingoPartitionServiceProvider.RANGE_FUNC_NAME;
import static io.dingodb.sdk.service.entity.meta.PartitionStrategy.PT_STRATEGY_HASH;
import static io.dingodb.sdk.service.entity.meta.PartitionStrategy.PT_STRATEGY_RANGE;
import static io.dingodb.store.proxy.mapper.Mapper.MAPPER;

public interface TableMapper {

    String TXN_LSM = "TXN_LSM";

    @SneakyThrows
    default PartitionStrategy toPartitionStrategy(String partitionStrategy) {
        return Optional.ofNullable(partitionStrategy)
            .filter($ -> $ != null && !$.isEmpty())
            .filter(s -> s.equalsIgnoreCase(HASH_FUNC_NAME))
            .map(s -> PT_STRATEGY_HASH)
            .orElse(PT_STRATEGY_RANGE);
    }

    default String fromPartitionStrategy(PartitionStrategy partitionStrategy) {
        return partitionStrategy == PT_STRATEGY_HASH ? HASH_FUNC_NAME : RANGE_FUNC_NAME;
    }

    default DingoType typeFrom(io.dingodb.sdk.service.entity.meta.ColumnDefinition cd) {
        return DingoTypeFactory.INSTANCE.fromName(cd.getSqlType(), cd.getElementType(), cd.isNullable());
    }

    @Mappings({
        @Mapping(source = "columnDefinition", target = "type"),
        @Mapping(source = "indexOfKey", target = "primaryKeyIndex"),
        @Mapping(source = "autoIncrement", target = "autoIncrement"),
        @Mapping(source = "defaultVal", target = "defaultValueExpr"),
        @Mapping(source = "sqlType", target = "sqlTypeName"),
        @Mapping(source = "elementType", target = "elementTypeName"),
    })
    Column columnFrom(io.dingodb.sdk.service.entity.meta.ColumnDefinition columnDefinition);

    @Named("columnsFrom")
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
        List<PartitionDetailDefinition> details, List<DingoCommonId> partIds, RecordEncoder encoder, byte namespace
    ) {
        List<DingoCommonId> ids = new ArrayList<>(partIds);
        return details.stream()
            .peek(partDef -> {
                byte[] keys = encoder.encodeKeyPrefix(partDef.getOperand(), partDef.getOperand().length);
                partDef.setKeys(keys);
            })
            .sorted((c1, c2) -> ByteArrayUtils.compare(c1.getKeys(), c2.getKeys()))
            .map(partDef -> Partition.builder()
                .range(Range.builder()
                    .startKey(realKey(partDef.getKeys(), ids.get(0), namespace))
                    .endKey(nextKey(ids.get(0), namespace)).build()
                ).id(ids.remove(0))
                .name(partDef.getPartName())
                .build()
            ).collect(Collectors.toList());
    }

    default PartitionRule partitionTo(
        PartitionDefinition source, List<DingoCommonId> partIds, RecordEncoder encoder, byte firstByte
    ) {
        return PartitionRule.builder()
            .strategy(toPartitionStrategy(source.getFuncName()))
            .columns(source.getColumns())
            .partitions(partitionsTo(source.getDetails(), partIds, encoder, firstByte))
            .build();
    }

    default Object[] operandFrom(Range key, KeyValueCodec codec) {
        return codec.decodeKeyPrefix(CodecService.INSTANCE.setId(key.getStartKey(), 0));
    }

    default io.dingodb.meta.entity.Partition partitionFrom(
        Partition partition, KeyValueCodec codec, String strategy
    ) {
        byte[] start = partition.getRange().getStartKey();
        byte[] end = partition.getRange().getEndKey();
        if (HASH_FUNC_NAME.equals(strategy)) {
            start = Arrays.copyOf(start, start.length);
            end = Arrays.copyOf(end, end.length);
        } else {
            start = CodecService.INSTANCE.setId(Arrays.copyOf(start, start.length), 0);
            end = CodecService.INSTANCE.setId(Arrays.copyOf(end, end.length), 0);
        }
        return io.dingodb.meta.entity.Partition.builder()
            .id(MAPPER.idFrom(partition.getId()))
            .operand(operandFrom(partition.getRange(), codec))
            .start(start)
            .end(end)
            .name(partition.getName())
            .build();
    }

    default List<io.dingodb.meta.entity.Partition> partitionFrom(
        List<Partition> partitions, KeyValueCodec codec, String strategy
    ) {
        return partitions.stream().map($ -> partitionFrom($, codec, strategy)).collect(Collectors.toList());
    }

    @Mappings({
        @Mapping(source = "columns", target = "columns", qualifiedByName = "columnsFrom"),
        @Mapping(target = "engine", defaultValue = TXN_LSM)
    })
    void tableFrom(
        io.dingodb.sdk.service.entity.meta.TableDefinition tableDefinition, @MappingTarget Table.TableBuilder builder
    );

    default Table tableFrom(
        io.dingodb.sdk.service.entity.meta.TableDefinitionWithId tableWithId,
        List<io.dingodb.sdk.service.entity.meta.TableDefinitionWithId> indexes
    ) {
        io.dingodb.sdk.service.entity.meta.TableDefinition definition = tableWithId.getTableDefinition();
        Table.TableBuilder builder = Table.builder();
        tableFrom(definition, builder);
        PartitionRule partitionRule = definition.getTablePartition();
        if (partitionRule != null) {
            builder.partitionStrategy(fromPartitionStrategy(partitionRule.getStrategy()));
        }
        KeyValueCodec codec = CodecService.INSTANCE
            .createKeyValueCodec(definition.getCodecVersion(), definition.getVersion(),
            columnDefinitionFrom(definition.getColumns()));

        if (definition.getTablePartition() != null) {
            builder.partitions(partitionFrom(
                definition.getTablePartition().getPartitions(),
                codec,
                fromPartitionStrategy(partitionRule.getStrategy()))
            );
        }
        builder.schemaState(io.dingodb.common.meta.SchemaState.get(tableWithId.getTableDefinition().getSchemaState().number));
        builder.tableId(MAPPER.idFrom(tableWithId.getTableId()));
        builder.indexes(indexes.stream().map($ -> indexTableFrom(builder, $, Collections.emptyList()))
            .collect(Collectors.toList()));
        return builder.build();
    }

    default IndexTable indexTableFrom(
        Table.TableBuilder tableBuilder,
        io.dingodb.sdk.service.entity.meta.TableDefinitionWithId tableWithId,
        List<io.dingodb.sdk.service.entity.meta.TableDefinitionWithId> indexes
    ) {
        IndexTable.IndexTableBuilder builder = IndexTable.builder();
        io.dingodb.sdk.service.entity.meta.TableDefinition definition = tableWithId.getTableDefinition();
        tableFrom(definition, builder);
        PartitionRule partitionRule = definition.getTablePartition();
        builder.partitionStrategy(fromPartitionStrategy(partitionRule.getStrategy()));
        KeyValueCodec codec = CodecService.INSTANCE.createKeyValueCodec(
            definition.getCodecVersion(), definition.getVersion(), columnDefinitionFrom(definition.getColumns())
        );
        builder.partitions(partitionFrom(
            definition.getTablePartition().getPartitions(),
            codec,
            fromPartitionStrategy(partitionRule.getStrategy())
        ));
        Table table = tableBuilder.build();
        builder.tableId(MAPPER.idFrom(tableWithId.getTableId()));
        builder.primaryId(table.tableId);
        List<String> names = definition.getColumns().stream()
            .map(io.dingodb.sdk.service.entity.meta.ColumnDefinition::getName)
            .collect(Collectors.toList());
        List<Integer> columnIndices = table.getColumnIndices(names);
        builder.mapping(TupleMapping.of(columnIndices));
        builder.schemaState(io.dingodb.common.meta.SchemaState.get(
            tableWithId.getTableDefinition().getSchemaState().number)
        );
        MAPPER.setIndex(builder, definition.getIndexParameter());
        return builder.build();
    }

    io.dingodb.sdk.service.entity.meta.TableDefinition tableTo(TableDefinition tableDefinition);

    default TableDefinitionWithId tableTo(
        TableIdWithPartIds ids, TableDefinition tableDefinition, long tenantId
    ) {
        TupleType keyType = (TupleType) tableDefinition.getKeyType();
        TupleMapping keyMapping = TupleMapping.of(IntStream.range(0, keyType.fieldCount()).toArray());
        RecordEncoder encoder = new RecordEncoder(
            1, CodecService.createSchemasForType(keyType, keyMapping), 0
        );
        if (tableDefinition.getEngine() == null || tableDefinition.getEngine().isEmpty()) {
            tableDefinition.setEngine(TXN_LSM);
        }
        tableDefinition.setEngine(tableDefinition.getEngine().toUpperCase());
        byte namespace = (byte) (tableDefinition.getEngine().startsWith("TXN") ? 't' : 'r');
        io.dingodb.sdk.service.entity.meta.TableDefinition definition = tableTo(tableDefinition);
        if (tableDefinition.getPartDefinition() != null) {
            definition.setTablePartition(
                partitionTo(tableDefinition.getPartDefinition(), ids.getPartIds(), encoder, namespace)
            );
        }
        definition.setName(definition.getName().toUpperCase());
        definition.setSchemaState(convertSchemaState(tableDefinition.getSchemaState()));
        return TableDefinitionWithId.builder().tenantId(tenantId)
            .tableDefinition(definition).tableId(ids.getTableId()).build();
    }

    default SchemaState convertSchemaState(io.dingodb.common.meta.SchemaState schemaState) {
        switch (schemaState) {
            case SCHEMA_NONE:
                return SchemaState.SCHEMA_NONE;
            case SCHEMA_DELETE_ONLY:
                return SchemaState.SCHEMA_DELETE_ONLY;
            case SCHEMA_WRITE_ONLY:
                return SchemaState.SCHEMA_WRITE_ONLY;
            case SCHEMA_WRITE_REORG:
                return SchemaState.SCHEMA_WRITE_REORG;
            case SCHEMA_DELETE_REORG:
                return SchemaState.SCHEMA_DELETE_REORG;
            case SCHEMA_PUBLIC:
                return SchemaState.SCHEMA_PUBLIC;
            default:
                return SchemaState.SCHEMA_PUBLIC;
        }
    }

    default byte[] realKey(byte[] key, DingoCommonId id, byte namespace) {
        key = CodecService.INSTANCE.setId(key, MAPPER.idFrom(id));
        key[0] = namespace;
        return key;
    }

    default byte[] nextKey(DingoCommonId id, byte namespace) {
        DingoCommonId nextId = MAPPER.copyId(id);
        nextId.setEntityId(id.getEntityId() + 1);
        byte[] key = CodecService.INSTANCE.setId(CodecService.INSTANCE.empty(), MAPPER.idFrom(nextId));
        key[0] = namespace;
        return key;
    }

}
