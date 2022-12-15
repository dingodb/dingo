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

package io.dingodb.server.coordinator.meta.adaptor.impl;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.common.codec.DingoKeyValueCodec;
import io.dingodb.common.codec.KeyValueCodec;
import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.DebugLog;
import io.dingodb.common.util.NoBreakFunctions;
import io.dingodb.common.util.Optional;
import io.dingodb.server.coordinator.config.CoordinatorConfiguration;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.schedule.ClusterScheduler;
import io.dingodb.server.coordinator.store.MetaStore;
import io.dingodb.server.protocol.meta.Column;
import io.dingodb.server.protocol.meta.Replica;
import io.dingodb.server.protocol.meta.Table;
import io.dingodb.server.protocol.meta.TablePart;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.dingodb.common.util.ByteArrayUtils.EMPTY_BYTES;
import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;

@Slf4j
public class TableAdaptor extends BaseAdaptor<Table> {

    public static final CommonId META_ID = CommonId.prefix(ID_TYPE.table, TABLE_IDENTIFIER.table);
    public static final byte[] SEQ_KEY = META_ID.encode();

    private final ColumnAdaptor columnAdaptor;
    private final TablePartAdaptor tablePartAdaptor;
    private final Map<CommonId, Map<String, CommonId>> idNameMap = new ConcurrentHashMap<>();

    public TableAdaptor(MetaStore metaStore) {
        super(metaStore);
        this.columnAdaptor = new ColumnAdaptor(metaStore);
        this.tablePartAdaptor = new TablePartAdaptor(metaStore);
        this.metaMap.values().forEach(table -> idNameMap
            .computeIfAbsent(table.getSchema(), k -> new ConcurrentHashMap<>())
            .put(table.getName(), table.getId())
        );
        MetaAdaptorRegistry.register(Table.class, this);
    }

    @Override
    public CommonId metaId() {
        return META_ID;
    }

    @Override
    protected void doSave(Table meta) {
    }

    public void pureSave(Table table) {
        super.save(table);
        metaStore.upsertKeyValue(table.getId().encode(), encodeMeta(table));
    }

    @Override
    protected CommonId newId(Table table) {
        CommonId id = new CommonId(
            META_ID.type(), META_ID.identifier(), table.getSchema().seq(), metaStore.generateSeq(SEQ_KEY), 1
        );
        idNameMap
            .computeIfAbsent(table.getSchema(), k -> new ConcurrentHashMap<>())
            .put(table.getName(), id);
        return id;
    }

    public void create(CommonId schemaId, TableDefinition definition) {
        Table table = definitionToMeta(schemaId, definition);
        table.setCreateTime(System.currentTimeMillis());
        ArrayList<KeyValue> keyValues = new ArrayList<>(definition.getColumnsCount() + 2);
        CommonId tableId = newId(table);
        table.setId(tableId);

        keyValues.add(new KeyValue(tableId.encode(), encodeMeta(table)));

        List<Column> columns = definition.getColumns()
            .stream()
            .map(columnDefinition -> definitionToMeta(table, columnDefinition))
            .peek(column -> column.setId(columnAdaptor.newId(column)))
            .collect(Collectors.toList());

        if (log.isDebugEnabled()) {
            log.debug("receive create tableName:{} definition:{}, after convert columns:{}",
                table.getName(),
                definition,
                columns.stream().map(Column::toString).collect(Collectors.joining("\n")));
        }

        columns.stream()
            .map(column -> new KeyValue(column.getId().encode(), columnAdaptor.encodeMeta(column)))
            .forEach(keyValues::add);
        int ttl = table.getTtl();
        List<TablePart> tablePartList = Optional.mapOrNull(definition.getPartDefinition(), __ -> getPreDefineParts(table, definition));
        if (tablePartList == null) {
            TablePart tablePart = TablePart.builder()
                .version(0)
                .schema(table.getSchema())
                .table(tableId)
                .start(EMPTY_BYTES)
                .createTime(System.currentTimeMillis())
                .build();
            tablePart.setId(tablePartAdaptor.newId(tablePart));
            tablePartList = new ArrayList<>();
            tablePartList.add(tablePart);
        }
        metaStore.upsertKeyValue(keyValues);
        keyValues.clear();
        super.save(table);
        columns.forEach(columnAdaptor::save);
        for (TablePart tablePart : tablePartList) {
            tablePart.setTtl(ttl);
            keyValues.add(new KeyValue(tablePart.getId().encode(), tablePartAdaptor.encodeMeta(tablePart)));

            tablePartAdaptor.save(tablePart);
            try {
                ClusterScheduler.instance().getTableScheduler(tableId).assignPart(tablePart).get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException("Table meta save success, but schedule failed.", e);
            }
        }
        metaStore.upsertKeyValue(keyValues);
    }

    @SuppressWarnings("checkstyle:MultipleVariableDeclarations")
    private List<TablePart> getPreDefineParts(Table table, TableDefinition definition) {
        List<TablePart> tablePartList = new ArrayList<>();
        String strategy = definition.getPartDefinition().getFuncName();
        int primaryKeyCount = definition.getPrimaryKeyCount();
        if ("range".equalsIgnoreCase(strategy)) {
            List<PartitionDetailDefinition> partDetailList = definition.getPartDefinition().getDetails();
            KeyValueCodec partKeyCodec = new DingoKeyValueCodec(definition.getDingoType(), definition.getKeyMapping());
            Iterator<byte[]> keys = partDetailList.stream()
                .map(PartitionDetailDefinition::getOperand)
                .map(operand -> operand.toArray(new Object[primaryKeyCount]))
                .map(NoBreakFunctions.wrap(partKeyCodec::encodeKey))
                .collect(Collectors.toCollection(() -> new TreeSet<>(ByteArrayUtils::compare)))
                .iterator();

            byte [] start = EMPTY_BYTES, key;
            TablePart tablePart;
            while (keys.hasNext()) {
                key = keys.next();
                tablePartList.add(
                    tablePart = TablePart.builder()
                        .version(1)
                        .schema(table.getSchema())
                        .table(table.getId())
                        .start(start)
                        .end(key)
                        .createTime(System.currentTimeMillis())
                        .build()
                );
                tablePart.setId(tablePartAdaptor.newId(tablePart));
                start = key;
            }
            tablePartList.add(
                tablePart = TablePart.builder()
                    .version(1)
                    .schema(table.getSchema())
                    .table(table.getId())
                    .start(start)
                    .end(null)
                    .createTime(System.currentTimeMillis())
                    .build()
            );
            tablePart.setId(tablePartAdaptor.newId(tablePart));
        }
        return tablePartList;
    }

    public TablePart newPart(CommonId tableId, byte[] start, byte[] end) {
        Table table = get(tableId);
        int ttl = table.getTtl();
        log.info("TableAdaptor newPart, table id: {}, ttl: {}.", table.getId(), ttl);
        TablePart tablePart = TablePart.builder()
            .version(0)
            .schema(table.getSchema())
            .table(table.getId())
            .start(start)
            .end(end)
            .ttl(ttl)
            .build();
        tablePart.setId(tablePartAdaptor.newId(tablePart));
        metaStore.upsertKeyValue(tablePart.getId().encode(), tablePartAdaptor.encodeMeta(tablePart));
        tablePartAdaptor.save(tablePart);
        return tablePart;
    }

    public TablePart updatePart(CommonId tablePartId, byte[] start, byte[] end) {
        TablePart tablePart = tablePartAdaptor.get(tablePartId);
        tablePart.setStart(start);
        tablePart.setEnd(end);
        metaStore.upsertKeyValue(tablePart.getId().encode(), tablePartAdaptor.encodeMeta(tablePart));
        tablePartAdaptor.save(tablePart);
        return tablePart;
    }

    @Override
    protected void doDelete(Table table) {
        CommonId id = table.getId();
        List<Column> columns = columnAdaptor.getByDomain(id.seq());
        List<TablePart> tableParts = tablePartAdaptor.getByDomain(id.seq());
        ArrayList<byte[]> keys = new ArrayList<>(columns.size() + tableParts.size() + 1);
        columns.forEach(column -> keys.add(column.getId().encode()));
        tableParts.forEach(part -> keys.add(part.getId().encode()));
        ReplicaAdaptor replicaAdaptor = MetaAdaptorRegistry.getMetaAdaptor(Replica.class);
        tableParts.stream().flatMap(part -> replicaAdaptor.getByDomain(part.getId().seq()).stream())
            .map(Replica::getId)
            .peek(replicaAdaptor::delete)
            .map(CommonId::encode)
            .forEach(keys::add);
        keys.add(id.encode());
        metaStore.delete(keys);
        metaMap.remove(id);
        tableParts.stream()
            .map(TablePart::getId)
            .peek(tablePartAdaptor::delete)
            .flatMap(partId -> replicaAdaptor.getByDomain(partId.seq()).stream())
            .map(Replica::getId)
            .forEach(replicaAdaptor::delete);
        columnAdaptor.deleteByDomain(id.seq());
        ClusterScheduler.instance().getTableScheduler(id).deleteTable();
        ClusterScheduler.instance().deleteTableScheduler(id);
    }


    public boolean delete(CommonId id, String tableName) {
        return Optional.ofNullable(idNameMap.get(id))
            .map(__ -> __.remove(tableName))
            .ifPresent(this::delete)
            .isPresent();
    }

    public TableDefinition get(CommonId schemaId, String tableName) {
        return getDefinition(getTableId(schemaId, tableName));
    }

    public TableDefinition getDefinition(CommonId id) {
        return Optional.mapOrNull(get(id), this::metaToDefinition);
    }

    public Map<String, TableDefinition> getAllDefinition(CommonId id) {
        return Optional.ofNullable(idNameMap.get(id)).map(Map::values).map(__ -> __.stream()
            .map(this::getDefinition)
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(TableDefinition::getName, Function.identity())
        )).orElseGet(HashMap::new);
    }

    public CommonId getTableId(CommonId id, String tableName) {
        return Optional.mapOrNull(idNameMap.get(id), __ -> __.get(tableName));
    }

    private TableDefinition metaToDefinition(Table table) {
        TableDefinition tableDefinition = new TableDefinition(table.getName());
        List<ColumnDefinition> columnDefinitions = columnAdaptor.getByDomain(table.getId().seq()).stream()
            .map(this::metaToDefinition)
            .collect(Collectors.toList());
        tableDefinition.setColumns(columnDefinitions);
        tableDefinition.setPartDefinition(table.getPartDefinition());
        tableDefinition.setProperties(table.getProperties());
        tableDefinition.setTtl(table.getTtl());
        DebugLog.debug(log, "Meta to table definition: {}", tableDefinition);
        return tableDefinition;
    }

    private ColumnDefinition metaToDefinition(Column column) {
        return ColumnDefinition.builder()
            .name(column.getName())
            .nullable(column.isNullable())
            .precision(column.getPrecision())
            .primary(column.isPrimary())
            .scale(column.getScale())
            .type(column.getType())
            .elementType(column.getElementType())
            .defaultValue(column.getDefaultValue())
            .build();
    }

    private Table definitionToMeta(CommonId schemaId, TableDefinition definition) {
        return Table.builder()
            .name(definition.getName())
            .schema(schemaId)
            .partMaxCount(CoordinatorConfiguration.schedule().getDefaultAutoMaxCount())
            .partMaxSize(CoordinatorConfiguration.schedule().getDefaultAutoMaxSize())
            .autoSplit(CoordinatorConfiguration.schedule().isAutoSplit())
            .properties(definition.getProperties())
            .partDefinition(definition.getPartDefinition())
            .ttl(definition.getTtl())
            .build();
    }

    private Column definitionToMeta(Table table, ColumnDefinition definition) {
        return Column.builder()
            .name(definition.getName())
            .precision(definition.getPrecision())
            .primary(definition.isPrimary())
            .scale(definition.getScale())
            .type(definition.getTypeName())
            .elementType(definition.getElementType())
            .nullable(definition.isNullable())
            .table(table.getId())
            .schema(table.getSchema())
            .defaultValue(definition.getDefaultValue())
            .build();
    }

    @AutoService(BaseAdaptor.Creator.class)
    public static class Creator implements BaseAdaptor.Creator<Table, TableAdaptor> {
        @Override
        public TableAdaptor create(MetaStore metaStore) {
            return new TableAdaptor(metaStore);
        }
    }

}
