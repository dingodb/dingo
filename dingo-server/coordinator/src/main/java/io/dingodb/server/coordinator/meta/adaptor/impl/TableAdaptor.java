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
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.Optional;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.schedule.ClusterScheduler;
import io.dingodb.server.coordinator.store.MetaStore;
import io.dingodb.server.protocol.meta.Column;
import io.dingodb.server.protocol.meta.Replica;
import io.dingodb.server.protocol.meta.Table;
import io.dingodb.server.protocol.meta.TablePart;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.dingodb.common.codec.PrimitiveCodec.encodeInt;
import static io.dingodb.common.util.ByteArrayUtils.EMPTY_BYTES;
import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;

@Slf4j
public class TableAdaptor extends BaseAdaptor<Table> {

    public static final CommonId META_ID = CommonId.prefix(ID_TYPE.table, TABLE_IDENTIFIER.table);

    private final ColumnAdaptor columnAdaptor;
    private final TablePartAdaptor tablePartAdaptor;
    private final Map<String, CommonId> tableIdMap = new ConcurrentHashMap<>();

    public TableAdaptor(MetaStore metaStore) {
        super(metaStore);
        this.columnAdaptor = new ColumnAdaptor(metaStore);
        this.tablePartAdaptor = new TablePartAdaptor(metaStore);
        this.metaMap.forEach((id, table) -> tableIdMap.put(table.getName(), id));
        MetaAdaptorRegistry.register(Table.class, this);
    }

    @Override
    public CommonId metaId() {
        return META_ID;
    }

    @Override
    protected void doSave(Table meta) {
    }

    @Override
    protected CommonId newId(Table table) {
        CommonId id = new CommonId(
            META_ID.type(),
            META_ID.identifier(),
            table.getSchema().seqContent(),
            metaStore.generateSeq(CommonId.prefix(META_ID.type(), META_ID.identifier()).encode())
        );
        tableIdMap.put(table.getName(), id);
        return id;
    }

    public void create(CommonId schemaId, TableDefinition definition) {
        Table table = definitionToMeta(schemaId, definition);
        ArrayList<KeyValue> keyValues = new ArrayList<>(definition.getColumnsCount() + 2);
        table.setId(newId(table));

        keyValues.add(new KeyValue(table.getId().encode(), encodeMeta(table)));

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

        TablePart tablePart = TablePart.builder()
            .version(0)
            .schema(table.getSchema())
            .table(table.getId())
            .start(EMPTY_BYTES)
            .build();
        tablePart.setId(tablePartAdaptor.newId(tablePart));
        keyValues.add(new KeyValue(tablePart.getId().encode(), tablePartAdaptor.encodeMeta(tablePart)));

        metaStore.upsertKeyValue(keyValues);
        metaMap.put(table.getId(), table);
        super.save(table);
        columns.forEach(columnAdaptor::save);
        tablePartAdaptor.save(tablePart);
        try {
            ClusterScheduler.instance().getTableScheduler(table.getId()).assignPart(tablePart);
        } catch (Exception e) {
            throw new RuntimeException("Table meta save success, but schedule failed.", e);
        }
    }

    public TablePart newPart(CommonId tableId, byte[] start, byte[] end) {
        Table table = get(tableId);
        TablePart tablePart = TablePart.builder()
            .version(0)
            .schema(table.getSchema())
            .table(table.getId())
            .start(start)
            .end(end)
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
    public void delete(CommonId id) {
        Table table = get(id);
        List<Column> columns = columnAdaptor.getByDomain(table.getId().seqContent());
        List<TablePart> tableParts = tablePartAdaptor.getByDomain(table.getId().seqContent());
        ArrayList<byte[]> keys = new ArrayList<>(columns.size() + tableParts.size() + 1);
        columns.forEach(column -> keys.add(column.getId().encode()));
        tableParts.forEach(part -> keys.add(part.getId().encode()));
        ReplicaAdaptor replicaAdaptor = MetaAdaptorRegistry.getMetaAdaptor(Replica.class);
        tableParts.stream().flatMap(part -> replicaAdaptor.getByDomain(part.getId().seqContent()).stream())
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
            .flatMap(partId -> replicaAdaptor.getByDomain(partId.seqContent()).stream())
            .map(Replica::getId)
            .forEach(replicaAdaptor::delete);
        columnAdaptor.deleteByDomain(id.domain());
        ClusterScheduler.instance().getTableScheduler(id).deleteTable();
        ClusterScheduler.instance().deleteTableScheduler(id);
    }

    public Boolean delete(String tableName) {
        return Optional.of(tableIdMap.remove(tableName))
            .ifPresent(this::delete)
            .isPresent();
    }

    public TableDefinition get(String tableName) {
        return metaToDefinition(get(tableIdMap.get(tableName)));
    }

    public List<CommonId> getAllKey() {
        return new ArrayList<>(tableIdMap.values());
    }

    public Map<String, TableDefinition> getAllDefinition() {
        return getAll().stream().map(this::metaToDefinition)
            .collect(Collectors.toMap(TableDefinition::getName, Function.identity()));
    }

    public CommonId getTableId(String tableName) {
        return tableIdMap.get(tableName);
    }

    private TableDefinition metaToDefinition(Table table) {
        TableDefinition tableDefinition = new TableDefinition(table.getName());
        List<ColumnDefinition> columnDefinitions = columnAdaptor.getByDomain(encodeInt(table.getId().seq())).stream()
            .map(this::metaToDefinition)
            .collect(Collectors.toList());
        tableDefinition.setColumns(columnDefinitions);
        if (log.isDebugEnabled()) {
            log.info("Meta to table definition: {}", tableDefinition);
        }
        return tableDefinition;
    }

    private ColumnDefinition metaToDefinition(Column column) {
        return ColumnDefinition.builder()
            .name(column.getName())
            .notNull(column.isNotNull())
            .precision(column.getPrecision())
            .primary(column.isPrimary())
            .scale(column.getScale())
            .type(SqlTypeName.get(column.getType()))
            .defaultValue(column.getDefaultValue())
            .build();
    }


    private Table definitionToMeta(CommonId schemaId, TableDefinition definition) {
        return Table.builder().name(definition.getName()).schema(schemaId).build();
    }

    private Column definitionToMeta(Table table, ColumnDefinition definition) {
        return Column.builder()
            .name(definition.getName())
            .precision(definition.getPrecision())
            .primary(definition.isPrimary())
            .scale(definition.getScale())
            .type(definition.getType().getName())
            .notNull(definition.isNotNull())
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
