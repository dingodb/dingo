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
import io.dingodb.server.coordinator.config.CoordinatorConfiguration;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.schedule.ClusterScheduler;
import io.dingodb.server.coordinator.store.MetaStore;
import io.dingodb.server.protocol.meta.Column;
import io.dingodb.server.protocol.meta.Replica;
import io.dingodb.server.protocol.meta.Table;
import io.dingodb.server.protocol.meta.TablePart;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.type.SqlTypeName;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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
    private final Map<String, CommonId> idMap = new ConcurrentHashMap<>();

    public TableAdaptor(MetaStore metaStore) {
        super(metaStore);
        this.columnAdaptor = new ColumnAdaptor(metaStore);
        this.tablePartAdaptor = new TablePartAdaptor(metaStore);
        this.metaMap.forEach((id, table) -> tableIdMap.put(table.getName(), id));
        this.metaMap.forEach((id, table) -> idMap.put(id.toString(), id));
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
            META_ID.type(),
            META_ID.identifier(),
            table.getSchema().seqContent(),
            metaStore.generateSeq(CommonId.prefix(META_ID.type(), META_ID.identifier()).encode())
        );
        tableIdMap.put(table.getName(), id);
        idMap.put(id.toString(), id);
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

        TablePart tablePart = TablePart.builder()
            .version(0)
            .schema(table.getSchema())
            .table(tableId)
            .start(EMPTY_BYTES)
            .createTime(System.currentTimeMillis())
            .build();
        tablePart.setId(tablePartAdaptor.newId(tablePart));
        keyValues.add(new KeyValue(tablePart.getId().encode(), tablePartAdaptor.encodeMeta(tablePart)));

        metaStore.upsertKeyValue(keyValues);
        metaMap.put(tableId, table);
        super.save(table);
        columns.forEach(columnAdaptor::save);
        tablePartAdaptor.save(tablePart);
        try {
            ClusterScheduler.instance().getTableScheduler(tableId).assignPart(tablePart).get(30, TimeUnit.SECONDS);
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
    protected void doDelete(Table table) {
        CommonId id = table.getId();
        List<Column> columns = columnAdaptor.getByDomain(id.seqContent());
        List<TablePart> tableParts = tablePartAdaptor.getByDomain(id.seqContent());
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
        columnAdaptor.deleteByDomain(id.domainContent());
        ClusterScheduler.instance().getTableScheduler(id).deleteTable();
        ClusterScheduler.instance().deleteTableScheduler(id);
    }

    public Boolean delete(String tableName) {
        if (tableIdMap.containsKey(tableName)) {
            CommonId id = tableIdMap.get(tableName);
            idMap.remove(id.toString());
            tableIdMap.remove(tableName);
            delete(id);
            return true;
        }
        return false;
    }

    public TableDefinition get(String tableName) {
        return metaToDefinition(get(tableIdMap.get(tableName)));
    }

    public TableDefinition getDefinition(CommonId id) {
        if (get(id) == null) {
            return null;
        }
        return metaToDefinition(get(id));
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
            .elementType(SqlTypeName.get(column.getElementType()))
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
            .build();
    }

    private Column definitionToMeta(Table table, ColumnDefinition definition) {
        SqlTypeName elementTypeName = definition.getElementType();
        return Column.builder()
            .name(definition.getName())
            .precision(definition.getPrecision())
            .primary(definition.isPrimary())
            .scale(definition.getScale())
            .type(definition.getType().getName())
            .elementType(elementTypeName == null ? null : elementTypeName.getName())
            .notNull(definition.isNotNull())
            .table(table.getId())
            .schema(table.getSchema())
            .defaultValue(definition.getDefaultValue())
            .build();
    }

    public Integer getUdfVersion(CommonId id, String udfName) {
        byte[] udfKey = udfNameCommonIdToBytes(id, udfName);
        byte[] versionBytes = this.metaStore.getValueByPrimaryKey(udfKey);
        if (versionBytes == null) {
            return 0;
        }
        return bytesToInt(versionBytes);
    }

    public Integer updateUdfVersion(CommonId id, String udfName) {
        byte[] udfKey = udfNameCommonIdToBytes(id, udfName);
        byte[] versionBytes = this.metaStore.getValueByPrimaryKey(udfKey);
        if (versionBytes == null) {
            this.metaStore.upsertKeyValue(udfKey, intToBytes(1));
            return 1;
        }
        Integer version = bytesToInt(versionBytes);
        version++;
        this.metaStore.upsertKeyValue(udfKey, intToBytes(version));
        return version;
    }

    public boolean updateUdfFunction(CommonId id, String udfName, Integer version, String function) {
        byte[] udfKey = udfNameCommonIdVersionToBytes(id, udfName, version);
        return this.metaStore.upsertKeyValue(udfKey, function.getBytes(StandardCharsets.UTF_8));
    }

    public boolean deleteUdfFunction(CommonId id, String udfName, Integer version) {
        byte[] udfKey = udfNameCommonIdVersionToBytes(id, udfName, version);
        return this.metaStore.delete(udfKey);
    }

    public String getUdfFunction(CommonId id, String udfName, Integer version) {
        byte[] udfKey = udfNameCommonIdVersionToBytes(id, udfName, version);
        byte[] functionBytes = this.metaStore.getValueByPrimaryKey(udfKey);
        if (functionBytes == null) {
            return null;
        }
        return new String(functionBytes, StandardCharsets.UTF_8);
    }

    private byte[] udfNameCommonIdToBytes(CommonId id, String udfName) {
        byte[] udfNameBytes = udfName.getBytes(StandardCharsets.UTF_8);
        byte[] idBytes = id.encode();
        byte[] result = new byte[udfNameBytes.length + idBytes.length];
        System.arraycopy(udfNameBytes, 0,
            result, 0, udfNameBytes.length);
        System.arraycopy(idBytes, 0,
            result, udfNameBytes.length - 1, idBytes.length);
        return result;
    }

    private byte[] udfNameCommonIdVersionToBytes(CommonId id, String udfName, Integer version) {
        byte[] udfNameCommonIdBytes = udfNameCommonIdToBytes(id, udfName);
        byte[] versionBytes = intToBytes(version);
        byte[] result = new byte[udfNameCommonIdBytes.length + versionBytes.length];
        System.arraycopy(udfNameCommonIdBytes, 0,
            result, 0, udfNameCommonIdBytes.length);
        System.arraycopy(versionBytes, 0,
            result, udfNameCommonIdBytes.length - 1, versionBytes.length);
        return result;
    }

    private byte[] intToBytes(Integer num) {
        return num.toString().getBytes(StandardCharsets.UTF_8);
    }

    private Integer bytesToInt(byte[] numBytes) {
        String numStr = new String(numBytes, StandardCharsets.UTF_8);
        return Integer.parseInt(numStr);
    }

    public CommonId getTableIdByIdString(CommonId id) {
        return idMap.get(id.toString());
    }

    @AutoService(BaseAdaptor.Creator.class)
    public static class Creator implements BaseAdaptor.Creator<Table, TableAdaptor> {
        @Override
        public TableAdaptor create(MetaStore metaStore) {
            return new TableAdaptor(metaStore);
        }
    }
}
