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

package io.dingodb.server.executor.service;

import com.google.auto.service.AutoService;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.mysql.SchemataDefinition;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.Optional;
import io.dingodb.meta.MetaService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;
import io.dingodb.verify.service.InformationServiceProvider;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Map;
import java.util.NavigableMap;

public class InformationService implements io.dingodb.verify.service.InformationService {

    public static final InformationService INSTANCE = new InformationService();

    @AutoService(InformationServiceProvider.class)
    public static class Provider implements InformationServiceProvider {
        @Override
        public io.dingodb.verify.service.InformationService get() {
            return INSTANCE;
        }
    }

    public static final String SCHEMATA = "SCHEMATA";
    public static final String TABLES = "TABLES";

    private final MetaService metaService = MetaService.root().getSubMetaService("INFORMATION_SCHEMA");

    private final StoreService storeService = StoreService.getDefault();

    private final CommonId schemataId = metaService.getTableId(SCHEMATA);
    private final CommonId tableMetaId = metaService.getTableId(TABLES);

    private final TableDefinition schemata = metaService.getTableDefinition(SCHEMATA);
    private final TableDefinition tableMeta = metaService.getTableDefinition(TABLES);

    private final StoreInstance schemataStore = storeService.getInstance(schemataId, getRegionId(schemataId));
    private final StoreInstance tableMetaStore = storeService.getInstance(tableMetaId, getRegionId(tableMetaId));

    private final KeyValueCodec schemataCodec =
        CodecService.getDefault().createKeyValueCodec(getPartId(schemataId, schemataStore.id()), schemata);
    private final KeyValueCodec tableMetaCodec =
        CodecService.getDefault().createKeyValueCodec(getPartId(tableMetaId, tableMetaStore.id()), tableMeta);



    private CommonId getRegionId(CommonId tableId) {
        return Optional.ofNullable(metaService.getRangeDistribution(tableId))
            .map(NavigableMap::firstEntry)
            .map(Map.Entry::getValue)
            .map(RangeDistribution::getId)
            .orElseThrow("Cannot get region for " + tableId);
    }

    private static CommonId getPartId(CommonId tableId, CommonId regionId) {
        return new CommonId(CommonId.CommonType.PARTITION, tableId.seq, regionId.domain);
    }

    @Override
    public void saveSchemata(SchemataDefinition schemataDefinition) {
        Object[] row = createSchemataRow(schemataDefinition);
        try {
            schemataStore.insert(schemataCodec.encode(row));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropSchemata(String schemaName) {
        Object[] keys = getSchemataKeys(schemaName);
        try {
            schemataStore.delete(schemataCodec.encodeKey(keys));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateSchemata(SchemataDefinition schemataDefinition) {
    }

    @Override
    public void saveTableMeta(String schemaName, TableDefinition tableDefinition) {
        Object[] row = createTableMetaRow(schemaName, tableDefinition);
        try {
            tableMetaStore.insert(tableMetaCodec.encode(row));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropTableMeta(String schemaName, String tableName) {
        Object[] keys = getTableMetaKeys(schemaName, tableName);
        try {
            tableMetaStore.delete(tableMetaCodec.encodeKey(keys));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateTableMeta(String schemaName, TableDefinition tableDefinition) {
        Object[] keys = getTableMetaKeys(schemaName, tableDefinition.getName());
        try {
            KeyValue oldKeyValue = tableMetaStore.get(tableMetaCodec.encodeKey(keys));
            Object[] values = tableMetaCodec.decode(oldKeyValue);
            if (values == null) {
                return;
            }
            if (StringUtils.isNotBlank(tableDefinition.getCollate())) {
                values[17] = tableDefinition.getCollate();
            }
            if (StringUtils.isNotBlank(tableDefinition.getComment())) {
                values[20] = tableDefinition.getComment();
            }
            values[15] = System.currentTimeMillis();
            KeyValue newKeyValue  = tableMetaCodec.encode(values);
            tableMetaStore.update(newKeyValue, oldKeyValue);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Object[] getSchemataKeys(String schemaName) {
        Object[] row = new Object[5];
        row[0] = "def";
        row[1] = schemaName;
        return row;
    }

    private static Object[] getTableMetaKeys(String schemaName, String table) {
        Object[] row = new Object[21];
        row[0] = "def";
        row[1] = schemaName;
        row[2] = table;
        return row;
    }

    private static Object[] createSchemataRow(SchemataDefinition schemataDefinition) {
        Object[] row = new Object[5];
        row[0] = "def";
        row[1] = schemataDefinition.getSchemaName();
        row[2] = schemataDefinition.getCharacter();
        row[3] = schemataDefinition.getCollate();
        row[4] = null;
        return row;
    }

    private Object[] createTableMetaRow(String schemaName, TableDefinition tableDefinition) {
        Object[] row = new Object[tableMeta.getColumnsCount()];
        // catalog
        row[0] = "def";
        // schema name
        row[1] = schemaName;
        // table name
        row[2] = tableDefinition.getName();
        // table type
        row[3] = "BASE TABLE";
        // engine
        row[4] = "ENG_ROCKSDB";
        // version
        row[5] = 10;
        // row format
        row[6] = "Dynamic";
        // row count
        row[7] = 0;
        // avg row length
        row[8] = 0;
        // data length
        row[9] = 1000;
        // max data length
        row[10] = 0;
        // index length
        row[11] = 0;
        // data free
        row[12] = 0;
        // auto increment
        if (tableDefinition.getAutoIncrement() == -1) {
            row[13] = null;
        } else {
            row[13] = tableDefinition.getAutoIncrement();
        }
        // create time
        row[14] = new Timestamp(System.currentTimeMillis());
        // update time
        row[15] = null;
        // check time
        row[16] = null;
        // table collate;
        row[17] = "utf8_bin";
        // check sum
        row[18] = null;
        // create_options
        row[19] = "";
        // comment
        row[20] = tableDefinition.getComment();
        return row;
    }
}
