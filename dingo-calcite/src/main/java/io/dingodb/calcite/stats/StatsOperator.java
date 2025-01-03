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

package io.dingodb.calcite.stats;

import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.session.SessionUtil;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Utils;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.transaction.StoreKvTxn;
import io.dingodb.store.api.transaction.StoreTxnService;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

@Slf4j
public abstract class StatsOperator {
    public static StoreTxnService storeTxnService;
    public static MetaService metaService;

    public static final String ANALYZE_TASK = "ANALYZE_TASK";
    public static final String TABLE_BUCKETS = "TABLE_BUCKETS";
    public static final String TABLE_STATS = "TABLE_STATS";
    public static final String CM_SKETCH = "CM_SKETCH";

    public static Table analyzeTaskTable;
    public static Table bucketsTable;
    public static Table statsTable;
    public static Table cmSketchTable;
    public static CommonId analyzeTaskTblId;
    public static CommonId bucketsTblId;
    public static CommonId statsTblId;
    public static CommonId cmSketchTblId;

    public static KeyValueCodec analyzeTaskCodec;
    public static KeyValueCodec bucketsCodec;
    public static KeyValueCodec statsCodec;
    public static KeyValueCodec cmSketchCodec;

    public static StoreKvTxn analyzeTaskStore;
    public static StoreKvTxn bucketsStore;
    public static StoreKvTxn statsStore;
    public static StoreKvTxn cmSketchStore;

    static {
        try {
            io.dingodb.meta.InfoSchemaService infoSchemaService = io.dingodb.meta.InfoSchemaService.root();
            while (!infoSchemaService.prepare()) {
                Utils.sleep(5000L);
            }
            storeTxnService = StoreTxnService.getDefault();
            metaService = MetaService.root().getSubMetaService("MYSQL");
            analyzeTaskTable = getTable(ANALYZE_TASK);
            bucketsTable = getTable(TABLE_BUCKETS);
            statsTable = getTable(TABLE_STATS);
            cmSketchTable = getTable(CM_SKETCH);
            analyzeTaskTblId = analyzeTaskTable.tableId;
            bucketsTblId = bucketsTable.tableId;
            statsTblId = statsTable.tableId;
            cmSketchTblId = cmSketchTable.tableId;
            analyzeTaskCodec = CodecService.getDefault()
                .createKeyValueCodec(
                    analyzeTaskTable.getCodecVersion(), analyzeTaskTable.version,
                    analyzeTaskTable.tupleType(), analyzeTaskTable.keyMapping()
                );
            bucketsCodec = CodecService.getDefault()
                .createKeyValueCodec(bucketsTable.getCodecVersion(), bucketsTable.version,
                bucketsTable.tupleType(), bucketsTable.keyMapping());
            statsCodec = CodecService.getDefault()
                .createKeyValueCodec(statsTable.getCodecVersion(), statsTable.version,
                statsTable.tupleType(), statsTable.keyMapping());
            cmSketchCodec = CodecService.getDefault()
                .createKeyValueCodec(cmSketchTable.getCodecVersion(), cmSketchTable.version,
                cmSketchTable.tupleType(), cmSketchTable.keyMapping());
            analyzeTaskStore = storeTxnService.getInstance(analyzeTaskTblId,
                getRegionId(analyzeTaskTblId));
            bucketsStore = storeTxnService.getInstance(bucketsTblId, getRegionId(bucketsTblId));
            statsStore = storeTxnService.getInstance(statsTblId, getRegionId(statsTblId));
            cmSketchStore = storeTxnService
                .getInstance(cmSketchTblId, getRegionId(cmSketchTblId));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public static CommonId getRegionId(CommonId tableId) {
        return Optional.ofNullable(metaService.getRangeDistribution(tableId))
            .map(NavigableMap::firstEntry)
            .map(Map.Entry::getValue)
            .map(RangeDistribution::getId)
            .orElseThrow("Cannot get region for " + tableId);
    }

    public static void upsert(StoreKvTxn store, KeyValueCodec codec, List<Object[]> rowList) {
        rowList.forEach(row -> {
            KeyValue old = store.get(codec.encodeKey(row));
            KeyValue keyValue = codec.encode(row);
            if (old == null || old.getValue() == null) {
                store.insert(keyValue.getKey(), keyValue.getValue());
            } else {
                store.update(keyValue.getKey(), keyValue.getValue());
            }
        });
    }

    public static void delStats(String schemaName, String tableName) {
    }

    public static void delStats(String table, String schemaName, String tableName) {
        String sqlTemp = "delete from %s where schema_name='%s' and table_name='%s'";
        String sql = String.format(sqlTemp, table, schemaName, tableName);
        String error = SessionUtil.INSTANCE.exeUpdateInTxn(sql);
        if (error != null) {
            LogUtils.error(log, "delStats error:{}, table:{}, schema:{}, tableName:{}",
                error, table, schemaName, tableName);
        }
    }

    public List<Object[]> scan(StoreKvTxn store, KeyValueCodec codec, RangeDistribution rangeDistribution) {
        try {
            Iterator<KeyValue> iterator = store.range(
                rangeDistribution.getStartKey(), rangeDistribution.getEndKey()
            );
            List<Object[]> list = new ArrayList<>();
            while (iterator.hasNext()) {
                list.add(codec.decode(iterator.next()));
            }
            return list;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Object[] get(StoreKvTxn store, KeyValueCodec codec, Object[] key) {
        try {
            KeyValue keyValue = store.get(codec.encodeKey(key));
            if (keyValue.getValue() == null || keyValue.getValue().length == 0) {
                return null;
            }
            return codec.decode(keyValue);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Object[] getAnalyzeTaskKeys(String schemaName, String tableName) {
        Object[] values = new Object[analyzeTaskTable.getColumns().size()];
        values[0] = schemaName;
        values[1] = tableName;
        return values;
    }

    public static Object[] generateAnalyzeTask(String schemaName,
                                               String tableName,
                                               long totalCount,
                                               long modifyCount) {
        return new Object[] {schemaName, tableName, "", totalCount, null, null,
            StatsTaskState.PENDING.getState(), null, DingoConfiguration.host(), modifyCount,
            new Timestamp(System.currentTimeMillis()), 0, 0, 0, 0};
    }

    public static Table getTable(String tableName) {
        int times = 10;
        DdlService ddlService = DdlService.root();
        while (times-- > 0) {
            InfoSchema is = ddlService.getIsLatest();
            if (is != null) {
                Table table = is.getTable("MYSQL", tableName);
                if (table != null) {
                    return table;
                }
            }
            try {
                Thread.sleep(10000L);
            } catch (Exception ignored) {

            }
        }
        throw new RuntimeException("init user error");
    }

}
