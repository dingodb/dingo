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
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.Optional;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import static io.dingodb.common.util.Utils.calculatePrefixCount;

@Slf4j
public abstract class StatsOperator {
    public static StoreService storeService;
    public static MetaService metaService;

    public static final String ANALYZE_TASK = "analyze_task";
    public static final String TABLE_BUCKETS = "table_buckets";
    public static final String TABLE_STATS = "table_stats";
    public static final String CM_SKETCH = "cm_sketch";

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

    public static StoreInstance analyzeTaskStore;
    public static StoreInstance bucketsStore;
    public static StoreInstance statsStore;
    public static StoreInstance cmSketchStore;

    static {
        try {
            storeService = StoreService.getDefault();
            metaService = MetaService.root().getSubMetaService("MYSQL");
            analyzeTaskTable = metaService.getTable(ANALYZE_TASK);
            bucketsTable = metaService.getTable(TABLE_BUCKETS);
            statsTable = metaService.getTable(TABLE_STATS);
            cmSketchTable = metaService.getTable(CM_SKETCH);
            analyzeTaskTblId = analyzeTaskTable.tableId;
            bucketsTblId = bucketsTable.tableId;
            statsTblId = statsTable.tableId;
            cmSketchTblId = cmSketchTable.tableId;
            analyzeTaskCodec = CodecService.getDefault()
                .createKeyValueCodec(analyzeTaskTable.version, analyzeTaskTable.tupleType(), analyzeTaskTable.keyMapping());
            bucketsCodec = CodecService.getDefault()
                .createKeyValueCodec(bucketsTable.version, bucketsTable.tupleType(), bucketsTable.keyMapping());
            statsCodec = CodecService.getDefault()
                .createKeyValueCodec(statsTable.version, statsTable.tupleType(), statsTable.keyMapping());
            cmSketchCodec = CodecService.getDefault()
                .createKeyValueCodec(cmSketchTable.version, cmSketchTable.tupleType(), cmSketchTable.keyMapping());
            analyzeTaskStore = storeService.getInstance(analyzeTaskTblId,
                getRegionId(analyzeTaskTblId));
            bucketsStore = storeService.getInstance(bucketsTblId, getRegionId(bucketsTblId));
            statsStore = storeService.getInstance(statsTblId, getRegionId(statsTblId));
            cmSketchStore = storeService
                .getInstance(cmSketchTblId, getRegionId(cmSketchTblId));
        } catch (Exception e) {
        }
    }

    public static CommonId getRegionId(CommonId tableId) {
        return Optional.ofNullable(metaService.getRangeDistribution(tableId))
            .map(NavigableMap::firstEntry)
            .map(Map.Entry::getValue)
            .map(RangeDistribution::getId)
            .orElseThrow("Cannot get region for " + tableId);
    }

    public void upsert(StoreInstance store, KeyValueCodec codec, List<Object[]> rowList) {
        rowList.forEach(row -> {
            KeyValue old = store.get(codec.encodeKey(row));
            if (old == null || old.getValue() == null) {
                store.insert(codec.encode(row));
            } else {
                store.update(codec.encode(row), old);
            }

        });
    }

    public static void delStats(String schemaName, String tableName) {
        try {
            Object[] tuple = new Object[15];
            tuple[0] = schemaName;
            tuple[1] = tableName;
            delStats(analyzeTaskStore, analyzeTaskCodec, tuple);

            tuple = new Object[8];
            tuple[0] = schemaName;
            tuple[1] = tableName;
            delStats(cmSketchStore, cmSketchCodec, tuple);
            delStats(statsStore, statsCodec, tuple);

            tuple = new Object[5];
            tuple[0] = schemaName;
            tuple[1] = tableName;
            delStats(bucketsStore, bucketsCodec, tuple);
        } catch (Exception ignored) {
        }
    }

    public static void delStats(StoreInstance store, KeyValueCodec codec, Object[] tuples) {
        byte[] prefix = codec.encodeKeyPrefix(tuples, calculatePrefixCount(tuples));
        store.delete(new StoreInstance.Range(prefix, prefix, true, true));
    }

    public List<Object[]> scan(StoreInstance store, KeyValueCodec codec, RangeDistribution rangeDistribution) {
        try {
            Iterator<KeyValue> iterator = store.scan(
                new StoreInstance.Range(rangeDistribution.getStartKey(), rangeDistribution.getEndKey(),
                    rangeDistribution.isWithStart(), true)
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

    public static Object[] get(StoreInstance store, KeyValueCodec codec, Object[] key) {
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

    public Object[] generateAnalyzeTask(String schemaName,
                                        String tableName,
                                        long totalCount,
                                        long modifyCount) {
        return new Object[] {schemaName, tableName, "", totalCount, null, null,
            StatsTaskState.PENDING.getState(), null, DingoConfiguration.host(), modifyCount,
            new Timestamp(System.currentTimeMillis()), 0, 0, 0, 0};
    }

}
