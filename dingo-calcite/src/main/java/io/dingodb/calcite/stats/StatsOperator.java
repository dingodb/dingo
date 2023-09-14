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
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.Optional;
import io.dingodb.meta.MetaService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public abstract class StatsOperator {
    public static StoreService storeService = StoreService.getDefault();
    public static MetaService metaService = MetaService.root().getSubMetaService("mysql");

    public static CommonId analyzeTaskTblId = metaService.getTableId("analyze_task");
    public static CommonId bucketsTblId = metaService.getTableId("table_buckets");
    public static CommonId statsTblId = metaService.getTableId("table_stats");
    public static CommonId cmSketchTblId = metaService.getTableId("cm_sketch");

    public static final TableDefinition analyzeTaskTd = metaService.getTableDefinition(analyzeTaskTblId);
    public static final TableDefinition bucketsTd = metaService.getTableDefinition(bucketsTblId);
    public static final TableDefinition statsTd = metaService.getTableDefinition(statsTblId);
    public static final TableDefinition cmSketchTd = metaService.getTableDefinition(cmSketchTblId);

    public static final KeyValueCodec analyzeTaskCodec = CodecService.getDefault()
        .createKeyValueCodec(analyzeTaskTblId, analyzeTaskTd);
    public static final KeyValueCodec bucketsCodec = CodecService.getDefault()
        .createKeyValueCodec(bucketsTblId, bucketsTd);
    public static final KeyValueCodec statsCodec = CodecService.getDefault().createKeyValueCodec(statsTblId, statsTd);
    public static final KeyValueCodec cmSketchCodec = CodecService.getDefault()
        .createKeyValueCodec(cmSketchTblId, cmSketchTd);

    public static final StoreInstance analyzeTaskStore = storeService.getInstance(analyzeTaskTblId,
        getRegionId(analyzeTaskTblId));
    public static final StoreInstance bucketsStore = storeService.getInstance(bucketsTblId, getRegionId(bucketsTblId));
    public static final StoreInstance statsStore = storeService.getInstance(statsTblId, getRegionId(statsTblId));
    public static final StoreInstance cmSketchStore = storeService
        .getInstance(cmSketchTblId, getRegionId(cmSketchTblId));

    public static CommonId getRegionId(CommonId tableId) {
        return Optional.ofNullable(metaService.getRangeDistribution(tableId))
            .map(NavigableMap::firstEntry)
            .map(Map.Entry::getValue)
            .map(RangeDistribution::getId)
            .orElseThrow("Cannot get region for " + tableId);
    }

    public void upsert(StoreInstance store, KeyValueCodec codec, List<Object[]> rowList) {
        rowList.forEach(row -> {
            try {
                KeyValue old = store.get(codec.encodeKey(row));
                if (old == null) {
                    store.insert(codec.encode(row));
                } else {
                    store.update(codec.encode(row), old);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
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
        Object[] values = new Object[analyzeTaskTd.getColumnsCount()];
        values[0] = schemaName;
        values[1] = tableName;
        return values;
    }

    public Object[] generateAnalyzeTask(String schemaName,
                                        String tableName,
                                        Long totalCount,
                                        long modifyCount) {
        return new Object[] {schemaName, tableName, "", totalCount, null, null,
            StatsTaskState.PENDING.getState(), null, DingoConfiguration.host(), modifyCount,
            new Timestamp(System.currentTimeMillis())};
    }

}
