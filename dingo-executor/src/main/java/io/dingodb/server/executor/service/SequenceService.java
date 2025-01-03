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
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.sequence.SequenceDefinition;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Utils;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.SequenceServiceProvider;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.Table;
import io.dingodb.server.executor.common.SequenceGenerator;
import io.dingodb.server.executor.ddl.DdlContext;
import io.dingodb.store.service.StoreKvTxn;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
public class SequenceService implements io.dingodb.meta.SequenceService {

    public static final SequenceService INSTANCE = new SequenceService();

    @AutoService(SequenceServiceProvider.class)
    public static class Provider implements SequenceServiceProvider {
        @Override
        public io.dingodb.meta.SequenceService get() {
            return INSTANCE;
        }
    }

    public static final String SEQUENCE_TABLE = "SEQUENCE";

    private MetaService metaService;
    private Table table;
    private StoreKvTxn store;
    private KeyValueCodec codec;

    private Map<String, SequenceGenerator> queueMap;

    private SequenceService() {
        try {
            InfoSchemaService infoSchemaService = InfoSchemaService.root();
            while (!infoSchemaService.prepare() || DdlContext.INSTANCE.waiting.get()) {
                Utils.sleep(5000L);
            }
            this.metaService = MetaService.root();
            this.table = getTable(SEQUENCE_TABLE);
            CommonId tableId = table.getTableId();
            this.store = new StoreKvTxn(tableId, getRegionId(tableId));
            this.codec = CodecService.getDefault().createKeyValueCodec(this.table.getCodecVersion(),
                getPartId(tableId, store.getRegionId()), table.tupleType(), table.keyMapping()
            );
            this.queueMap = new ConcurrentHashMap<>();
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage());
        }
    }

    @Override
    public boolean existsSequence(String name) {
        Object[] values = new Object[table.columns.size()];
        values[0] = name;
        KeyValue keyValue = store.get(codec.encodeKey(values));
        return keyValue.getValue() != null && keyValue.getValue().length != 0;
    }

    @Override
    public void createSequence(SequenceDefinition sequence) {
        Object[] row = new Object[table.columns.size()];
        for (int i = 0; i < table.columns.size(); i++) {
            Column column = table.columns.get(i);
            switch (column.getName()) {
                case "NAME":
                    row[i] = sequence.getName();
                    break;
                case "INCREMENT":
                    if (sequence.getIncrement() == 0) {
                        row[i] = 1;
                    } else {
                        row[i] = sequence.getIncrement();
                    }
                    break;
                case "MINVALUE":
                    row[i] = sequence.getMinvalue();
                    break;
                case "MAXVALUE":
                    row[i] = sequence.getMaxvalue();
                    break;
                case "START":
                    row[i] = sequence.getStart();
                    break;
                case "CACHE":
                    if (sequence.getCache() == 0) {
                        row[i] = 1000;
                    } else {
                        row[i] = sequence.getCache();
                    }
                    break;
                case "CYCLE":
                    row[i] = sequence.isCycle();
                    break;
                default:
                    throw new IllegalArgumentException(column.getName());
            }
        }
        KeyValue keyValue = codec.encode(row);
        store.insert(keyValue.getKey(), keyValue.getValue());
        LogUtils.debug(log, "create sequence: {}", sequence);
    }

    @Override
    public void dropSequence(String name) {
        Object[] keyObj = new Object[table.columns.size()];
        keyObj[0] = name;
        byte[] keys = codec.encodeKey(keyObj);
        if (store.get(keys).getValue() == null) {
            return;
        }
        store.del(keys);
        LogUtils.debug(log, "drop sequence: {}", name);
    }

    @Override
    public Long nextVal(String name) {
        SequenceGenerator generator = queueMap.computeIfAbsent(name, k -> {
            SequenceDefinition definition = getSequence(name);
            if (definition == null) {
                return null;
            }
            return new SequenceGenerator(definition);
        });
        return generator == null ? null : generator.next();
    }

    @Override
    public Long setVal(String name, Long seq) {
        return null;
    }

    @Override
    public Long currVal(String name) {
        return null;
    }

    @Override
    public Long lastVal(String name) {
        return null;
    }

    @Override
    public SequenceDefinition getSequence(String name) {
        Object[] keyObj = new Object[table.columns.size()];
        keyObj[0] = name;
        byte[] keys = codec.encodeKey(keyObj);
        KeyValue keyValue = store.get(keys);
        if (keyValue.getValue() == null) {
            return null;
        }
        Object[] row = codec.decode(keyValue);
        return new SequenceDefinition(
            (String) row[0],
            (int) row[1],
            (long) row[2],
            (long) row[3],
            (long) row[4],
            (int) row[5],
            (boolean) row[6]);
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
            } catch (InterruptedException ignore) {

            }
        }
        throw new RuntimeException("init sequence error");
    }

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
}
