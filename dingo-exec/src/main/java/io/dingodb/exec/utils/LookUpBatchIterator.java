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

package io.dingodb.exec.utils;

import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.Services;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;

public class LookUpBatchIterator implements Iterator<Object[]> {
    private final Iterator<Object[]> sourceIterator;
    private final TupleMapping keyMapping;
    private final Table table;
    private final KeyValueCodec lookupCodec;
    private final CommonId tableId;
    private final long scanTs;
    private final long timeout;
    private final int batchSize;

    private final List<Object[]> currentBatch;
    private Iterator<Object[]> currentResultIterator;

    public LookUpBatchIterator(Iterator<Object[]> sourceIterator,
                               TupleMapping keyMapping,
                               Table table,
                               KeyValueCodec lookupCodec,
                               CommonId tableId,
                               long scanTs,
                               long timeout,
                               int batchSize) {
        this.sourceIterator = sourceIterator;
        this.keyMapping = keyMapping;
        this.table = table;
        this.lookupCodec = lookupCodec;
        this.tableId = tableId;
        this.scanTs = scanTs;
        this.timeout = timeout;
        this.batchSize = batchSize;
        this.currentBatch = new ArrayList<>(batchSize);
    }

    @Override
    public boolean hasNext() {
        if (currentResultIterator != null && currentResultIterator.hasNext()) {
            return true;
        }

        currentBatch.clear();

        while (sourceIterator.hasNext() && currentBatch.size() < batchSize) {
            currentBatch.add(sourceIterator.next());
        }

        if (currentBatch.isEmpty()) {
            return false;
        }

        List<Object[]> results = lookUp(currentBatch, keyMapping, table, lookupCodec, tableId, scanTs, timeout);
        currentResultIterator = results.iterator();

        return currentResultIterator.hasNext();
    }

    @Override
    public Object[] next() {
        if (!hasNext()) {
            throw new NoSuchElementException("LookUpBatchIterator hash next is false");
        }
        return currentResultIterator.next();
    }

    public List<Object[]> lookUp(List<Object[]> tupleList,
                                        TupleMapping keyMapping,
                                        Table table,
                                        KeyValueCodec lookupCodec,
                                        CommonId tableId,
                                        long scanTs,
                                        long timeout) {
        Map<CommonId, List<byte[]>> keyList = new HashMap<>();
        for (Object[] tuples : tupleList) {
            NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges =
                MetaService.root().getRangeDistribution(table.tableId);
            Object[] keyTuples = new Object[table.getColumns().size()];
            for (int i = 0; i < keyMapping.getMappings().length; i++) {
                keyTuples[keyMapping.get(i)] = tuples[i];
            }
            byte[] keys = lookupCodec.encodeKey(keyTuples);
            CommonId regionId = PartitionService.getService(
                    Optional.ofNullable(table.getPartitionStrategy())
                        .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
                .calcPartId(keys, ranges);

            keys = CodecService.getDefault().setId(keys, regionId.domain);
            if (keyList.containsKey(regionId)) {
                keyList.get(regionId).add(keys);
            } else {
                List<byte[]> valList = new ArrayList<>();
                valList.add(keys);
                keyList.put(regionId, valList);
            }
        }

        List<Object[]> lookupResult = new ArrayList<>();
        for (Map.Entry<CommonId, List<byte[]>> item : keyList.entrySet()) {
            StoreInstance store = Services.KV_STORE.getInstance(tableId, item.getKey());
            List<KeyValue> res = store.txnGet(scanTs, item.getValue(), timeout);
            lookupResult.addAll(res.stream().map(lookupCodec::decode).toList());
        }
        keyList.clear();
        tupleList.clear();
        return lookupResult;
    }
}
