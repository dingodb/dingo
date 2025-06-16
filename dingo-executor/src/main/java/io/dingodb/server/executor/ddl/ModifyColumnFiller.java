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

package io.dingodb.server.executor.ddl;

import com.google.common.collect.Iterators;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.ddl.ReorgBackFillTask;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.Services;
import io.dingodb.exec.converter.ModifyTypeConverter;
import io.dingodb.exec.transaction.base.CacheToObject;
import io.dingodb.exec.transaction.base.TxnLocalData;
import io.dingodb.exec.transaction.util.TransactionCacheToMutation;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.exception.WriteConflictException;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.dingodb.common.CommonId.CommonType.FILL_BACK;
import static io.dingodb.common.util.NoBreakFunctions.wrap;
import static io.dingodb.exec.transaction.util.TransactionUtil.max_pre_write_count;

@Slf4j
public class ModifyColumnFiller extends IndexAddFiller {

    DingoType dingoType;
    protected CommonId replicaId;

    private boolean switchPos = false;
    private int removePos = -1;
    private int addPos = -1;

    @Override
    public NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> getRegionList() {
        return MetaService.root().getRangeDistribution(replicaId);
    }

    @Override
    public void initFiller() {
        super.initFiller();
        replicaId = indexTable.tableId;
        Properties properties = indexTable.getProperties();
        if (properties != null) {
            switchPos = properties.getOrDefault("switchPos", "false").equals("true");
            if (switchPos) {
                removePos = Integer.parseInt(properties.getProperty("removePos", "-1"));
                addPos = Integer.parseInt(properties.getProperty("addPos", "-1"));
            }
        }
        LogUtils.info(log, "replicaTableId:{}", replicaId);
    }

    @Override
    public boolean preWritePrimary(ReorgBackFillTask task) {
        ownerRegionId = task.getRegionId().seq;
        txnId = new CommonId(CommonId.CommonType.TRANSACTION, 0, task.getStartTs());
        txnIdKey = txnId.encode();
        commitTs = TsoService.getDefault().tso();
        table = InfoSchemaService.root().getTableDef(task.getTableId().domain, task.getTableId().seq);
        indexTable = InfoSchemaService.root().getIndexDef(task.getTableId().domain, task.getTableId().seq,
            task.getIndexId().seq);

        this.dingoType = indexTable.tupleType();
        initFiller();
        LogUtils.info(log, "modify column filler dingo type:{}", dingoType);
        indexCodec = CodecService.getDefault()
            .createKeyValueCodec(indexTable.getCodecVersion(), indexTable.version, indexTable.tupleType(),
            indexTable.keyMapping());
        ps = PartitionService.getService(
            Optional.ofNullable(indexTable.getPartitionStrategy())
                .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
        // reorging when region split
        StoreInstance kvStore = Services.KV_STORE.getInstance(task.getTableId(), task.getRegionId());
        KeyValueCodec codec  = CodecService.getDefault().createKeyValueCodec(table.getCodecVersion(),
            table.getVersion(), table.tupleType(), table.keyMapping()
        );
        Iterator<KeyValue> iterator = kvStore.txnScanWithoutStream(
            task.getStartTs(),
            new StoreInstance.Range(task.getStart(), task.getEnd(), task.isWithStart(), task.isWithEnd()),
            50000
        );
        tupleIterator = Iterators.transform(iterator,
            wrap(codec::decode)::apply
        );
        boolean preRes = false;
        while (tupleIterator.hasNext()) {
            Object[] tuples = tupleIterator.next();

            Object[] tuplesTmp = getNewTuples(colLen, tuples);

            KeyValue keyValue = wrap(indexCodec::encode).apply(tuplesTmp);
            if (keyValue == null) {
                throw new RuntimeException("modify column encode error");
            }
            NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges =
                getRegionList();
            CommonId partId = ps.calcPartId(keyValue.getKey(), ranges);
            CodecService.getDefault().setId(keyValue.getKey(), partId.domain);

            CommonId tableId = indexTable.tableId;
            int op = Op.PUT.getCode();
            byte[] key = keyValue.getKey();
            byte[] value = keyValue.getValue();
            primaryObj = new CacheToObject(TransactionCacheToMutation.cacheToMutation(
                op, key, value,0L, tableId, partId, txnId), tableId, partId
            );
            try {
                preWritePrimaryKey(primaryObj);
            } catch (WriteConflictException e) {
                conflict.incrementAndGet();
                continue;
            }
            preRes = true;
            break;
        }
        return preRes;
    }

    @Override
    public BackFillResult backFillDataInTxn(ReorgBackFillTask task, boolean withCheck) {
        CommonId tableId = task.getTableId();
        Iterator<Object[]> tupleIterator;
        if (task.getRegionId().seq != ownerRegionId) {
            tupleIterator = getIterator(task, tableId, withCheck);
        } else {
            tupleIterator = this.tupleIterator;
        }
        long start = System.currentTimeMillis();
        Map<String, TxnLocalData> caches = new TreeMap<>();
        long scanCount = 0;
        while (tupleIterator.hasNext()) {
            scanCount += 1;
            if (scanCount % 409600 == 0) {
                LogUtils.info(log, "bckFillDataInTxn loop count:{}, regionId:{}", scanCount, task.getRegionId());
            }
            Object[] tuple = tupleIterator.next();
            //Object[] tuplesTmp = columnIndices.stream().map(i -> tuple[i]).toArray();
            Object[] tuplesTmp = getNewTuples(colLen, tuple);
            TxnLocalData txnLocalData = getTxnLocalData(tuplesTmp);
            if (indexTable.unique && ByteArrayUtils.compare(txnLocalData.getKey(), primaryKey, 1) == 0) {
                duplicateKey(tuplesTmp);
            }
            String cacheKey = Base64.getEncoder().encodeToString(txnLocalData.getKey());
            if (!caches.containsKey(cacheKey)) {
                caches.put(cacheKey, txnLocalData);
            } else if (indexTable.unique) {
                duplicateKey(tuplesTmp);
            }
            if (caches.size() % max_pre_write_count == 0) {
                try {
                    List<TxnLocalData> txnLocalDataList = new ArrayList<>(caches.values());
                    preWriteSecondSkipConflict(txnLocalDataList);
                } finally {
                    caches.clear();
                }
            }
        }

        this.scanCount.addAndGet(scanCount);
        BackFillResult backFillResult = BackFillResult.builder().scanCount(scanCount).build();
        Collection<TxnLocalData> tupleList = caches.values();
        if (tupleList.isEmpty()) {
            return backFillResult;
        }
        List<TxnLocalData> txnLocalDataList = new ArrayList<>(tupleList);
        preWriteSecondSkipConflict(txnLocalDataList);
        backFillResult.addCount(tupleList.size());
        LogUtils.info(log, "pre write second, regionId:{}, iterator cost:{}ms, scanCount:{}",
            task.getRegionId(), (System.currentTimeMillis() - start), scanCount);
        doneRegionIdList.add(task.getRegionId());
        return backFillResult;
    }

    @NonNull
    public Object[] getNewTuples(int colLen, Object[] tuples) {
        if (switchPos) {
            List<Object> valList = new ArrayList<>();
            for (Object valItem : tuples) {
                valList.add(valItem);
            }
            Object removeVal = valList.remove(removePos);
            valList.add(addPos, removeVal);
            tuples = valList.toArray();
        }
        return transformType(tuples);
    }

    @Override
    public TxnLocalData getTxnLocalData(Object[] tuples) {
        KeyValue keyValue = wrap(indexCodec::encode).apply(tuples);
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges =
            getRegionList();
        CommonId partId = ps.calcPartId(keyValue.getKey(), ranges);
        CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
        return TxnLocalData.builder()
            .dataType(FILL_BACK)
            .txnId(txnId)
            .tableId(indexTable.tableId)
            .partId(partId)
            .op(Op.PUT)
            .key(keyValue.getKey())
            .value(keyValue.getValue())
            .build();
    }

    public Object[] transformType(Object[] tuples) {
        return (Object[]) dingoType.convertFrom(tuples, new ModifyTypeConverter());
    }

    @Override
    public BackFillResult backFillDataInTxnWithCheck(ReorgBackFillTask task, boolean withCheck) {
        CommonId tableId = task.getTableId();
        Iterator<Object[]> tupleIterator = getIterator(task, tableId, withCheck);
        StoreInstance cache = Services.LOCAL_STORE.getInstance(null, null);
        long start = System.currentTimeMillis();
        Map<String, TxnLocalData> caches = new TreeMap<>();
        long scanCount = 0;
        while (tupleIterator.hasNext()) {
            scanCount += 1;
            Object[] tuple = tupleIterator.next();
            Object[] tuplesTmp = getNewTuples(colLen, tuple);
            TxnLocalData txnLocalData = getTxnLocalData(tuplesTmp);
            if (indexTable.unique) {
                if (ByteArrayUtils.compare(txnLocalData.getKey(), primaryKey, 1) == 0) {
                    duplicateKey(tuplesTmp);
                } else {
                    byte[] key = getLocalKey(txnLocalData.getKey(), txnLocalData.getPartId().encode());
                    if (cache.get(key) != null) {
                        continue;
                    }
                }
            }
            String cacheKey = Base64.getEncoder().encodeToString(txnLocalData.getKey());
            if (!caches.containsKey(cacheKey)) {
                caches.put(cacheKey, txnLocalData);
            } else if (indexTable.unique) {
                duplicateKey(tuplesTmp);
            }
            if (caches.size() % max_pre_write_count == 0) {
                try {
                    List<TxnLocalData> txnLocalDataList = new ArrayList<>(caches.values());
                    preWriteSecondSkipConflict(txnLocalDataList);
                } finally {
                    caches.clear();
                }
            }
        }

        this.scanCount.addAndGet(scanCount);
        BackFillResult backFillResult = BackFillResult.builder().scanCount(scanCount).build();
        Collection<TxnLocalData> tupleList = caches.values();
        if (tupleList.isEmpty()) {
            return backFillResult;
        }
        List<TxnLocalData> txnLocalDataList = new ArrayList<>(tupleList);
        preWriteSecondSkipConflict(txnLocalDataList);
        backFillResult.addCount(tupleList.size());
        LogUtils.info(log, "pre write second with check, iterator cost:{}ms, scanCount:{}, regionId:{}",
            (System.currentTimeMillis() - start), scanCount, task.getRegionId());
        doneRegionIdList.add(task.getRegionId());
        return backFillResult;
    }

}
