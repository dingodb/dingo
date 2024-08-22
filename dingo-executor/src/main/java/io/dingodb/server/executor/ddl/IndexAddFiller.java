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

import com.codahale.metrics.Timer;
import com.google.common.collect.Iterators;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.ddl.ReorgBackFillTask;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.scalar.BooleanType;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Utils;
import io.dingodb.exec.Services;
import io.dingodb.exec.transaction.base.CacheToObject;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.base.TxnLocalData;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.transaction.params.CommitParam;
import io.dingodb.exec.transaction.params.PreWriteParam;
import io.dingodb.exec.transaction.util.TransactionCacheToMutation;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.exec.transaction.util.Txn;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.server.executor.service.BackFiller;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.Mutation;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import io.dingodb.store.api.transaction.exception.WriteConflictException;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;
import io.dingodb.common.metrics.DingoMetrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.dingodb.common.CommonId.CommonType.FILL_BACK;
import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public class IndexAddFiller implements BackFiller {
    public Table table;
    protected IndexTable indexTable;
    List<Integer> columnIndices;
    int colLen;
    KeyValueCodec indexCodec;
    PartitionService ps;
    CommonId txnId;
    byte[] primaryKey;
    long timeOut = 50000;
    Future<?> future;
    public static final long preBatch = 1024;

    long ownerRegionId;
    Iterator<Object[]> tupleIterator;
    private static final DingoType dingoType = new BooleanType(true);
    CacheToObject primaryObj = null;
    int isolationLevel = IsolationLevel.ReadCommitted.getCode();
    long commitTs;
    byte[] txnIdKey;

    AtomicLong conflict = new AtomicLong(0);
    AtomicLong addCount = new AtomicLong(0);
    AtomicLong scanCount = new AtomicLong(0);
    AtomicLong commitCnt = new AtomicLong(0);

    public IndexAddFiller() {
    }

    @Override
    public boolean preWritePrimary(ReorgBackFillTask task) {
        ownerRegionId = task.getRegionId().seq;
        txnId = new CommonId(CommonId.CommonType.TRANSACTION, 0, task.getStartTs());
        txnIdKey = txnId.encode();
        commitTs = TsoService.getDefault().tso();
        table = InfoSchemaService.root().getTableDef(task.getTableId().domain, task.getTableId().seq);
        indexTable = InfoSchemaService.root().getIndexDef(task.getTableId().seq, task.getIndexId().seq);
        columnIndices = table.getColumnIndices(indexTable.columns.stream()
            .map(Column::getName)
            .collect(Collectors.toList()));
        indexCodec = CodecService.getDefault()
            .createKeyValueCodec(indexTable.version, indexTable.tupleType(), indexTable.keyMapping());
        ps = PartitionService.getService(
            Optional.ofNullable(indexTable.getPartitionStrategy())
                .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
        // reorging when region split
        StoreInstance kvStore = Services.KV_STORE.getInstance(task.getTableId(), task.getRegionId());
        KeyValueCodec codec  = CodecService.getDefault().createKeyValueCodec(table.getVersion(), table.tupleType(), table.keyMapping());
        Iterator<KeyValue> iterator = kvStore.txnScan(
            task.getStartTs(),
            new StoreInstance.Range(task.getStart(), task.getEnd(), task.isWithStart(), task.isWithEnd()),
            50000
        );
        tupleIterator = Iterators.transform(iterator,
            wrap(codec::decode)::apply
        );
        LogUtils.info(log, "[ddl] index reorg pre write primary start, index name:{}", indexTable.getName());
        boolean preRes = false;
        while (tupleIterator.hasNext()) {
            Object[] tuples = tupleIterator.next();

            Object[] tuplesTmp = columnIndices.stream().map(i -> tuples[i]).toArray();
            KeyValue keyValue = wrap(indexCodec::encode).apply(tuplesTmp);
            NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges =
                MetaService.root().getRangeDistribution(indexTable.tableId);
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
                long start = System.currentTimeMillis();
                preWritePrimaryKey(primaryObj);
                long sub = System.currentTimeMillis() - start;
                LogUtils.info(log, "[ddl] index reorg pre write primary, cost:{}", sub);
            } catch (WriteConflictException e) {
                conflict.incrementAndGet();
                continue;
            }
            preRes = true;
            break;
        }
        return preRes;
    }

    protected void preWritePrimaryKey(CacheToObject cacheToObject) {
        primaryKey = cacheToObject.getMutation().getKey();
        // 2、call sdk preWritePrimaryKey
        TxnPreWrite txnPreWrite = TxnPreWrite.builder()
            .isolationLevel(IsolationLevel.of(
                IsolationLevel.ReadCommitted.getCode()
            ))
            .mutations(Collections.singletonList(cacheToObject.getMutation()))
            .primaryLock(primaryKey)
            .startTs(txnId.seq)
            .lockTtl(TransactionManager.lockTtlTm())
            .txnSize(1L)
            .tryOnePc(false)
            .maxCommitTs(0L)
            .lockExtraDatas(TransactionUtil.toLockExtraDataList(cacheToObject.getTableId(), cacheToObject.getPartId(), txnId,
                TransactionType.OPTIMISTIC.getCode(), 1))
            .build();
        try {
            StoreInstance store = Services.KV_STORE.getInstance(cacheToObject.getTableId(), cacheToObject.getPartId());
            //LogUtils.info(log, "[ddl] index reorg call rpc start, startTs:{}", txnId.seq);
            this.future = store.txnPreWritePrimaryKey(txnPreWrite, timeOut);
            //LogUtils.info(log, "[ddl] index reorg call rpc end, startTs:{}", txnId.seq);
        } catch (RegionSplitException e) {
            LogUtils.error(log, e.getMessage(), e);

            boolean prewriteResult = false;
            int i = 0;
            while (!prewriteResult) {
                i ++;
                try {
                    CommonId regionId = TransactionUtil.singleKeySplitRegionId(
                        cacheToObject.getTableId(),
                        txnId,
                        cacheToObject.getMutation().getKey()
                    );
                    StoreInstance store = Services.KV_STORE.getInstance(cacheToObject.getTableId(), regionId);
                    this.future = store.txnPreWritePrimaryKey(txnPreWrite, timeOut);
                    prewriteResult = true;
                } catch (RegionSplitException e1) {
                    Utils.sleep(100);
                    LogUtils.error(log, "prewrite primary region split, retry count:" + i);
                }
            }
        }
        if (this.future == null) {
            throw new RuntimeException(txnId + " future is null "
                + cacheToObject.getPartId() + ",preWritePrimaryKey false,PrimaryKey:"
                + Arrays.toString(primaryKey));
        }
    }

    @Override
    public BackFillResult backFillDataInTxn(ReorgBackFillTask task) {
        CommonId tableId = task.getTableId();
        Iterator<Object[]> tupleIterator;
        long start = System.currentTimeMillis();
        if (task.getRegionId().seq != ownerRegionId) {
            StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, task.getRegionId());
            KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(table.getVersion(), table.tupleType(), table.keyMapping());
            Iterator<KeyValue> iterator = null;
            int retry = 3;
            while (retry -- > 0) {
                try {
                    iterator = kvStore.txnScan(
                        task.getStartTs(),
                        new StoreInstance.Range(task.getStart(), task.getEnd(), task.isWithStart(), task.isWithEnd()),
                        50000
                    );
                } catch (RegionSplitException ignored) {
                }
            }
            if (iterator == null) {
                throw new RuntimeException("index reorg scan error");
            }
            tupleIterator = Iterators.transform(iterator,
                wrap(codec::decode)::apply
            );
        } else {
            tupleIterator = this.tupleIterator;
        }
        long end = System.currentTimeMillis();
        LogUtils.info(log, "pre write second, init iterator cost:{}ms", (end - start));
        Map<String, TxnLocalData> caches = new TreeMap<>();
        //int batchCnt = 1024;
        long scanCount = 0;
        while (tupleIterator.hasNext()) {
            scanCount += 1;
            Object[] tuple = tupleIterator.next();
            TxnLocalData txnLocalData = getTxnLocalData(tuple);
            String cacheKey = Base64.getEncoder().encodeToString(txnLocalData.getKey());
            if (!caches.containsKey(cacheKey)) {
                caches.put(cacheKey, txnLocalData);
            }
            if (caches.size() % preBatch == 0) {
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
        LogUtils.info(log, "pre write second, iterator cost:{}ms", (System.currentTimeMillis() - end));
        return backFillResult;
    }

    @Override
    public boolean commitPrimary() {
        Txn txn = new Txn(txnId, false, 0, timeOut);
        txn.setPrimaryKey(primaryKey);
        txn.setCommitTs(this.commitTs);
        boolean res = txn.commitPrimaryData(primaryObj);
        if (res) {
            this.commitTs = txn.getCommitTs();
        }
        return res;
    }

    @Override
    public boolean commitSecond() {
        StoreInstance cache = Services.LOCAL_STORE.getInstance(null, null);
        byte[] prefix = new byte[1 + txnIdKey.length];
        prefix[0] = (byte)FILL_BACK.getCode();
        System.arraycopy(txnIdKey, 0, prefix, 1, txnIdKey.length);
        Iterator<KeyValue> iterator = cache.scan(prefix);

        Map<String, KeyValue> caches = new TreeMap<>();
        int batchCnt = 1024;
        while (iterator.hasNext()) {
            commitCnt.incrementAndGet();
            KeyValue keyValue = iterator.next();
            String key = Base64.getEncoder().encodeToString(keyValue.getKey());
            if (!caches.containsKey(key)) {
                caches.put(key, keyValue);
            }
            if (caches.size() % batchCnt == 0) {
                try {
                    commitSecondData(caches.values());
                } finally {
                    caches.clear();
                }
            }
        }
        if (!caches.values().isEmpty()) {
            commitSecondData(caches.values());
        }
        LogUtils.info(log, "[ddl] index reorg conflict cnt:{}", conflict);
        return true;
    }

    @Override
    public long getScanCount() {
        return this.scanCount.get();
    }

    @Override
    public long getAddCount() {
        return this.addCount.get();
    }

    @Override
    public long getCommitCount() {
        return this.commitCnt.get();
    }

    @Override
    public long getConflictCount() {
        return this.conflict.get();
    }

    public void commitSecondData(Collection<KeyValue> secondData) {
        CommitParam param = new CommitParam(dingoType, isolationLevel, txnId.seq,
            commitTs, primaryKey, TransactionType.OPTIMISTIC);
        param.init(null);
        for (KeyValue keyValue : secondData) {
            CommonId tableId = indexTable.tableId;
            int from = 1;
            Arrays.copyOfRange(keyValue.getKey(), from, from += CommonId.LEN);
            CommonId newPartId = CommonId.decode(Arrays.copyOfRange(keyValue.getKey(), from, from += CommonId.LEN));
            byte[] key = new byte[keyValue.getKey().length - from];
            System.arraycopy(keyValue.getKey(), from , key, 0, key.length);
            CommonId partId = param.getPartId();
            if (partId == null) {
                partId = newPartId;
                param.setPartId(partId);
                param.setTableId(tableId);
                param.addKey(key);
            } else if (partId.equals(newPartId)) {
                param.addKey(key);
                if (param.getKeys().size() == TransactionUtil.max_pre_write_count) {
                    boolean result = Txn.txnCommit(param, txnId, tableId, partId);
                    if (!result) {
                        throw new RuntimeException(txnId + " " + partId + ",txnCommit false,PrimaryKey:"
                            + Arrays.toString(param.getPrimaryKey()));
                    }
                    param.getKeys().clear();
                    param.setPartId(null);
                }
            } else {
                boolean result = Txn.txnCommit(param, txnId, param.getTableId(), partId);
                if (!result) {
                    throw new RuntimeException(txnId + " " + partId + ",txnCommit false,PrimaryKey:"
                        + Arrays.toString(param.getPrimaryKey()));
                }
                param.getKeys().clear();
                param.addKey(key);
                param.setPartId(newPartId);
                param.setTableId(tableId);
            }
        }
        if (!param.getKeys().isEmpty()) {
            boolean result = Txn.txnCommit(param, txnId, param.getTableId(), param.getPartId());
            if (!result) {
                throw new RuntimeException(txnId + " " + param.getPartId()
                    + ",txnCommit false,PrimaryKey:" + Arrays.toString(param.getPrimaryKey()));
            }
        }
    }

    public TxnLocalData getTxnLocalData(Object[] tuples) {
        Object[] tuplesTmp = columnIndices.stream().map(i -> tuples[i]).toArray();
        KeyValue keyValue = wrap(indexCodec::encode).apply(tuplesTmp);
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges =
            MetaService.root().getRangeDistribution(indexTable.tableId);
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

    protected void preWriteSecondSkipConflict(List<TxnLocalData> secondList) {
        try {
            Timer.Context timeCtx = DingoMetrics.getTimeContext("ReorgPreSecond" + secondList.size());
            preWriteSecondKey(secondList);
            timeCtx.stop();
        } catch (WriteConflictException e) {
            conflict.incrementAndGet();
            if (e.doneCnt > 0) {
                secondList = secondList.subList(e.doneCnt, secondList.size());
            }
            handleConflict(secondList, e.key);
            preWriteSecondSkipConflict(secondList);
        }
    }

    private static void handleConflict(List<TxnLocalData> tupleList, byte[] key) {
        tupleList.removeIf(txnLocalData -> {
            byte[] conflictKey = txnLocalData.getKey();
            return ByteArrayUtils.compare(key, conflictKey) == 0;
        });
    }

    private void removeDoneKey(List<TxnLocalData> secondList, List<Mutation> mutationList) {
        Timer.Context timeCtx = DingoMetrics.getTimeContext("removeDoneKey");
        StoreInstance localStore = Services.LOCAL_STORE.getInstance(null, null);
        addCount.addAndGet(mutationList.size());
        secondList.forEach(txnLocalData -> {
            byte[] doneKey = txnLocalData.getKey();
            boolean res =  mutationList.stream()
                .anyMatch(mutation -> ByteArrayUtils.compare(mutation.getKey(), doneKey) == 0);
            if (res) {
                byte[] key = txnLocalData.getKey();
                byte[] partId = txnLocalData.getPartId().encode();
                byte[] ek = new byte[key.length + 1 + txnIdKey.length + partId.length];
                ek[0] = (byte) FILL_BACK.getCode();
                System.arraycopy(txnIdKey, 0, ek, 1, txnIdKey.length);
                System.arraycopy(partId, 0, ek, 1 + txnIdKey.length, partId.length);
                System.arraycopy(key, 0, ek, 1 + txnIdKey.length + partId.length, key.length);
                KeyValue extraKeyValue = new KeyValue(ek, null);
                localStore.put(extraKeyValue);
            }
        });
        timeCtx.stop();
    }

    private void preWriteSecondKey(List<TxnLocalData> secondList) {
        if (secondList.isEmpty()) {
            return;
        }
        long start = System.currentTimeMillis();
        PreWriteParam param = new PreWriteParam(dingoType, primaryKey, txnId.seq,
            isolationLevel, TransactionType.OPTIMISTIC, timeOut);
        param.init(null);
        int preDoneCnt = 0;
        try {
            for (int i = 0; i < secondList.size(); i++) {
                TxnLocalData txnLocalData = secondList.get(i);
                CommonId newPartId = txnLocalData.getPartId();
                int op = txnLocalData.getOp().getCode();
                byte[] key = txnLocalData.getKey();
                byte[] value = txnLocalData.getValue();
                Mutation mutation = TransactionCacheToMutation.cacheToMutation(op, key, value, 0L, indexTable.tableId, newPartId, txnId);
                CommonId partId = param.getPartId();
                if (partId == null) {
                    partId = newPartId;
                    param.setPartId(partId);
                    param.setTableId(indexTable.tableId);
                    param.addMutation(mutation);
                } else if (partId.equals(newPartId)) {
                    param.addMutation(mutation);
                    if (param.getMutations().size() == preBatch) {
                        long sub = System.currentTimeMillis() - start;
                        LogUtils.debug(log, "pre write 1024 cost:{}", sub);
                        boolean result = Txn.txnPreWrite(param, txnId, indexTable.tableId, partId);
                        sub = System.currentTimeMillis() - start;
                        LogUtils.debug(log, "pre write 1024 cost:{}", sub);

                        if (!result) {
                            throw new RuntimeException(txnId + " " + partId + ",txnPreWrite false,PrimaryKey:"
                                + Arrays.toString(param.getPrimaryKey()));
                        }
                        preDoneCnt += param.getMutations().size();
                        removeDoneKey(secondList, param.getMutations());
                        param.getMutations().clear();
                        param.setPartId(null);
                        long tmp = System.currentTimeMillis();
                        sub = tmp - start;
                        LogUtils.debug(log, "pre write and remove doen key 1024 cost:{}", sub);
                        start = tmp;
                    }
                } else {
                    LogUtils.info(log, "pre write diff partId");
                    boolean result = Txn.txnPreWrite(param, txnId, param.getTableId(), partId);
                    if (!result) {
                        throw new RuntimeException(txnId + " " + partId + ",txnPreWrite false,PrimaryKey:"
                            + Arrays.toString(param.getPrimaryKey()));
                    }
                    preDoneCnt += param.getMutations().size();
                    removeDoneKey(secondList, param.getMutations());
                    param.getMutations().clear();
                    param.addMutation(mutation);
                    param.setPartId(newPartId);
                    param.setTableId(indexTable.tableId);
                }
            }
        } catch (WriteConflictException e) {
            e.doneCnt += preDoneCnt;
            throw e;
        }
        long end1 = System.currentTimeMillis();
        if (!param.getMutations().isEmpty()) {
            try {
                boolean result = Txn.txnPreWrite(param, txnId, param.getTableId(), param.getPartId());
                if (!result) {
                    throw new RuntimeException(txnId + " " + param.getPartId() + ",txnPreWrite false,PrimaryKey:"
                        + Arrays.toString(param.getPrimaryKey()));
                }
                preDoneCnt += param.getMutations().size();
                removeDoneKey(secondList, param.getMutations());
                param.getMutations().clear();
            } catch (WriteConflictException e) {
                e.doneCnt += preDoneCnt;
                throw e;
            }
        }
        LogUtils.debug(log, "index reorg mod done, cost:{}", (System.currentTimeMillis() - end1));
    }
}
