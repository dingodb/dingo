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
import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.scalar.BooleanType;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Pair;
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
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.partition.base.RangePartitionService;
import io.dingodb.server.executor.service.BackFiller;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.Mutation;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import io.dingodb.store.api.transaction.exception.WriteConflictException;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;

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
import static io.dingodb.exec.transaction.util.TransactionUtil.max_pre_write_count;
import static io.dingodb.exec.transaction.util.Txn.txnCommit;
import static io.dingodb.exec.transaction.util.Txn.txnPreWriteWithRePartId;

@Slf4j
public class IndexAddFiller implements BackFiller {
    public Table table;
    protected long schemaId;
    protected IndexTable indexTable;
    List<Integer> columnIndices;
    int colLen;
    KeyValueCodec indexCodec;
    PartitionService ps;
    CommonId txnId;
    byte[] primaryKey;
    byte[] originPrimaryKey;
    long timeOut = 50000;
    Future<?> future;

    long ownerRegionId;
    Iterator<Object[]> tupleIterator;
    private static final DingoType dingoType = new BooleanType(true);
    CacheToObject primaryObj = null;
    int isolationLevel = IsolationLevel.ReadCommitted.getCode();
    long commitTs;
    byte[] txnIdKey;
    List<CommonId> doneRegionIdList = new ArrayList<>();

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
        indexTable = InfoSchemaService.root().getIndexDef(task.getTableId().domain, task.getTableId().seq,
            task.getIndexId().seq);
        initFiller();
        columnIndices = table.getColumnIndices(indexTable.columns.stream()
            .map(Column::getName)
            .collect(Collectors.toList()));
        indexCodec = CodecService.getDefault()
            .createKeyValueCodec(indexTable.getCodecVersion(), indexTable.version,
            indexTable.tupleType(), indexTable.keyMapping());
        ps = PartitionService.getService(
            Optional.ofNullable(indexTable.getPartitionStrategy())
                .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
        // reorging when region split
        StoreInstance kvStore = Services.KV_STORE.getInstance(task.getTableId(), task.getRegionId());
        KeyValueCodec codec  = CodecService.getDefault().createKeyValueCodec(
            table.getCodecVersion(), table.getVersion(), table.tupleType(), table.keyMapping()
        );
        Iterator<KeyValue> iterator = kvStore.txnScanWithoutStream(
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
                getRegionList();
            CommonId partId = ps.calcPartId(keyValue.getKey(), ranges);
            CodecService.getDefault().setId(keyValue.getKey(), partId.domain);

            CommonId tableId = indexTable.tableId;
            int op;
            if (indexTable.unique) {
                op = Op.PUTIFABSENT.getCode();
            } else {
                op = Op.PUT.getCode();
            }
            byte[] key = keyValue.getKey();
            byte[] value = keyValue.getValue();
            primaryObj = new CacheToObject(TransactionCacheToMutation.cacheToMutation(
                op, key, value,0L, tableId, partId, txnId), tableId, partId
            );
            try {
                preWritePrimaryKey(primaryObj);
                originPrimaryKey = codec.encodeKey(tuples);
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
            Object[] tuplesTmp = columnIndices.stream().map(i -> tuple[i]).toArray();
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
            Object[] tuplesTmp = columnIndices.stream().map(i -> tuple[i]).toArray();
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

    @Override
    public boolean commitPrimary() {
        if (primaryObj == null) {
            return true;
        }
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
        if (primaryObj == null) {
            return true;
        }
        Iterator<KeyValue> iterator = getLocalIterator();

        CommitParam param = new CommitParam(dingoType, isolationLevel, txnId.seq,
            commitTs, primaryKey, TransactionType.OPTIMISTIC);
        param.init(null);
        while (iterator.hasNext()) {
            commitCnt.incrementAndGet();
            if (commitCnt.get() % 409600 == 0) {
                LogUtils.info(log, "commitSecond cnt:{}", commitCnt.get());
            }
            KeyValue keyValue = iterator.next();
            CommonId tableId = indexTable.tableId;
            int from = 1;
            //Arrays.copyOfRange(keyValue.getKey(), from, from += CommonId.LEN);
            from += CommonId.LEN;
            CommonId newPartId = CommonId.decode(Arrays.copyOfRange(keyValue.getKey(), from, from += CommonId.LEN));
            byte[] key = new byte[keyValue.getKey().length - from];
            System.arraycopy(keyValue.getKey(), from , key, 0, key.length);
            NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges =
                getRegionList();
            if (ranges.size() > 1 && ps instanceof RangePartitionService) {
                CodecService.getDefault().setId(key, 0);
                newPartId = ps.calcPartId(key, ranges);
            }
            CommonId partId = param.getPartId();
            if (partId == null) {
                partId = newPartId;
                param.setPartId(partId);
                param.setTableId(tableId);
                param.addKey(key);
            } else if (partId.equals(newPartId)) {
                param.addKey(key);
                if (param.getKeys().size() == max_pre_write_count) {
                    boolean result = txnCommit(param, txnId, tableId, partId);
                    if (!result) {
                        throw new RuntimeException(txnId + " " + partId + ",txnCommit false,PrimaryKey:"
                            + Arrays.toString(param.getPrimaryKey()));
                    }
                    param.getKeys().clear();
                    param.setPartId(null);
                }
            } else {
                boolean result = txnCommit(param, txnId, param.getTableId(), partId);
                if (!result) {
                    throw new RuntimeException(txnId + " " + partId + ",txnCommit false,PrimaryKey:"
                        + Arrays.toString(param.getPrimaryKey()));
                }
                param.getKeys().clear();
                param.addKey(key);
                param.setPartId(newPartId);
                param.setTableId(tableId);
            }
            //String key = Base64.getEncoder().encodeToString(keyValue.getKey());
            //if (!caches.containsKey(key)) {
            //    caches.put(key, keyValue);
            //}
            //if (caches.size() % max_pre_write_count == 0) {
            //    try {
            //        commitSecondData(caches.values());
            //    } finally {
            //        caches.clear();
            //    }
            //}
        }
        if (!param.getKeys().isEmpty()) {
            boolean result = txnCommit(param, txnId, param.getTableId(), param.getPartId());
            if (!result) {
                throw new RuntimeException(txnId + " " + param.getPartId() + ",txnCommit false,PrimaryKey:"
                    + Arrays.toString(param.getPrimaryKey()));
            }
            //commitSecondData(caches.values());
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

    @Override
    public void close() {
        if (this.future != null) {
            this.future.cancel(true);
        }
        if (primaryObj == null) {
            return;
        }
        StoreInstance cache = Services.LOCAL_STORE.getInstance(null, null);
        Iterator<KeyValue> iterator = getLocalIterator();
        long removeLocalKeyCnt = 0;
        while (iterator.hasNext()) {
            cache.delete(iterator.next().getKey());
            removeLocalKeyCnt ++;
        }
        LogUtils.info(log, "close done, remove cache cnt:{}", removeLocalKeyCnt);
    }

    @Override
    public List<CommonId> getDoneRegion() {
        return doneRegionIdList;
    }

    @Override
    public NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> getRegionList() {
        return MetaService.root().getRangeDistribution(indexTable.tableId);
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
            .lockExtraDatas(TransactionUtil.toLockExtraDataList(
                cacheToObject.getTableId(), cacheToObject.getPartId(), txnId,
                TransactionType.OPTIMISTIC.getCode(), 1))
            .build();
        try {
            StoreInstance store = Services.KV_STORE.getInstance(cacheToObject.getTableId(), cacheToObject.getPartId());
            this.future = store.txnPreWritePrimaryKey(txnPreWrite, timeOut);
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
                    LogUtils.error(log, "preWrite primary region split, retry count:" + i);
                }
            }
        }
        if (this.future == null) {
            throw new RuntimeException(txnId + " future is null "
                + cacheToObject.getPartId() + ",preWritePrimaryKey false,PrimaryKey:"
                + Arrays.toString(primaryKey));
        }
    }

    private Iterator<Object[]> getIterator(ReorgBackFillTask task, CommonId tableId, boolean check) {
        StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, task.getRegionId());
        KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(
            table.getCodecVersion(), table.getVersion(), table.tupleType(), table.keyMapping()
        );
        Iterator<KeyValue> iterator = kvStore.txnScanWithoutStream(
            task.getStartTs(),
            new StoreInstance.Range(task.getStart(), task.getEnd(), task.isWithStart(), task.isWithEnd()),
            50000
        );
        if (!check) {
            iterator = Iterators.filter(iterator,
                t -> {
                    if (originPrimaryKey == null) {
                        return true;
                    } else {
                        return ByteArrayUtils.compare(t.getKey(), originPrimaryKey) != 0;
                    }
                }
            );
        }
        return Iterators.transform(iterator,
            wrap(codec::decode)::apply
        );
    }

    private Iterator<KeyValue> getLocalIterator() {
        StoreInstance cache = Services.LOCAL_STORE.getInstance(null, null);
        byte[] prefix = new byte[1 + txnIdKey.length];
        prefix[0] = (byte)FILL_BACK.getCode();
        System.arraycopy(txnIdKey, 0, prefix, 1, txnIdKey.length);
        return cache.scan(prefix);
    }

    public void commitSecondData(Collection<KeyValue> secondData) {
        CommitParam param = new CommitParam(dingoType, isolationLevel, txnId.seq,
            commitTs, primaryKey, TransactionType.OPTIMISTIC);
        param.init(null);
        for (KeyValue keyValue : secondData) {
            CommonId tableId = indexTable.tableId;
            int from = 1;
            //Arrays.copyOfRange(keyValue.getKey(), from, from += CommonId.LEN);
            from += CommonId.LEN;
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
                if (param.getKeys().size() == max_pre_write_count) {
                    boolean result = txnCommit(param, txnId, tableId, partId);
                    if (!result) {
                        throw new RuntimeException(txnId + " " + partId + ",txnCommit false,PrimaryKey:"
                            + Arrays.toString(param.getPrimaryKey()));
                    }
                    param.getKeys().clear();
                    param.setPartId(null);
                }
            } else {
                boolean result = txnCommit(param, txnId, param.getTableId(), partId);
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
            boolean result = txnCommit(param, txnId, param.getTableId(), param.getPartId());
            if (!result) {
                throw new RuntimeException(txnId + " " + param.getPartId()
                    + ",txnCommit false,PrimaryKey:" + Arrays.toString(param.getPrimaryKey()));
            }
        }
    }

    public TxnLocalData getTxnLocalData(Object[] tuplesTmp) {
        KeyValue keyValue = wrap(indexCodec::encode).apply(tuplesTmp);
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges =
            getRegionList();
        CommonId partId = ps.calcPartId(keyValue.getKey(), ranges);
        CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
        Op op;
        if (indexTable.unique) {
            op = Op.PUTIFABSENT;
        } else {
            op = Op.PUT;
        }
        return TxnLocalData.builder()
            .dataType(FILL_BACK)
            .txnId(txnId)
            .tableId(indexTable.tableId)
            .partId(partId)
            .op(op)
            .key(keyValue.getKey())
            .value(keyValue.getValue())
            .build();
    }

    protected void preWriteSecondSkipConflict(List<TxnLocalData> secondList) {
        try {
            Timer.Context timeCtx = DingoMetrics.getTimeContext("ReorgPreSecond");
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

    private void removeDoneKey(CommonId part, List<byte[]> mutationList) {
        Timer.Context timeCtx = DingoMetrics.getTimeContext("removeDoneKey");
        StoreInstance localStore = Services.LOCAL_STORE.getInstance(null, null);
        addCount.addAndGet(mutationList.size());
        mutationList.forEach(key -> {
            byte[] partId = part.encode();
            byte[] localKey = getLocalKey(key, partId);
            localStore.put(new KeyValue(localKey, null));
        });
        timeCtx.stop();
    }

    private byte[] getLocalKey(byte[] key, byte[] partId) {
        byte[] ek = new byte[key.length + 1 + txnIdKey.length + partId.length];
        ek[0] = (byte) FILL_BACK.getCode();
        System.arraycopy(txnIdKey, 0, ek, 1, txnIdKey.length);
        System.arraycopy(partId, 0, ek, 1 + txnIdKey.length, partId.length);
        System.arraycopy(key, 0, ek, 1 + txnIdKey.length + partId.length, key.length);
        return ek;
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
            for (TxnLocalData txnLocalData : secondList) {
                CommonId newPartId = txnLocalData.getPartId();
                int op = txnLocalData.getOp().getCode();
                byte[] key = txnLocalData.getKey();
                byte[] value = txnLocalData.getValue();
                Mutation mutation = TransactionCacheToMutation.cacheToMutation(
                    op, key, value, 0L, indexTable.tableId, newPartId, txnId
                );
                CommonId partId = param.getPartId();
                if (partId == null) {
                    partId = newPartId;
                    param.setPartId(partId);
                    param.setTableId(indexTable.tableId);
                    param.addMutation(mutation);
                } else if (partId.equals(newPartId)) {
                    param.addMutation(mutation);
                    if (param.getMutations().size() == max_pre_write_count) {
                        long sub = System.currentTimeMillis() - start;
                        LogUtils.debug(log, "pre write cost:{}", sub);
                        Pair<Boolean, Map<CommonId, List<byte[]>>> result
                            = txnPreWriteWithRePartId(param, txnId, indexTable.tableId, partId);
                        sub = System.currentTimeMillis() - start;
                        LogUtils.debug(log, "pre write cost:{}", sub);

                        if (!result.getKey()) {
                            throw new RuntimeException(txnId + " " + partId + ",txnPreWrite false,PrimaryKey:"
                                + Arrays.toString(param.getPrimaryKey()));
                        }
                        preDoneCnt += param.getMutations().size();
                        result.getValue().forEach(this::removeDoneKey);
                        param.getMutations().clear();
                        param.setPartId(null);
                        long tmp = System.currentTimeMillis();
                        sub = tmp - start;
                        LogUtils.debug(log, "pre write and remove done key cost:{}", sub);
                        start = tmp;
                    }
                } else {
                    Pair<Boolean, Map<CommonId, List<byte[]>>> result
                        = txnPreWriteWithRePartId(param, txnId, param.getTableId(), partId);
                    if (!result.getKey()) {
                        throw new RuntimeException(txnId + " " + partId + ",txnPreWrite false,PrimaryKey:"
                            + Arrays.toString(param.getPrimaryKey()));
                    }
                    preDoneCnt += param.getMutations().size();
                    result.getValue().forEach(this::removeDoneKey);
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
                Pair<Boolean, Map<CommonId, List<byte[]>>> result
                    = txnPreWriteWithRePartId(param, txnId, param.getTableId(), param.getPartId());
                if (!result.getKey()) {
                    throw new RuntimeException(txnId + " " + param.getPartId() + ",txnPreWrite false,PrimaryKey:"
                        + Arrays.toString(param.getPrimaryKey()));
                }
                preDoneCnt += param.getMutations().size();
                result.getValue().forEach(this::removeDoneKey);
                param.getMutations().clear();
            } catch (WriteConflictException e) {
                e.doneCnt += preDoneCnt;
                throw e;
            }
        }
        LogUtils.debug(log, "index reorg mod done, cost:{}", (System.currentTimeMillis() - end1));
    }

    public void duplicateKey(Object[] tuple) {
        StringBuilder duplicateKey = new StringBuilder("'");
        for (int i = 0; i < indexTable.getColumns().size(); i ++) {
            Column column = indexTable.getColumns().get(i);
            if (column.isPrimary()) {
                duplicateKey.append(tuple[i]).append("-");
            }
        }
        duplicateKey.deleteCharAt(duplicateKey.length() - 1);
        duplicateKey.append("'");
        throw new RuntimeException("Duplicate entry " + duplicateKey
            + " for key '" + indexTable.getName() + ".PRIMARY'");
    }

    public void initFiller() {
        schemaId = table.getTableId().domain;
    }
}
