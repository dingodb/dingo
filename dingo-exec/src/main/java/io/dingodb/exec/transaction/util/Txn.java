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

package io.dingodb.exec.transaction.util;

import com.codahale.metrics.Timer;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.TupleType;
import io.dingodb.common.type.scalar.BooleanType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.util.Pair;
import io.dingodb.common.util.Utils;
import io.dingodb.exec.Services;
import io.dingodb.exec.transaction.base.CacheToObject;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.base.TxnLocalData;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.transaction.params.CommitParam;
import io.dingodb.exec.transaction.params.PreWriteParam;
import io.dingodb.exec.transaction.params.RollBackParam;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.Mutation;
import io.dingodb.store.api.transaction.data.commit.TxnCommit;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;
import io.dingodb.store.api.transaction.data.rollback.TxnBatchRollBack;
import io.dingodb.store.api.transaction.exception.CommitTsExpiredException;
import io.dingodb.store.api.transaction.exception.DuplicateEntryException;
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import io.dingodb.store.api.transaction.exception.WriteConflictException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Slf4j
public class Txn {
    int isolationLevel = IsolationLevel.ReadCommitted.getCode();
    long startTs;
    CommonId txnId;
    Future<?> future;
    @Getter
    @Setter
    long commitTs;
    @Setter
    byte[] primaryKey;
    DingoType dingoType;

    boolean retry;
    int retryCnt;
    long timeOut;

    CacheToObject primaryObj = null;

    public Txn(CommonId txnId, boolean retry, int retryCnt, long timeOut) {
        dingoType = new BooleanType(true);
        this.startTs = txnId.seq;
        this.txnId = txnId;
        this.retry = retry;
        this.retryCnt = retryCnt;
        this.timeOut = timeOut;
    }

    public int commit(List<TxnLocalData> tupleList) {
        List<TxnLocalData> secondList = null;
        try {
            // get local mem data first data and transform to cacheToObject
            TxnLocalData primary = tupleList.get(0);
            primaryObj = getCacheToObject(primary);

            preWritePrimaryKey(primaryObj);
            // pre write second key
            secondList = tupleList.subList(1, tupleList.size());
            if (!secondList.isEmpty()) {
                preWriteSecondKey(secondList);
            }
        } catch (WriteConflictException e) {
            LogUtils.error(log, e.getMessage(), e);
            // rollback or retry
            resolveWriteConflict(e, secondList, tupleList);
        } catch (DuplicateEntryException e) {
            LogUtils.error(log, e.getMessage(), e);
            // rollback
            rollback(tupleList);
            throw e;
        }

        try {
            this.commitTs = TransactionManager.getCommitTs();
            // commit primary key
            boolean result = commitPrimaryData(primaryObj);
            if (!result) {
                assert primaryObj != null;
                throw new RuntimeException(txnId + " " + primaryObj.getPartId()
                    + ",txnCommitPrimaryKey false,commit_ts:" + commitTs + ",PrimaryKey:"
                    + Arrays.toString(primaryKey));
            }
            // commit second key
            assert secondList != null;
            if (!secondList.isEmpty()) {
                commitSecondData(secondList);
            }
            return tupleList.size();
        } finally {
            if (future != null) {
                future.cancel(true);
            }
        }
    }

    public static CacheToObject getCacheToObject(TxnLocalData txnLocalData) {
        CommonId tableId = txnLocalData.getTableId();
        CommonId newPartId = txnLocalData.getPartId();
        int op = txnLocalData.getOp().getCode();
        byte[] key = txnLocalData.getKey();
        byte[] value = txnLocalData.getValue();
        return new CacheToObject(TransactionCacheToMutation.cacheToMutation(
            op, key, value,0L, tableId, newPartId, txnLocalData.getTxnId()), tableId, newPartId
        );
    }

    private void preWritePrimaryKey(CacheToObject cacheToObject) {
        primaryKey = cacheToObject.getMutation().getKey();
        // 2、call sdk preWritePrimaryKey
        TxnPreWrite txnPreWrite = TxnPreWrite.builder()
            .isolationLevel(IsolationLevel.of(
                isolationLevel
            ))
            .mutations(Collections.singletonList(cacheToObject.getMutation()))
            .primaryLock(primaryKey)
            .startTs(startTs)
            .lockTtl(TransactionManager.lockTtlTm())
            .txnSize(1L)
            .tryOnePc(false)
            .maxCommitTs(0L)
            .lockExtraDatas(TransactionUtil.toLockExtraDataList(cacheToObject.getTableId(),
                cacheToObject.getPartId(), txnId,
                TransactionType.OPTIMISTIC.getCode(), 1))
            .build();
        try {
            StoreInstance store = Services.KV_STORE.getInstance(cacheToObject.getTableId(), cacheToObject.getPartId());
            this.future = store.txnPreWritePrimaryKey(txnPreWrite, timeOut);
        } catch (RegionSplitException e) {
            LogUtils.error(log, e.getMessage(), e);
            long start = System.currentTimeMillis();
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
                    LogUtils.error(log, "pre write primary region split, retry count:" + i);
                }
            }
            long sub = System.currentTimeMillis() - start;
            LogUtils.info(log, "pre write primary region split failed retry cost:{}", sub);
        }
        if (this.future == null) {
            throw new RuntimeException(txnId + " future is null "
                + cacheToObject.getPartId() + ",preWritePrimaryKey false,PrimaryKey:"
                + Arrays.toString(primaryKey));
        }
    }

    public static Boolean txnPreWrite(PreWriteParam param, CommonId txnId, CommonId tableId, CommonId partId) {
        // 1、call sdk TxnPreWrite
        Timer.Context timeCtx = DingoMetrics.getTimeContext("preWrite");
        param.setTxnSize(param.getMutations().size());
        TxnPreWrite txnPreWrite = TxnPreWrite.builder()
            .isolationLevel(IsolationLevel.of(param.getIsolationLevel()))
            .mutations(param.getMutations())
            .primaryLock(param.getPrimaryKey())
            .startTs(param.getStartTs())
            .lockTtl(TransactionManager.lockTtlTm())
            .txnSize(param.getTxnSize())
            .tryOnePc(param.isTryOnePc())
            .maxCommitTs(param.getMaxCommitTs())
            .lockExtraDatas(TransactionUtil.toLockExtraDataList(tableId, partId, txnId,
                param.getTransactionType().getCode(), param.getMutations().size()))
            .build();
        try {
            StoreInstance store = Services.KV_STORE.getInstance(tableId, partId);
            return store.txnPreWrite(txnPreWrite, param.getTimeOut());
        } catch (RegionSplitException e) {
            LogUtils.error(log, e.getMessage(), e);
            // 2、regin split
            long start = System.currentTimeMillis();
            boolean prewriteSecondResult = false;
            int i = 0;
            while (!prewriteSecondResult) {
                i ++;
                try {
                    Map<CommonId, List<byte[]>> partMap = TransactionUtil.multiKeySplitRegionId(tableId, txnId,
                        TransactionUtil.mutationToKey(param.getMutations()));
                    for (Map.Entry<CommonId, List<byte[]>> entry : partMap.entrySet()) {
                        CommonId regionId = entry.getKey();
                        List<byte[]> value = entry.getValue();
                        StoreInstance store = Services.KV_STORE.getInstance(tableId, regionId);
                        txnPreWrite.setMutations(TransactionUtil.keyToMutation(value, param.getMutations()));
                        boolean result = store.txnPreWrite(txnPreWrite ,param.getTimeOut());
                        if (!result) {
                            return false;
                        }
                    }
                    prewriteSecondResult = true;
                } catch (RegionSplitException e1) {
                    Utils.sleep(1000);
                    LogUtils.error(log, "pre write second region split, retry count:" + i, e);
                }
            }
            long sub = System.currentTimeMillis() - start;
            LogUtils.info(log, "pre write region split failed retry cost:{}", sub);
            return true;
        } finally {
            timeCtx.stop();
        }
    }

    public static Pair<Boolean, Map<CommonId, List<byte[]>>> txnPreWriteWithRePartId(
        PreWriteParam param, CommonId txnId, CommonId tableId, CommonId partId
    ) {
        // 1、call sdk TxnPreWrite
        Timer.Context timeCtx = DingoMetrics.getTimeContext("preWrite");
        param.setTxnSize(param.getMutations().size());
        TxnPreWrite txnPreWrite = TxnPreWrite.builder()
            .isolationLevel(IsolationLevel.of(param.getIsolationLevel()))
            .mutations(param.getMutations())
            .primaryLock(param.getPrimaryKey())
            .startTs(param.getStartTs())
            .lockTtl(TransactionManager.lockTtlTm())
            .txnSize(param.getTxnSize())
            .tryOnePc(param.isTryOnePc())
            .maxCommitTs(param.getMaxCommitTs())
            .lockExtraDatas(TransactionUtil.toLockExtraDataList(tableId, partId, txnId,
                param.getTransactionType().getCode(), param.getMutations().size()))
            .build();
        try {
            StoreInstance store = Services.KV_STORE.getInstance(tableId, partId);
            boolean res = store.txnPreWrite(txnPreWrite, param.getTimeOut());
            if (!res) {
                return Pair.of(false, new HashMap<>());
            } else {
                List<byte[]> keyList = param.getMutations().stream().map(Mutation::getKey)
                    .collect(Collectors.toList());
                Map<CommonId, List<byte[]>> map = new HashMap<>();
                map.put(partId, keyList);
                return Pair.of(true, map);
            }
        } catch (RegionSplitException e) {
            LogUtils.error(log, e.getMessage(), e);
            // 2、regin split
            long start = System.currentTimeMillis();
            boolean prewriteSecondResult = false;
            int i = 0;
            Map<CommonId, List<byte[]>> partMap = null;
            while (!prewriteSecondResult) {
                i ++;
                try {
                    partMap = TransactionUtil.multiKeySplitRegionId(tableId, txnId,
                        TransactionUtil.mutationToKey(param.getMutations()));
                    for (Map.Entry<CommonId, List<byte[]>> entry : partMap.entrySet()) {
                        CommonId regionId = entry.getKey();
                        List<byte[]> value = entry.getValue();
                        StoreInstance store = Services.KV_STORE.getInstance(tableId, regionId);
                        txnPreWrite.setMutations(TransactionUtil.keyToMutation(value, param.getMutations()));
                        boolean result = store.txnPreWrite(txnPreWrite ,param.getTimeOut());
                        if (!result) {
                            return Pair.of(false, new HashMap<>());
                        }
                    }
                    prewriteSecondResult = true;
                } catch (RegionSplitException e1) {
                    Utils.sleep(1000);
                    LogUtils.error(log, "pre write second region split with res, "
                         + "retry count:{}, originRegionId:{}", i, partId, e);
                }
            }
            long sub = System.currentTimeMillis() - start;
            LogUtils.info(log, "pre write region split with res failed retry cost:{}, originRegionId:{}",
                sub, partId);
            return Pair.of(true, partMap);
        } finally {
            timeCtx.stop();
        }
    }

    private void preWriteSecondKey(List<TxnLocalData> secondList) {
        PreWriteParam param = new PreWriteParam(dingoType, primaryKey, startTs,
            isolationLevel, TransactionType.OPTIMISTIC, timeOut);
        param.init(null);
        for (TxnLocalData txnLocalData : secondList) {
            CommonId txnId = txnLocalData.getTxnId();
            CommonId tableId = txnLocalData.getTableId();
            CommonId newPartId = txnLocalData.getPartId();
            int op = txnLocalData.getOp().getCode();
            byte[] key = txnLocalData.getKey();
            byte[] value = txnLocalData.getValue();
            Mutation mutation = TransactionCacheToMutation.cacheToMutation(
                op, key, value, 0L, tableId, newPartId, txnId
            );
            CommonId partId = param.getPartId();
            if (partId == null) {
                partId = newPartId;
                param.setPartId(partId);
                param.setTableId(tableId);
                param.addMutation(mutation);
            } else if (partId.equals(newPartId)) {
                param.addMutation(mutation);
                if (param.getMutations().size() == TransactionUtil.max_pre_write_count) {
                    boolean result = txnPreWrite(param, txnId, tableId, partId);
                    if (!result) {
                        throw new RuntimeException(txnId + " " + partId + ",txnPreWrite false,PrimaryKey:"
                            + Arrays.toString(param.getPrimaryKey()));
                    }
                    param.getMutations().clear();
                    param.setPartId(null);
                }
            } else {
                boolean result = txnPreWrite(param, txnId, param.getTableId(), partId);
                if (!result) {
                    throw new RuntimeException(txnId + " " + partId + ",txnPreWrite false,PrimaryKey:"
                        + Arrays.toString(param.getPrimaryKey()));
                }
                param.getMutations().clear();
                param.addMutation(mutation);
                param.setPartId(newPartId);
                param.setTableId(tableId);
            }
        }

        if (!param.getMutations().isEmpty()) {
            boolean result = txnPreWrite(param, txnId, param.getTableId(), param.getPartId());
            if (!result) {
                throw new RuntimeException(txnId + " " + param.getPartId() + ",txnPreWrite false,PrimaryKey:"
                    + Arrays.toString(param.getPrimaryKey()));
            }
            param.getMutations().clear();
        }
    }

    public boolean commitPrimaryData(CacheToObject cacheToObject) {
        try {
            // 1、call sdk commitPrimaryKey
            long start = System.currentTimeMillis();
            while (true) {
                TxnCommit commitRequest = TxnCommit.builder()
                    .isolationLevel(IsolationLevel.of(isolationLevel))
                    .startTs(startTs)
                    .commitTs(commitTs)
                    .keys(Collections.singletonList(primaryKey))
                    .build();
                try {
                    StoreInstance store = Services.KV_STORE.getInstance(
                        cacheToObject.getTableId(), cacheToObject.getPartId()
                    );
                    return store.txnCommit(commitRequest);
                } catch (RegionSplitException e) {
                    LogUtils.error(log, e.getMessage(), e);
                    // 2、regin split
                    CommonId regionId = TransactionUtil.singleKeySplitRegionId(
                        cacheToObject.getTableId(), txnId, primaryKey
                    );
                    cacheToObject.setPartId(regionId);
                    Utils.sleep(100);
                } catch (CommitTsExpiredException e) {
                    LogUtils.error(log, e.getMessage(), e);
                    this.commitTs = TransactionManager.getCommitTs();
                }
                long elapsed = System.currentTimeMillis() - start;
                if (elapsed > timeOut) {
                    LogUtils.error(log, "txn commit primary timeout", cacheToObject);
                    return false;
                }
            }
        } catch (Throwable throwable) {
            LogUtils.error(log, "txn commit primary error:" + throwable.getMessage(), throwable);
        }
        return false;
    }

    public void commitSecondData(List<TxnLocalData> secondData) {
        CommitParam param = new CommitParam(dingoType, isolationLevel, startTs,
            commitTs, primaryKey, TransactionType.OPTIMISTIC);
        param.init(null);
        for (TxnLocalData txnLocalData : secondData) {
            CommonId txnId = txnLocalData.getTxnId();
            CommonId tableId = txnLocalData.getTableId();
            CommonId newPartId = txnLocalData.getPartId();
            byte[] key = txnLocalData.getKey();
            if (tableId.type == CommonId.CommonType.INDEX) {
                IndexTable indexTable = (IndexTable) TransactionManager.getIndex(txnId, tableId);
                if (indexTable == null) {
                    indexTable = (IndexTable) DdlService.root().getTable(tableId);
                }
                if (indexTable.indexType.isVector) {
                    KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(
                        indexTable.getCodecVersion(), indexTable.version, indexTable.tupleType(),
                        indexTable.keyMapping()
                    );
                    Object[] decodeKey = codec.decodeKeyPrefix(key);
                    TupleMapping mapping = TupleMapping.of(new int[]{0});
                    DingoType dingoType = new LongType(false);
                    TupleType tupleType = DingoTypeFactory.tuple(new DingoType[]{dingoType});
                    KeyValueCodec vectorCodec = CodecService.getDefault().createKeyValueCodec(
                        indexTable.getCodecVersion(), indexTable.version, tupleType, mapping
                    );
                    key = vectorCodec.encodeKeyPrefix(new Object[]{decodeKey[0]}, 1);
                }
            }
            CommonId partId = param.getPartId();
            if (partId == null) {
                partId = newPartId;
                param.setPartId(partId);
                param.setTableId(tableId);
                param.addKey(key);
            } else if (partId.equals(newPartId)) {
                param.addKey(key);
                if (param.getKeys().size() == TransactionUtil.max_pre_write_count) {
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

    public static boolean txnCommit(CommitParam param, CommonId txnId, CommonId tableId, CommonId newPartId) {
        // 1、Async call sdk TxnCommit
        TxnCommit commitRequest = TxnCommit.builder()
            .isolationLevel(IsolationLevel.of(param.getIsolationLevel()))
            .startTs(param.getStartTs())
            .commitTs(param.getCommitTs())
            .keys(param.getKeys())
            .build();
        try {
            StoreInstance store = Services.KV_STORE.getInstance(tableId, newPartId);
            return store.txnCommit(commitRequest);
        } catch (RegionSplitException e) {
            LogUtils.error(log, e.getMessage(), e);
            long start = System.currentTimeMillis();
            boolean commitSecondResult = false;
            int i = 0;
            while (!commitSecondResult) {
                try {
                    i ++;
                    Map<CommonId, List<byte[]>> partMap = TransactionUtil.multiKeySplitRegionId(
                        tableId,
                        txnId,
                        param.getKeys()
                    );
                    for (Map.Entry<CommonId, List<byte[]>> entry : partMap.entrySet()) {
                        CommonId regionId = entry.getKey();
                        List<byte[]> value = entry.getValue();
                        StoreInstance store = Services.KV_STORE.getInstance(tableId, regionId);
                        commitRequest.setKeys(value);
                        boolean result = store.txnCommit(commitRequest);
                        if (!result) {
                            return false;
                        }
                    }
                    commitSecondResult = true;
                } catch (RegionSplitException e1) {
                    Utils.sleep(1000);
                    LogUtils.error(log, "commit second region split, retry count:" + i);
                }
            }
            long sub = System.currentTimeMillis() - start;
            LogUtils.info(log, "commit region split failed retry cost:{}", sub);

            return true;
        }
    }

    public void resolveWriteConflict(
        RuntimeException exception, List<TxnLocalData> secondList, List<TxnLocalData> tupleList
    ) {
        rollback(tupleList);
        int txnRetryLimit = retryCnt;
        RuntimeException conflictException = exception;
        while (retry && (txnRetryLimit-- > 0)) {
            try {
                conflictException = null;
                this.startTs = TransactionManager.nextTimestamp();
                preWriteSecondKey(secondList);
                break;
            } catch (WriteConflictException e1) {
                conflictException = e1;
                LogUtils.error(log, e1.getMessage(), e1);
                rollback(tupleList);
            } catch (RuntimeException e2) {
                conflictException = e2;
                LogUtils.error(log, e2.getMessage(), e2);
                break;
            }
        }
        if (conflictException != null) {
            throw conflictException;
        }
    }

    public synchronized void rollback(List<TxnLocalData> tupleList) {
        try {
            if (primaryObj != null) {
                rollBackPrimaryKey(primaryObj);
            }
            if (tupleList.isEmpty()) {
                return;
            }
            // 1、get commit_ts
            // 2、generator job、task、RollBackOperator
            // 3、run RollBack
            RollBackParam param = new RollBackParam(
                dingoType, isolationLevel, startTs, TransactionType.OPTIMISTIC, primaryKey
            );
            param.init(null);
            for (TxnLocalData txnLocalData : tupleList) {
                CommonId txnId = txnLocalData.getTxnId();
                CommonId tableId = txnLocalData.getTableId();
                CommonId newPartId = txnLocalData.getPartId();
                byte[] key = txnLocalData.getKey();
                long forUpdateTs = 0;

                if (tableId.type == CommonId.CommonType.INDEX) {
                    IndexTable indexTable = (IndexTable) TransactionManager.getIndex(txnId, tableId);
                    if (indexTable == null) {
                        indexTable = (IndexTable) DdlService.root().getTable(tableId);
                    }
                    if (indexTable.indexType.isVector) {
                        KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(
                            indexTable.getCodecVersion(), indexTable.version,
                            indexTable.tupleType(), indexTable.keyMapping()
                        );
                        Object[] decodeKey = codec.decodeKeyPrefix(key);
                        TupleMapping mapping = TupleMapping.of(new int[]{0});
                        DingoType dingoType = new LongType(false);
                        TupleType tupleType = DingoTypeFactory.tuple(new DingoType[]{dingoType});
                        KeyValueCodec vectorCodec = CodecService.getDefault().createKeyValueCodec(
                            indexTable.getCodecVersion(), indexTable.version, tupleType, mapping
                        );
                        key = vectorCodec.encodeKeyPrefix(new Object[]{decodeKey[0]}, 1);
                    }
                }

                CommonId partId = param.getPartId();
                if (partId == null) {
                    partId = newPartId;
                    param.setPartId(partId);
                    param.addKey(key);
                    param.setTableId(tableId);
                    param.addForUpdateTs(forUpdateTs);
                } else if (partId.equals(newPartId)) {
                    param.addKey(key);
                    param.addForUpdateTs(forUpdateTs);
                    if (param.getKeys().size() == TransactionUtil.max_pre_write_count) {
                        boolean result = txnRollBack(param, txnId, tableId, partId);
                        if (!result) {
                            throw new RuntimeException(txnId + " " + partId + ",txnBatchRollback false");
                        }
                        param.getKeys().clear();
                        param.setPartId(null);
                    }
                } else {
                    boolean result = txnRollBack(param, txnId, param.getTableId(), partId);
                    if (!result) {
                        throw new RuntimeException(txnId + " " + partId + ",txnBatchRollback false");
                    }
                    param.getKeys().clear();
                    param.addKey(key);
                    param.setPartId(newPartId);
                    param.setTableId(tableId);
                    param.addForUpdateTs(forUpdateTs);
                }
            }
            if (!param.getKeys().isEmpty()) {
                boolean result = txnRollBack(param, txnId, param.getTableId(), param.getPartId());
                if (!result) {
                    throw new RuntimeException(txnId + " " + param.getPartId() + ",txnBatchRollback false");
                }
                param.getKeys().clear();
            }
        } catch (Throwable t) {
            LogUtils.error(log, t.getMessage(), t);
            throw new RuntimeException(t);
        } finally {
            if (future != null) {
                future.cancel(true);
            }
        }
    }

    private void rollBackPrimaryKey(CacheToObject cacheToObject) {
        boolean result = TransactionUtil.rollBackPrimaryKey(
            txnId,
            cacheToObject.getTableId(),
            cacheToObject.getPartId(),
            isolationLevel,
            startTs,
            primaryKey
        );
        if (!result) {
            throw new RuntimeException(txnId + ",rollBackPrimaryKey false");
        }
    }

    private static boolean txnRollBack(RollBackParam param, CommonId txnId, CommonId tableId, CommonId newPartId) {
        // 1、Async call sdk TxnRollBack
        TxnBatchRollBack rollBackRequest = TxnBatchRollBack.builder()
            .isolationLevel(IsolationLevel.of(param.getIsolationLevel()))
            .startTs(param.getStartTs())
            .keys(param.getKeys())
            .build();
        try {
            StoreInstance store = Services.KV_STORE.getInstance(tableId, newPartId);
            return store.txnBatchRollback(rollBackRequest);
        } catch (RuntimeException e) {
            LogUtils.error(log, e.getMessage(), e);
            // 2、regin split
            Map<CommonId, List<byte[]>> partMap = TransactionUtil.multiKeySplitRegionId(
                tableId,
                txnId,
                param.getKeys()
            );
            for (Map.Entry<CommonId, List<byte[]>> entry : partMap.entrySet()) {
                CommonId regionId = entry.getKey();
                List<byte[]> value = entry.getValue();
                StoreInstance store = Services.KV_STORE.getInstance(tableId, regionId);
                rollBackRequest.setKeys(value);
                boolean result = store.txnBatchRollback(rollBackRequest);
                if (!result) {
                    return false;
                }
            }
            return true;
        }
    }

    public void close() {
        if (this.future != null) {
            this.future.cancel(true);
        }
    }
}
