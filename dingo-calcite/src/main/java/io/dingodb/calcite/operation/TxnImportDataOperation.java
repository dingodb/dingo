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

package io.dingodb.calcite.operation;

import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.TupleType;
import io.dingodb.common.type.scalar.BooleanType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.exec.Services;
import io.dingodb.exec.transaction.base.CacheToObject;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.base.TxnLocalData;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.transaction.params.CommitParam;
import io.dingodb.exec.transaction.params.PreWriteParam;
import io.dingodb.exec.transaction.params.RollBackParam;
import io.dingodb.exec.transaction.util.TransactionCacheToMutation;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.Mutation;
import io.dingodb.store.api.transaction.data.commit.TxnCommit;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;
import io.dingodb.store.api.transaction.data.rollback.TxnBatchRollBack;
import io.dingodb.store.api.transaction.exception.DuplicateEntryException;
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import io.dingodb.store.api.transaction.exception.WriteConflictException;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

@Slf4j
public class TxnImportDataOperation {
    int isolationLevel = IsolationLevel.ReadCommitted.getCode();
    long startTs;
    CommonId txnId;
    Future future;
    long commitTs;
    byte[] primaryKey;
    DingoType dingoType;

    boolean retry;
    int retryCnt;
    long timeOut;

    public TxnImportDataOperation(Long startTs, CommonId txnId, boolean retry, int retryCnt, long timeOut) {
        dingoType = new BooleanType(true);
        this.startTs = startTs;
        this.txnId = txnId;
        this.retry = retry;
        this.retryCnt = retryCnt;
        this.timeOut = timeOut;
    }

    public int insertByTxn(List<Object[]> tupleList) {
        CacheToObject primaryObj = null;
        List<Object[]> secondList = null;
        try {
            // get local mem data first data and transform to cacheToObject
            Object[] primary = tupleList.get(0);
            primaryObj = getCacheToObject(primary);

            preWritePrimaryKey(primaryObj);
            // pre write second key
            secondList = tupleList.subList(1, tupleList.size());
            preWriteSecondKey(secondList);
        } catch (WriteConflictException e) {
            log.info(e.getMessage(), e);
            // rollback or retry
            resolveWriteConflict(e, secondList, tupleList);
        } catch (DuplicateEntryException e) {
            log.info(e.getMessage(), e);
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
            commitSecondData(secondList);
            return tupleList.size();
        } finally {
            if (future != null) {
                future.cancel(true);
            }
        }
    }

    public static CacheToObject getCacheToObject(Object[] tuples) {
        TxnLocalData txnLocalData = (TxnLocalData) tuples[0];
        CommonId tableId = txnLocalData.getTableId();
        CommonId newPartId = txnLocalData.getPartId();
        int op = txnLocalData.getOp().getCode();
        byte[] key = txnLocalData.getKey();
        byte[] value = txnLocalData.getValue();
        return new CacheToObject(TransactionCacheToMutation.cacheToMutation(
            op, key, value,0L, tableId, newPartId), tableId, newPartId
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
            .lockExtraDatas(TransactionUtil.toLockExtraDataList(cacheToObject.getTableId(), cacheToObject.getPartId(), txnId,
                TransactionType.OPTIMISTIC.getCode(), 1))
            .build();
        try {
            StoreInstance store = Services.KV_STORE.getInstance(cacheToObject.getTableId(), cacheToObject.getPartId());
            this.future = store.txnPreWritePrimaryKey(txnPreWrite, timeOut);
        } catch (RegionSplitException e) {
            log.error(e.getMessage(), e);

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
                    lookSleep();
                    log.error("prewrite primary region split, retry count:" + i);
                }
            }
        }
        if (this.future == null) {
            throw new RuntimeException(txnId + " future is null "
                + cacheToObject.getPartId() + ",preWritePrimaryKey false,PrimaryKey:"
                + Arrays.toString(primaryKey));
        }
    }

    private static boolean txnPreWrite(PreWriteParam param, CommonId txnId, CommonId tableId, CommonId partId) {
        // 1、call sdk TxnPreWrite
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
            log.error(e.getMessage(), e);
            // 2、regin split

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
                    lookSleep();
                    log.error("prewrite second region split, retry count:" + i);
                }
            }

            return true;
        }
    }

    private void preWriteSecondKey(List<Object[]> secondList) {
        PreWriteParam param = new PreWriteParam(dingoType, primaryKey, startTs,
            isolationLevel, TransactionType.OPTIMISTIC, timeOut);
        param.init(null);
        for (Object[] tuples : secondList) {
            TxnLocalData txnLocalData = (TxnLocalData) tuples[0];
            CommonId txnId = txnLocalData.getTxnId();
            CommonId tableId = txnLocalData.getTableId();
            CommonId newPartId = txnLocalData.getPartId();
            int op = txnLocalData.getOp().getCode();
            byte[] key = txnLocalData.getKey();
            byte[] value = txnLocalData.getValue();
            Mutation mutation = TransactionCacheToMutation.cacheToMutation(op, key, value, 0L, tableId, newPartId);
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

        if (param.getMutations().size() > 0) {
            boolean result = txnPreWrite(param, txnId, param.getTableId(), param.getPartId());
            if (!result) {
                throw new RuntimeException(txnId + " " + param.getPartId() + ",txnPreWrite false,PrimaryKey:"
                    + Arrays.toString(param.getPrimaryKey()));
            }
            param.getMutations().clear();
        }
    }

    public boolean commitPrimaryData(CacheToObject cacheToObject) {
        // 1、call sdk commitPrimaryKey
        TxnCommit commitRequest = TxnCommit.builder()
            .isolationLevel(IsolationLevel.of(isolationLevel))
            .startTs(startTs)
            .commitTs(commitTs)
            .keys(Collections.singletonList(primaryKey))
            .build();
        try {
            StoreInstance store = Services.KV_STORE.getInstance(cacheToObject.getTableId(), cacheToObject.getPartId());
            return store.txnCommit(commitRequest);
        } catch (RuntimeException e) {
            log.error(e.getMessage(), e);
            // 2、regin split
            boolean commitResult = false;
            int i = 0;
            while (!commitResult) {
                i ++;
                try {
                    CommonId regionId = TransactionUtil.singleKeySplitRegionId(cacheToObject.getTableId(), txnId, primaryKey);
                    StoreInstance store = Services.KV_STORE.getInstance(cacheToObject.getTableId(), regionId);
                    commitResult = store.txnCommit(commitRequest);
                } catch (RegionSplitException e1) {
                    lookSleep();
                    log.error("commit primary region split, retry count:" + i);
                }
            }
            return true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    public void commitSecondData(List<Object[]> secondData) {
        CommitParam param = new CommitParam(dingoType, isolationLevel, startTs,
            commitTs, primaryKey, TransactionType.OPTIMISTIC);
        param.init(null);
        for (Object[] tuples : secondData) {
            TxnLocalData txnLocalData = (TxnLocalData) tuples[0];
            CommonId txnId = txnLocalData.getTxnId();
            CommonId tableId = txnLocalData.getTableId();
            CommonId newPartId = txnLocalData.getPartId();
            byte[] key = txnLocalData.getKey();
            if (tableId.type == CommonId.CommonType.INDEX) {
                IndexTable indexTable = TransactionUtil.getIndexDefinitions(tableId);
                if (indexTable.indexType.isVector) {
                    KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(indexTable.tupleType(), indexTable.keyMapping());
                    Object[] decodeKey = codec.decodeKeyPrefix(key);
                    TupleMapping mapping = TupleMapping.of(new int[]{0});
                    DingoType dingoType = new LongType(false);
                    TupleType tupleType = DingoTypeFactory.tuple(new DingoType[]{dingoType});
                    KeyValueCodec vectorCodec = CodecService.getDefault().createKeyValueCodec(tupleType, mapping);
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
        if (param.getKeys().size() > 0) {
            boolean result = txnCommit(param, txnId, param.getTableId(), param.getPartId());
            if (!result) {
                throw new RuntimeException(txnId + " " + param.getPartId()
                    + ",txnCommit false,PrimaryKey:" + Arrays.toString(param.getPrimaryKey()));
            }
        }
    }

    private static boolean txnCommit(CommitParam param, CommonId txnId, CommonId tableId, CommonId newPartId) {
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
            log.error(e.getMessage(), e);

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
                    lookSleep();
                    log.error("commit second region split, retry count:" + i);
                }
            }

            return true;
        }
    }

    private static void lookSleep() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void resolveWriteConflict(RuntimeException exception, List<Object[]> secondList, List<Object[]> tupleList) {
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
                log.info(e1.getMessage(), e1);
                rollback(tupleList);
            } catch (RuntimeException e2) {
                conflictException = e2;
                log.error(e2.getMessage(), e2);
                break;
            }
        }
        if (conflictException != null) {
            throw conflictException;
        }
    }

    public synchronized void rollback(List<Object[]> tupleList) {
        if (tupleList.size() == 0) {
            return;
        }
        try {
            // 1、get commit_ts
            // 2、generator job、task、RollBackOperator
            // 3、run RollBack
            RollBackParam param = new RollBackParam(dingoType, isolationLevel, startTs, TransactionType.OPTIMISTIC, primaryKey);
            param.init(null);
            for (Object[] tuples : tupleList) {
                TxnLocalData txnLocalData = (TxnLocalData) tuples[0];
                CommonId txnId = txnLocalData.getTxnId();
                CommonId tableId = txnLocalData.getTableId();
                CommonId newPartId = txnLocalData.getPartId();
                byte[] key = txnLocalData.getKey();
                long forUpdateTs = 0;

                if (tableId.type == CommonId.CommonType.INDEX) {
                    IndexTable indexTable = TransactionUtil.getIndexDefinitions(tableId);
                    if (indexTable.indexType.isVector) {
                        KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(indexTable.tupleType(), indexTable.keyMapping());
                        Object[] decodeKey = codec.decodeKeyPrefix(key);
                        TupleMapping mapping = TupleMapping.of(new int[]{0});
                        DingoType dingoType = new LongType(false);
                        TupleType tupleType = DingoTypeFactory.tuple(new DingoType[]{dingoType});
                        KeyValueCodec vectorCodec = CodecService.getDefault().createKeyValueCodec(tupleType, mapping);
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
            if (param.getKeys().size() > 0) {
                boolean result = txnRollBack(param, txnId, param.getTableId(), param.getPartId());
                if (!result) {
                    throw new RuntimeException(txnId + " " + param.getPartId() + ",txnBatchRollback false");
                }
                param.getKeys().clear();
            }
        } catch (Throwable t) {
            log.info(t.getMessage(), t);
            throw new RuntimeException(t);
        } finally {
            if (future != null) {
                future.cancel(true);
            }
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
            log.error(e.getMessage(), e);
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

}
