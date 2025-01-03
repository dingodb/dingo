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

import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.log.MdcUtils;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.TupleType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.Services;
import io.dingodb.exec.transaction.base.TwoPhaseCommitData;
import io.dingodb.exec.transaction.base.TxnLocalData;
import io.dingodb.exec.transaction.base.TxnPartData;
import io.dingodb.exec.transaction.impl.TransactionCache;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.IndexType;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.Mutation;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.data.commit.TxnCommit;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;
import io.dingodb.store.api.transaction.data.rollback.TxnBatchRollBack;
import io.dingodb.store.api.transaction.data.rollback.TxnPessimisticRollBack;
import io.dingodb.store.api.transaction.exception.DuplicateEntryException;
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import io.dingodb.store.api.transaction.exception.WriteConflictException;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

import static io.dingodb.exec.transaction.util.TransactionUtil.keyToMutation;
import static io.dingodb.exec.transaction.util.TransactionUtil.multiKeySplitRegionId;
import static io.dingodb.exec.transaction.util.TransactionUtil.mutationToKey;
import static io.dingodb.exec.transaction.util.TransactionUtil.toForUpdateTsChecks;
import static io.dingodb.exec.transaction.util.TransactionUtil.toLockExtraDataList;
import static io.dingodb.exec.transaction.util.TransactionUtil.toPessimisticCheck;

@Slf4j
public final class TwoPhaseCommitUtils {

    private TwoPhaseCommitUtils() {
    }

    public static CompletableFuture<Boolean> preWriteSecondKeys(@NonNull TxnPartData txnPartData,
                                                                @Nullable TwoPhaseCommitData twoPhaseCommitData) {
        CommonId txnId = twoPhaseCommitData.getTxnId();
        CommonId tableId = txnPartData.getTableId();
        CommonId newPartId = txnPartData.getPartId();
        Iterator<Object[]> cacheData = TransactionCache.getCacheData(
            twoPhaseCommitData.getTxnId(),
            tableId,
            newPartId
        );
        byte[] primaryKey = twoPhaseCommitData.getPrimaryKey();
        List<Mutation> mutations = new ArrayList<>();
        Supplier<Boolean> supplier = () -> {
            MdcUtils.setTxnId(txnId.toString());
            while (cacheData.hasNext()) {
                Object[] tuple = cacheData.next();
                TxnLocalData txnLocalData = (TxnLocalData) tuple[0];
                int op = txnLocalData.getOp().getCode();
                byte[] key = txnLocalData.getKey();
                byte[] value = txnLocalData.getValue();
                // first key is primary key
                if (ByteArrayUtils.compare(key, primaryKey, 1) == 0) {
                    continue;
                }
                Mutation mutation = TransactionCacheToMutation.preWriteMutation(
                    txnId,
                    tableId,
                    newPartId,
                    op,
                    key,
                    value,
                    twoPhaseCommitData.isPessimistic()
                );
                LogUtils.debug(log, "mutation: {}", mutation);
                mutations.add(mutation);
                if (mutations.size() == TransactionUtil.max_pre_write_count) {
                    boolean result = TwoPhaseCommitUtils.txnPreWrite(
                        tableId,
                        newPartId,
                        mutations,
                        twoPhaseCommitData
                    );
                    if (!result) {
                        throw new RuntimeException(txnId + " " + newPartId
                            + ", txnPreWrite false, PrimaryKey:"
                            + Arrays.toString(primaryKey));
                    }
                    mutations.clear();
                }
            }

            if (!mutations.isEmpty()) {
                boolean result = TwoPhaseCommitUtils.txnPreWrite(
                    tableId,
                    newPartId,
                    mutations,
                    twoPhaseCommitData
                );
                if (!result) {
                    throw new RuntimeException(txnId + " " + newPartId
                        + ", txnPreWrite false, PrimaryKey:"
                        + Arrays.toString(primaryKey));
                }
            }
            MdcUtils.setTxnId(txnId.toString());
            return true;
        };
        return CompletableFuture.supplyAsync(
            supplier,
            Executors.executor("txnPreWrite-" + txnId + "-" + tableId + "-" + newPartId)
        ).exceptionally(
            ex -> {
                if (ex != null) {
                    if (ex.getCause() instanceof WriteConflictException) {
                        throw new WriteConflictException(
                            ex.getCause().getMessage(),
                            ((WriteConflictException) ex.getCause()).key
                        );
                    } else if (ex.getCause() instanceof DuplicateEntryException) {
                        throw new DuplicateEntryException(ex.getCause().getMessage());
                    } else {
                        throw new RuntimeException(ex);
                    }
                }
                return true;
            }
        );
    }

    public static byte[] commitKey(CommonId txnId,
                                   CommonId tableId,
                                   CommonId newPartId,
                                   int op,
                                   byte[] key,
                                   boolean isPessimistic) {
        if (!isPessimistic) {
            StoreInstance store = Services.LOCAL_STORE.getInstance(tableId, newPartId);
            byte[] txnIdByte = txnId.encode();
            byte[] tableIdByte = tableId.encode();
            byte[] partIdByte = newPartId.encode();
            int len = txnIdByte.length + tableIdByte.length + partIdByte.length;
            byte[] checkBytes = ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_CHECK_DATA,
                key,
                Op.CheckNotExists.getCode(),
                len,
                txnIdByte, tableIdByte, partIdByte
            );
            KeyValue keyValue = store.get(checkBytes);
            if (keyValue != null && keyValue.getValue() != null) {
                switch (Op.forNumber(op)) {
                    case PUT:
                        op = Op.PUTIFABSENT.getCode();
                        break;
                    case DELETE:
                        op = Op.CheckNotExists.getCode();
                        break;
                    default:
                        break;
                }
                if (op == Op.CheckNotExists.getCode()) {
                    return null;
                }
            }
        }
        if (tableId.type == CommonId.CommonType.INDEX) {
            IndexTable indexTable = (IndexTable) TransactionManager.getIndex(txnId, tableId);
            if (indexTable.indexType.isVector || indexTable.indexType == IndexType.DOCUMENT) {
                KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(
                    indexTable.codecVersion,
                    indexTable.version,
                    indexTable.tupleType(),
                    indexTable.keyMapping()
                );
                Object[] decodeKey = codec.decodeKeyPrefix(key);
                TupleMapping mapping = TupleMapping.of(new int[]{0});
                DingoType dingoType = new LongType(false);
                TupleType tupleType = DingoTypeFactory.tuple(new DingoType[]{dingoType});
                KeyValueCodec keyValueCodec = CodecService.getDefault().createKeyValueCodec(
                    indexTable.codecVersion,
                    indexTable.version,
                    tupleType,
                    mapping
                );
                key = keyValueCodec.encodeKeyPrefix(new Object[]{decodeKey[0]}, 1);
            }
        }
        return key;
    }

    public static CompletableFuture<Boolean> commitSecondKeys(@NonNull TxnPartData txnPartData,
                                                       @Nullable TwoPhaseCommitData twoPhaseCommitData) {
        CommonId txnId = twoPhaseCommitData.getTxnId();
        CommonId tableId = txnPartData.getTableId();
        CommonId newPartId = txnPartData.getPartId();
        Iterator<Object[]> cacheData = TransactionCache.getCacheData(
            txnId,
            tableId,
            newPartId
        );
        byte[] primaryKey = twoPhaseCommitData.getPrimaryKey();
        Supplier<Boolean> supplier = () -> {
            MdcUtils.setTxnId(txnId.toString());
            List<byte[]> keys = new ArrayList<>();
            while (cacheData.hasNext()) {
                Object[] tuple = cacheData.next();
                TxnLocalData txnLocalData = (TxnLocalData) tuple[0];
                int op = txnLocalData.getOp().getCode();
                byte[] key = txnLocalData.getKey();
                if (ByteArrayUtils.compare(key, primaryKey, 1) == 0) {
                    continue;
                }
                key = TwoPhaseCommitUtils.commitKey(
                    txnId,
                    tableId,
                    newPartId,
                    op,
                    key,
                    twoPhaseCommitData.isPessimistic()
                );
                if (key == null) {
                    continue;
                }
                keys.add(key);
                if (keys.size() == TransactionUtil.max_pre_write_count) {
                    boolean result = TwoPhaseCommitUtils.txnCommit(txnId, tableId, newPartId, keys, twoPhaseCommitData);
                    if (!result) {
                        throw new RuntimeException(txnId + " " + newPartId
                            + ",txnCommit false,PrimaryKey:"
                            + Arrays.toString(primaryKey)
                        );
                    }
                    keys.clear();
                }
            }
            if (!keys.isEmpty()) {
                boolean result = TwoPhaseCommitUtils.txnCommit(txnId, tableId, newPartId, keys, twoPhaseCommitData);
                if (!result) {
                    throw new RuntimeException(txnId + " " + newPartId
                        + ",txnCommit false,PrimaryKey:"
                        + Arrays.toString(primaryKey)
                    );
                }
            }
            return true;
        };

        return CompletableFuture.supplyAsync(
            supplier,
            Executors.executor("txnCommitSecond-" + txnId + "-" + tableId + "-" + newPartId)
        );
    }

    public static boolean txnPreWrite(@NonNull CommonId tableId,
                                      @Nullable CommonId newPartId,
                                      @NonNull List<Mutation> mutations,
                                      @Nullable TwoPhaseCommitData twoPhaseCommitData) {
        // 1、call sdk TxnPreWrite
        TxnPreWrite txnPreWrite;
        boolean isAsyncCommit = twoPhaseCommitData.getUseAsyncCommit().get();
        if (!twoPhaseCommitData.isPessimistic()) {
            txnPreWrite = TxnPreWrite.builder()
                .isolationLevel(IsolationLevel.of(twoPhaseCommitData.getIsolationLevel()))
                .mutations(mutations)
                .primaryLock(twoPhaseCommitData.getPrimaryKey())
                .startTs(twoPhaseCommitData.getTxnId().seq)
                .lockTtl(TransactionManager.lockTtlTm())
                .txnSize(mutations.size())
                .tryOnePc(false)
                .maxCommitTs(0L)
                .useAsyncCommit(isAsyncCommit)
                .secondaries(isAsyncCommit ? twoPhaseCommitData.getSecondaries() : Collections.emptyList())
                .minCommitTs(twoPhaseCommitData.getMinCommitTs().get())
                .lockExtraDatas(toLockExtraDataList(
                    tableId,
                    newPartId,
                    twoPhaseCommitData.getTxnId(),
                    twoPhaseCommitData.getType().getCode(),
                    mutations.size())
                )
                .build();
        } else {
            // ToDo Non-unique indexes do not require pessimistic locks and are equivalent to optimistic transactions
            txnPreWrite = TxnPreWrite.builder()
                .isolationLevel(IsolationLevel.of(twoPhaseCommitData.getIsolationLevel()))
                .mutations(mutations)
                .primaryLock(twoPhaseCommitData.getPrimaryKey())
                .startTs(twoPhaseCommitData.getTxnId().seq)
                .lockTtl(TransactionManager.lockTtlTm())
                .txnSize(mutations.size())
                .tryOnePc(false)
                .maxCommitTs(0L)
                .useAsyncCommit(isAsyncCommit)
                .secondaries(isAsyncCommit ? twoPhaseCommitData.getSecondaries() : Collections.emptyList())
                .minCommitTs(twoPhaseCommitData.getMinCommitTs().get())
                .pessimisticChecks(toPessimisticCheck(mutations.size()))
                .forUpdateTsChecks(toForUpdateTsChecks(mutations))
                .lockExtraDatas(toLockExtraDataList(
                    tableId,
                    newPartId,
                    twoPhaseCommitData.getTxnId(),
                    twoPhaseCommitData.getType().getCode(),
                    mutations.size())
                )
                .build();
        }
        try {
            LogUtils.info(log, "{}-{}, txnPreWrite...", tableId, newPartId);
            StoreInstance store = Services.KV_STORE.getInstance(tableId, newPartId);
            return store.txnPreWrite(txnPreWrite, twoPhaseCommitData.getLockTimeOut());
        } catch (RegionSplitException e) {
            LogUtils.error(log, e.getMessage(), e);
            // 2、regin split
            Map<CommonId, List<byte[]>> partMap = multiKeySplitRegionId(
                tableId,
                twoPhaseCommitData.getTxnId(),
                mutationToKey(mutations)
            );
            for (Map.Entry<CommonId, List<byte[]>> entry : partMap.entrySet()) {
                CommonId regionId = entry.getKey();
                List<byte[]> value = entry.getValue();
                StoreInstance store = Services.KV_STORE.getInstance(tableId, regionId);
                txnPreWrite.setMutations(keyToMutation(value, mutations));
                boolean result = store.txnPreWrite(txnPreWrite, twoPhaseCommitData.getLockTimeOut());
                if (!result) {
                    return false;
                }
            }
            return true;
        } finally {
            if (twoPhaseCommitData.getUseAsyncCommit().get()) {
                if (txnPreWrite.getMinCommitTs() == 0) {
                    LogUtils.info(log, "Async Commit Set False");
                    twoPhaseCommitData.getUseAsyncCommit().set(false);
                } else if (txnPreWrite.getMinCommitTs() > twoPhaseCommitData.getMinCommitTs().get()) {
                    twoPhaseCommitData.getMinCommitTs().set(txnPreWrite.getMinCommitTs());
                }
            }
        }
    }

    public static boolean txnCommit(@NonNull CommonId txnId,
                                    @Nullable CommonId tableId,
                                    @Nullable CommonId newPartId,
                                    @Nullable List<byte[]> keys,
                                    @Nullable TwoPhaseCommitData twoPhaseCommitData) {
        // 1、Async call sdk TxnCommit
        TxnCommit commitRequest = TxnCommit.builder()
            .isolationLevel(IsolationLevel.of(twoPhaseCommitData.getIsolationLevel()))
            .startTs(twoPhaseCommitData.getTxnId().seq)
            .commitTs(twoPhaseCommitData.getCommitTs())
            .keys(keys)
            .build();
        try {
            LogUtils.info(log, "{}-{}, txnCommit...", tableId, newPartId);
            StoreInstance store = Services.KV_STORE.getInstance(tableId, newPartId);
            return store.txnCommit(commitRequest);
        } catch (RegionSplitException e) {
            LogUtils.error(log, e.getMessage(), e);
            // 2、regin split
            Map<CommonId, List<byte[]>> partMap = multiKeySplitRegionId(tableId, txnId, keys);
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
            return true;
        }
    }

    public static CompletableFuture<Boolean> rollBackPartData(@NonNull TxnPartData txnPartData,
                                                        @Nullable TwoPhaseCommitData twoPhaseCommitData) {
        CommonId txnId = twoPhaseCommitData.getTxnId();
        CommonId tableId = txnPartData.getTableId();
        CommonId newPartId = txnPartData.getPartId();
        Iterator<Object[]> cacheData = TransactionCache.getCacheData(
            txnId,
            tableId,
            newPartId
        );
        byte[] primaryKey = twoPhaseCommitData.getPrimaryKey();
        boolean isPessimistic = twoPhaseCommitData.isPessimistic();
        Supplier<Boolean> supplier = () -> {
            MdcUtils.setTxnId(txnId.toString());
            List<byte[]> keys = new ArrayList<>();
            List<Long> forUpdateTsList = new ArrayList<>();
            while (cacheData.hasNext()) {
                Object[] tuple = cacheData.next();
                TxnLocalData txnLocalData = (TxnLocalData) tuple[0];
                int op = txnLocalData.getOp().getCode();
                byte[] key = txnLocalData.getKey();
                long forUpdateTs = 0;
                // first key is primary key
                if (isPessimistic && (ByteArrayUtils.compare(key, primaryKey, 1) == 0)) {
                    continue;
                }
                byte[] keyBytes = Arrays.copyOf(key, key.length);
                key = commitKey(txnId, tableId, newPartId, op, key, isPessimistic);
                if (key == null) {
                    continue;
                }
                forUpdateTs = getForUpdateTs(txnId, tableId, newPartId, isPessimistic, forUpdateTs, keyBytes);
                keys.add(key);
                forUpdateTsList.add(forUpdateTs);
                if (keys.size() == TransactionUtil.max_pre_write_count) {
                    boolean result = TwoPhaseCommitUtils.txnRollBack(
                        txnId,
                        tableId,
                        newPartId,
                        keys,
                        forUpdateTsList,
                        twoPhaseCommitData
                    );
                    if (!result) {
                        throw new RuntimeException(txnId + " " + newPartId + ",txnBatchRollback false");
                    }
                    keys.clear();
                    forUpdateTsList.clear();
                }
            }
            if (!keys.isEmpty()) {
                boolean result = TwoPhaseCommitUtils.txnRollBack(
                    txnId,
                    tableId,
                    newPartId,
                    keys,
                    forUpdateTsList,
                    twoPhaseCommitData
                );
                if (!result) {
                    throw new RuntimeException(txnId + " " + newPartId + ",txnBatchRollback false");
                }
            }
            MdcUtils.setTxnId(txnId.toString());
            return true;
        };
        return CompletableFuture.supplyAsync(
            supplier,
            Executors.executor("txnRollBack-" + txnId + "-" + tableId + "-" + newPartId)
        ).whenComplete(
            (result, ex) -> {
                if (ex != null) {
                    LogUtils.error(log, ex.getMessage(), ex);
                    MdcUtils.setTxnId(txnId.toString());
                    if (isPessimistic) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        ).thenApply(
            result -> result != null ? result : true
        );
    }

    public static long getForUpdateTs(CommonId txnId, CommonId tableId, CommonId newPartId, boolean isPessimistic, long forUpdateTs, byte[] keyBytes) {
        if (isPessimistic) {
            StoreInstance store = Services.LOCAL_STORE.getInstance(tableId, newPartId);
            byte[] txnIdByte = txnId.encode();
            byte[] tableIdByte = tableId.encode();
            byte[] partIdByte = newPartId.encode();
            int len = txnIdByte.length + tableIdByte.length + partIdByte.length;
            byte[] lockBytes = ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_LOCK,
                keyBytes,
                Op.LOCK.getCode(),
                len,
                txnIdByte,
                tableIdByte,
                partIdByte);
            KeyValue keyValue = store.get(lockBytes);
            if (keyValue == null) {
                throw new RuntimeException(txnId + " lock keyValue is null key is " + Arrays.toString(keyBytes));
            }
            forUpdateTs = ByteUtils.decodePessimisticLockValue(keyValue);
        }
        return forUpdateTs;
    }

    public static boolean txnRollBack(@Nullable CommonId txnId, @Nullable CommonId tableId,
                                      @Nullable CommonId newPartId, @Nullable List<byte[]> keys,
                                      @Nullable List<Long> forUpdateTsList,
                                      @Nullable TwoPhaseCommitData twoPhaseCommitData) {
        LogUtils.info(log, "{}-{}, txnRollBack...", tableId, newPartId);
        if (twoPhaseCommitData.isPessimistic()) {
            // call sdk TxnPessimisticRollBack
            for (int i = 0; i < keys.size(); i++) {
                boolean result = txnPessimisticRollBack(
                    keys.get(i),
                    twoPhaseCommitData.getTxnId().seq,
                    forUpdateTsList.get(i),
                    twoPhaseCommitData.getIsolationLevel(),
                    txnId,
                    tableId,
                    newPartId
                );
                if (!result) {
                    return false;
                }
            }
            return true;
        } else {
            // 1、Async call sdk TxnRollBack
            TxnBatchRollBack rollBackRequest = TxnBatchRollBack.builder().
                isolationLevel(IsolationLevel.of(twoPhaseCommitData.getIsolationLevel()))
                .startTs(twoPhaseCommitData.getTxnId().seq)
                .keys(keys)
                .build();
            try {
                StoreInstance store = Services.KV_STORE.getInstance(tableId, newPartId);
                return store.txnBatchRollback(rollBackRequest);
            } catch (RegionSplitException e) {
                LogUtils.error(log, e.getMessage(), e);
                // 2、regin split
                Map<CommonId, List<byte[]>> partMap = TransactionUtil.multiKeySplitRegionId(tableId, txnId, keys);
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

    public static boolean txnPessimisticRollBack(byte[] key, long startTs, long forUpdateTs, int isolationLevel,
                                           CommonId txnId, CommonId tableId, CommonId newPartId) {
        // 1、Async call sdk TxnPessimisticRollBack
        TxnPessimisticRollBack pessimisticRollBack = TxnPessimisticRollBack.builder()
            .isolationLevel(IsolationLevel.of(isolationLevel))
            .startTs(startTs)
            .forUpdateTs(forUpdateTs)
            .keys(Collections.singletonList(key))
            .build();
        try {
            StoreInstance store = Services.KV_STORE.getInstance(tableId, newPartId);
            return store.txnPessimisticLockRollback(pessimisticRollBack);
        } catch (RegionSplitException e) {
            LogUtils.error(log, e.getMessage(), e);
            // 2、regin split
            Map<CommonId, List<byte[]>> partMap = TransactionUtil.multiKeySplitRegionId(
                tableId,
                txnId,
                Collections.singletonList(key)
            );
            for (Map.Entry<CommonId, List<byte[]>> entry : partMap.entrySet()) {
                CommonId regionId = entry.getKey();
                List<byte[]> value = entry.getValue();
                StoreInstance store = Services.KV_STORE.getInstance(tableId, regionId);
                pessimisticRollBack.setKeys(value);
                boolean result = store.txnPessimisticLockRollback(pessimisticRollBack);
                if (!result) {
                    return false;
                }
            }
            return true;
        }
    }
}
