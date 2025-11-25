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
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.log.MdcUtils;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.TupleType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
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
import io.dingodb.store.api.transaction.exception.CommitTsExpiredException;
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
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static io.dingodb.exec.transaction.util.TransactionUtil.keyToMutation;
import static io.dingodb.exec.transaction.util.TransactionUtil.multiKeySplitRegionId;
import static io.dingodb.exec.transaction.util.TransactionUtil.mutationToKey;
import static io.dingodb.exec.transaction.util.TransactionUtil.toForUpdateTsChecks;
import static io.dingodb.exec.transaction.util.TransactionUtil.toLockExtraDataList;
import static io.dingodb.exec.transaction.util.TransactionUtil.toPessimisticCheck;

@Slf4j
public final class TwoPhaseCommitUtils {

    public static final long RETRY_INTERVAL_MS = 100;

    private TwoPhaseCommitUtils() {
    }

    public static CompletableFuture<Long> preWriteSecondKeys(@NonNull TxnPartData txnPartData,
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
        Supplier<Long> supplier = () -> {
            MdcUtils.setTxnId(txnId.toString());
            long count = 0L;
            boolean isPrimaryKeyPre = false;
            while (cacheData.hasNext()) {
                Object[] tuple = cacheData.next();
                TxnLocalData txnLocalData = (TxnLocalData) tuple[0];
                int op = txnLocalData.getOp().getCode();
                byte[] key = txnLocalData.getKey();
                byte[] value = txnLocalData.getValue();
                // first key is primary key
                boolean isPrimaryKey = ByteArrayUtils.compare(key, primaryKey, 1) == 0;
                if (isPrimaryKey && !twoPhaseCommitData.isParallelPreWrite()) {
                    continue;
                }
                if (isPrimaryKey && !twoPhaseCommitData.isPessimistic()) {
                    isPrimaryKeyPre = true;
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
                if (mutation.getOp() == Op.CheckNotExists) {
                    continue;
                }
                LogUtils.debug(log, "mutation: {}", mutation);
                mutations.add(mutation);
                if (mutations.size() == TransactionUtil.max_pre_write_count) {
                    boolean result;
                    if (isPrimaryKeyPre) {
                        result = TwoPhaseCommitUtils.txnPreWritePrimaryKey(
                            tableId,
                            newPartId,
                            mutations,
                            twoPhaseCommitData
                        );
                        isPrimaryKeyPre = false;
                    } else {
                        result = TwoPhaseCommitUtils.txnPreWrite(
                            tableId,
                            newPartId,
                            mutations,
                            twoPhaseCommitData
                        );
                    }
                    if (!result) {
                        throw new RuntimeException(txnId + " " + newPartId
                            + ", txnPreWrite false, PrimaryKey:"
                            + Arrays.toString(primaryKey));
                    }
                    mutations.clear();
                    count += TransactionUtil.max_pre_write_count;
                }
            }

            if (!mutations.isEmpty()) {
                boolean result;
                if (isPrimaryKeyPre) {
                    result = TwoPhaseCommitUtils.txnPreWritePrimaryKey(
                        tableId,
                        newPartId,
                        mutations,
                        twoPhaseCommitData
                    );
                } else {
                    result = TwoPhaseCommitUtils.txnPreWrite(
                        tableId,
                        newPartId,
                        mutations,
                        twoPhaseCommitData
                    );
                }
                if (!result) {
                    throw new RuntimeException(txnId + " " + newPartId
                        + ", txnPreWrite false, PrimaryKey:"
                        + Arrays.toString(primaryKey));
                }
                count += mutations.size();
            }
            MdcUtils.setTxnId(txnId.toString());
            return count;
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
                return 0L;
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

    public static CompletableFuture<Long> commitSecondKeys(@NonNull TxnPartData txnPartData,
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
        Supplier<Long> supplier = () -> {
            MdcUtils.setTxnId(txnId.toString());
            boolean isPrimaryKeyCommit = false;
            long count = 0L;
            List<byte[]> keys = new ArrayList<>();
            while (cacheData.hasNext()) {
                Object[] tuple = cacheData.next();
                TxnLocalData txnLocalData = (TxnLocalData) tuple[0];
                int op = txnLocalData.getOp().getCode();
                byte[] key = txnLocalData.getKey();
                boolean isPrimaryKey = ByteArrayUtils.compare(key, primaryKey, 1) == 0;
                if (isPrimaryKey && !twoPhaseCommitData.isParallelCommit()) {
                    continue;
                }
                if (isPrimaryKey) {
                    isPrimaryKeyCommit = true;
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
                    boolean result;
                    if (isPrimaryKeyCommit) {
                        result = TwoPhaseCommitUtils.txnCommitPrimaryKey(
                            txnId,
                            tableId,
                            newPartId,
                            keys,
                            twoPhaseCommitData
                        );
                        isPrimaryKeyCommit = false;
                    } else {
                        result = TwoPhaseCommitUtils.txnCommit(
                            txnId,
                            tableId,
                            newPartId,
                            keys,
                            twoPhaseCommitData
                        );
                    }
                    if (!result) {
                        throw new RuntimeException(txnId + " " + newPartId
                            + ",txnCommit false,PrimaryKey:"
                            + Arrays.toString(primaryKey)
                        );
                    }
                    keys.clear();
                    count += TransactionUtil.max_pre_write_count;
                }
            }
            if (!keys.isEmpty()) {
                boolean result;
                if (isPrimaryKeyCommit) {
                    result = TwoPhaseCommitUtils.txnCommitPrimaryKey(
                        txnId,
                        tableId,
                        newPartId,
                        keys,
                        twoPhaseCommitData
                    );
                } else {
                    result = TwoPhaseCommitUtils.txnCommit(
                        txnId,
                        tableId,
                        newPartId,
                        keys,
                        twoPhaseCommitData
                    );
                }
                if (!result) {
                    throw new RuntimeException(txnId + " " + newPartId
                        + ",txnCommit false,PrimaryKey:"
                        + Arrays.toString(primaryKey)
                    );
                }
                count += keys.size();
            }
            return count;
        };

        return CompletableFuture.supplyAsync(
            supplier,
            Executors.executor("txnCommitSecond-" + txnId + "-" + tableId + "-" + newPartId)
        );
    }

    public static boolean txnPreWritePrimaryKey(@NonNull CommonId tableId,
                                               @Nullable CommonId newPartId,
                                               @NonNull List<Mutation> mutations,
                                               @Nullable TwoPhaseCommitData twoPhaseCommitData) {
        Future future = null;
        // 1、call sdk TxnPreWrite
        TxnPreWrite txnPreWrite = buildTxnPreWriteRequest(twoPhaseCommitData, mutations, tableId, newPartId);
        final int MAX_RETRY_TIMES = Optional.mapOrGet(
            DingoConfiguration.instance().find("retry", int.class),
            __ -> __,
            () -> 60);
        try {
            LogUtils.info(log, "{}-{}, txnParallelPreWrite PrimaryKey...", tableId, newPartId);
            StoreInstance store = Services.KV_STORE.getInstance(tableId, newPartId);
            future = store.txnPreWritePrimaryKey(txnPreWrite, twoPhaseCommitData.getLockTimeOut());
            if (future == null) {
                throw new RuntimeException("Future is null, txnParallelPreWrite PrimaryKey false");
            }
            twoPhaseCommitData.setFuture(future);
            twoPhaseCommitData.getPrimaryKeyPreWrite().compareAndSet(false, true);
            LogUtils.info(log, "{}-{}, txnParallelPreWrite PrimaryKey end", tableId, newPartId);
            return true;
        } catch (RegionSplitException e) {
            LogUtils.error(log, "txnParallelPreWrite PrimaryKey regionSplitException occurred, retrying...", e);
            for (int retry = 1; retry < MAX_RETRY_TIMES; retry++) {
                try {
                    // 2、regin split
                    Map<CommonId, List<byte[]>> partMap = multiKeySplitRegionId(
                        tableId,
                        twoPhaseCommitData.getTxnId(),
                        mutationToKey(mutations)
                    );
                    for (Map.Entry<CommonId, List<byte[]>> entry : partMap.entrySet()) {
                        CommonId regionId = entry.getKey();
                        List<byte[]> value = entry.getValue();
                        boolean result = txnPreWriteRegionSplitRetry(
                            tableId,
                            regionId,
                            keyToMutation(value, mutations),
                            twoPhaseCommitData,
                            MAX_RETRY_TIMES
                        );
                        if (!result) {
                            LogUtils.warn(log, "txnParallelPreWrite PrimaryKey failed for region: {}", regionId);
                            break;
                        }
                    }
                    LogUtils.info(log, "txnParallelPreWrite PrimaryKey successful after retry {}", retry);
                    CommonId primaryKeyPartId = TransactionUtil.singleKeySplitRegionId(
                        tableId,
                        twoPhaseCommitData.getTxnId(),
                        twoPhaseCommitData.getPrimaryKey()
                    );
                    StoreInstance store = Services.KV_STORE.getInstance(tableId, primaryKeyPartId);
                    future = store.txnHeartBeat(twoPhaseCommitData.getTxnId().seq, twoPhaseCommitData.getPrimaryKey());
                    if (future == null) {
                        throw new RuntimeException("RegionSplit retry future is null, " +
                            "txnParallelPreWrite PrimaryKey false");
                    }
                    twoPhaseCommitData.setFuture(future);
                    twoPhaseCommitData.getPrimaryKeyPreWrite().compareAndSet(false, true);
                    LogUtils.info(log, "{}-{}, txnParallelPreWrite PrimaryKey end", tableId, primaryKeyPartId);
                    return true;
                } catch (RegionSplitException re) {
                    LogUtils.warn(log, "txnParallelPreWrite PrimaryKey retry:" + retry + " failed", re);
                    if (sleep()) {
                        return false;
                    }
                } catch (Exception ex) {
                    LogUtils.error(log, "txnParallelPreWrite PrimaryKey unexpected error during retry :" +
                        retry, ex);
                    return false;
                }
            }
            LogUtils.error(log, "Failed to txnParallelPreWrite PrimaryKey after {} retries", MAX_RETRY_TIMES);
            return false;
        } finally {
            if (twoPhaseCommitData.getUseAsyncCommit().get()) {
                if (txnPreWrite.getMinCommitTs() == 0) {
                    LogUtils.info(log, "txnParallelPreWrite PrimaryKey Async Commit Set False");
                    twoPhaseCommitData.getUseAsyncCommit().set(false);
                } else if (txnPreWrite.getMinCommitTs() > twoPhaseCommitData.getMinCommitTs().get()) {
                    twoPhaseCommitData.getMinCommitTs().set(txnPreWrite.getMinCommitTs());
                }
            }
        }
    }

    public static boolean txnPreWrite(@NonNull CommonId tableId,
                                      @Nullable CommonId newPartId,
                                      @NonNull List<Mutation> mutations,
                                      @Nullable TwoPhaseCommitData twoPhaseCommitData) {
        // 1、call sdk TxnPreWrite
        TxnPreWrite txnPreWrite = buildTxnPreWriteRequest(twoPhaseCommitData, mutations, tableId, newPartId);
        final int MAX_RETRY_TIMES = Optional.mapOrGet(
            DingoConfiguration.instance().find("retry", int.class),
            __ -> __,
            () -> 60);
        try {
            LogUtils.info(log, "{}-{}, txnPreWrite...", tableId, newPartId);
            StoreInstance store = Services.KV_STORE.getInstance(tableId, newPartId);
            return store.txnPreWrite(txnPreWrite, twoPhaseCommitData.getLockTimeOut());
        } catch (RegionSplitException e) {
            LogUtils.error(log, "txnPreWrite regionSplitException occurred, retrying...", e);
            for (int retry = 1; retry < MAX_RETRY_TIMES; retry++) {
                try {
                    // 2、regin split
                    Map<CommonId, List<byte[]>> partMap = multiKeySplitRegionId(
                        tableId,
                        twoPhaseCommitData.getTxnId(),
                        mutationToKey(mutations)
                    );
                    for (Map.Entry<CommonId, List<byte[]>> entry : partMap.entrySet()) {
                        CommonId regionId = entry.getKey();
                        List<byte[]> value = entry.getValue();
                        boolean result = txnPreWriteRegionSplitRetry(
                            tableId,
                            regionId,
                            keyToMutation(value, mutations),
                            twoPhaseCommitData,
                            MAX_RETRY_TIMES
                        );
                        if (!result) {
                            LogUtils.warn(log, "txnPreWrite failed for region: {}", regionId);
                            break;
                        }
                    }
                    LogUtils.info(log, "txnPreWrite successful after retry {}", retry);
                    return true;
                } catch (RegionSplitException re) {
                    LogUtils.warn(log, "txnPreWrite retry:" + retry + " failed", re);
                    if (sleep()) {
                        return false;
                    }
                } catch (Exception ex) {
                    LogUtils.error(log, "txnPreWrite unexpected error during retry :" + retry, ex);
                    return false;
                }
            }
            LogUtils.error(log, "Failed to txnPreWrite after {} retries", MAX_RETRY_TIMES);
            return false;
        } finally {
            if (twoPhaseCommitData.getUseAsyncCommit().get()) {
                if (txnPreWrite.getMinCommitTs() == 0) {
                    LogUtils.info(log, "TxnPreWrite Async Commit Set False");
                    twoPhaseCommitData.getUseAsyncCommit().set(false);
                } else if (txnPreWrite.getMinCommitTs() > twoPhaseCommitData.getMinCommitTs().get()) {
                    twoPhaseCommitData.getMinCommitTs().set(txnPreWrite.getMinCommitTs());
                }
            }
        }
    }

    private static boolean txnPreWriteRegionSplitRetry(@NonNull CommonId tableId,
                                      @Nullable CommonId newPartId,
                                      @NonNull List<Mutation> mutations,
                                      @Nullable TwoPhaseCommitData twoPhaseCommitData,
                                      int retry) {
        assert twoPhaseCommitData != null;
        // 1、call sdk TxnPreWrite
        TxnPreWrite txnPreWrite = buildTxnPreWriteRequest(twoPhaseCommitData, mutations, tableId, newPartId);
        try {
            if (sleep()) {
                return false;
            }
            LogUtils.info(log, "{}-{}, txnPreWriteRegionSplitRetry...", tableId, newPartId);
            StoreInstance store = Services.KV_STORE.getInstance(tableId, newPartId);
            return store.txnPreWrite(txnPreWrite, twoPhaseCommitData.getLockTimeOut());
        } catch (RegionSplitException e) {
            LogUtils.error(log, "txnPreWriteRegionSplitRetry regionSplitException occurred, retrying...", e);
            while (retry-- > 0) {
                try {
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
                        if (twoPhaseCommitData.getUseAsyncCommit().get()) {
                            if (txnPreWrite.getMinCommitTs() == 0) {
                                LogUtils.info(log, "TxnPreWriteRegionSplitRetry Async Commit Set False");
                                twoPhaseCommitData.getUseAsyncCommit().set(false);
                            } else if (txnPreWrite.getMinCommitTs() > twoPhaseCommitData.getMinCommitTs().get()) {
                                twoPhaseCommitData.getMinCommitTs().set(txnPreWrite.getMinCommitTs());
                            }
                        }
                        if (!result) {
                            LogUtils.warn(log, "txnPreWriteRegionSplitRetry failed for region: {}", regionId);
                            break;
                        }
                    }
                    LogUtils.info(log, "txnPreWriteRegionSplitRetry successful after retry {}", retry);
                    return true;
                } catch (RegionSplitException re) {
                    LogUtils.warn(log, "txnPreWriteRegionSplitRetry Retry:" + retry + " failed", re);
                    if (sleep()) {
                        return false;
                    }
                } catch (Exception ex) {
                    LogUtils.error(log, "txnPreWriteRegionSplitRetry unexpected error during retry", ex);
                    return false;
                }
            }
            LogUtils.error(log, "Failed to txnPreWriteRegionSplitRetry after {} retries", retry);
            return false;
        } finally {
            if (twoPhaseCommitData.getUseAsyncCommit().get()) {
                if (txnPreWrite.getMinCommitTs() == 0) {
                    LogUtils.info(log, "TxnPreWriteRegionSplitRetry Async Commit Set False");
                    twoPhaseCommitData.getUseAsyncCommit().set(false);
                } else if (txnPreWrite.getMinCommitTs() > twoPhaseCommitData.getMinCommitTs().get()) {
                    twoPhaseCommitData.getMinCommitTs().set(txnPreWrite.getMinCommitTs());
                }
            }
        }
    }

    private static TxnPreWrite buildTxnPreWriteRequest(@Nullable TwoPhaseCommitData twoPhaseCommitData,
                                                       @NonNull List<Mutation> mutations,
                                                       @NonNull CommonId tableId,
                                                       @Nullable CommonId newPartId) {
        TxnPreWrite txnPreWrite;
        assert twoPhaseCommitData != null;
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
        return txnPreWrite;
    }

    public static boolean txnCommitPrimaryKey(@NonNull CommonId txnId,
                                    @Nullable CommonId tableId,
                                    @Nullable CommonId newPartId,
                                    @Nullable List<byte[]> keys,
                                    @Nullable TwoPhaseCommitData twoPhaseCommitData) {
        assert twoPhaseCommitData != null;

        final int MAX_RETRY_TIMES = Optional.mapOrGet(
            DingoConfiguration.instance().find("retry", int.class),
            __ -> __,
            () -> 60);
        int commitRetry = MAX_RETRY_TIMES;
        while (commitRetry-- > 0) {
            try {
                // 1、Async call sdk TxnCommit
                TxnCommit commitRequest = buildCommitRequest(keys, twoPhaseCommitData);
                LogUtils.info(log, "{}-{}, txnParallelCommitPrimaryKey...", tableId, newPartId);
                StoreInstance store = Services.KV_STORE.getInstance(tableId, newPartId);
                return store.txnCommit(commitRequest);
            } catch (RegionSplitException e) {
                LogUtils.error(log, "txnParallelCommitPrimaryKey regionSplitException occurred, retrying...", e);
                for (int retry = 1; retry < MAX_RETRY_TIMES; retry++) {
                    try {
                        Map<CommonId, List<byte[]>> partMap = multiKeySplitRegionId(tableId, txnId, keys);
                        for (Map.Entry<CommonId, List<byte[]>> entry : partMap.entrySet()) {
                            CommonId regionId = entry.getKey();
                            List<byte[]> value = entry.getValue();
                            LogUtils.info(log, "RegionSplit retry {}-{}, txnParallelCommitPrimaryKey...",
                                tableId, regionId);
                            boolean result = txnCommitRegionSplitRetry(
                                txnId,
                                tableId,
                                regionId,
                                value,
                                twoPhaseCommitData,
                                MAX_RETRY_TIMES
                            );
                            if (!result) {
                                LogUtils.warn(log, "txnParallelCommitPrimaryKey failed for region: {}",
                                    regionId);
                                break;
                            }
                        }
                        LogUtils.info(log, "txnParallelCommitPrimaryKey successful after retry {}", retry);
                        return true;
                    } catch (RegionSplitException re) {
                        LogUtils.warn(log, "txnParallelCommitPrimaryKey retry:" + retry + " failed", re);
                        if (sleep()) {
                            return false;
                        }
                    } catch (Exception ex) {
                        LogUtils.error(log, "txnParallelCommitPrimaryKey unexpected error during retry :" +
                            retry, ex);
                        return false;
                    }
                }
                LogUtils.error(log, "Failed to txnParallelCommitPrimaryKey after {} retries", MAX_RETRY_TIMES);
                return false;
            } catch (CommitTsExpiredException e) {
                LogUtils.error(log, e.getMessage(), e);
                long commitTs = TransactionManager.getCommitTs();
                LogUtils.info(log, "txnParallelCommitPrimaryKey CommitTsExpiredException after retry: {}, " +
                    "commitTs: {}", commitRetry, commitTs);
                twoPhaseCommitData.setCommitTs(commitTs);
            }
        }
        return false;
    }

    public static boolean txnCommit(@NonNull CommonId txnId,
                                    @Nullable CommonId tableId,
                                    @Nullable CommonId newPartId,
                                    @Nullable List<byte[]> keys,
                                    @Nullable TwoPhaseCommitData twoPhaseCommitData) {
        assert twoPhaseCommitData != null;
        // 1、Async call sdk TxnCommit
        TxnCommit commitRequest = buildCommitRequest(keys, twoPhaseCommitData);

        final int MAX_RETRY_TIMES = Optional.mapOrGet(
            DingoConfiguration.instance().find("retry", int.class),
            __ -> __,
            () -> 60);

        try {
            LogUtils.info(log, "{}-{}, txnCommit...", tableId, newPartId);
            StoreInstance store = Services.KV_STORE.getInstance(tableId, newPartId);
            return store.txnCommit(commitRequest);
        } catch (RegionSplitException e) {
            LogUtils.error(log, "txnCommit regionSplitException occurred, retrying...", e);
            for (int retry = 1; retry < MAX_RETRY_TIMES; retry++) {
                try {
                    Map<CommonId, List<byte[]>> partMap = multiKeySplitRegionId(tableId, txnId, keys);
                    for (Map.Entry<CommonId, List<byte[]>> entry : partMap.entrySet()) {
                        CommonId regionId = entry.getKey();
                        List<byte[]> value = entry.getValue();
                        LogUtils.info(log, "RegionSplit retry {}-{}, txnCommit...", tableId, regionId);
                        boolean result = txnCommitRegionSplitRetry(
                            txnId,
                            tableId,
                            regionId,
                            value,
                            twoPhaseCommitData,
                            MAX_RETRY_TIMES
                        );
                        if (!result) {
                            LogUtils.warn(log, "txnCommit failed for region: {}", regionId);
                            break;
                        }
                    }
                    LogUtils.info(log, "txnCommit successful after retry {}", retry);
                    return true;
                } catch (RegionSplitException re) {
                    LogUtils.warn(log, "txnCommit retry:" + retry + " failed", re);
                    if (sleep()) {
                        return false;
                    }
                } catch (Exception ex) {
                    LogUtils.error(log, "txnCommit unexpected error during retry :" + retry, ex);
                    return false;
                }
            }
            LogUtils.error(log, "Failed to txnCommit after {} retries", MAX_RETRY_TIMES);
            return false;
        }
    }

    private static boolean txnCommitRegionSplitRetry(@NonNull CommonId txnId,
                                    @Nullable CommonId tableId,
                                    @Nullable CommonId newPartId,
                                    @Nullable List<byte[]> keys,
                                    @Nullable TwoPhaseCommitData twoPhaseCommitData,
                                    int retry) {
        assert twoPhaseCommitData != null;
        // 1、Async call sdk TxnCommit
        TxnCommit commitRequest = buildCommitRequest(keys, twoPhaseCommitData);
        try {
            if (sleep()) {
                return false;
            }
            LogUtils.info(log, "{}-{}, txnCommitRegionSplitRetry...", tableId, newPartId);
            StoreInstance store = Services.KV_STORE.getInstance(tableId, newPartId);
            return store.txnCommit(commitRequest);
        } catch (RegionSplitException e) {
            LogUtils.error(log, "txnCommitRegionSplitRetry regionSplitException occurred, retrying...", e);
            while (retry-- > 0) {
                try {
                    Map<CommonId, List<byte[]>> partMap = multiKeySplitRegionId(tableId, txnId, keys);
                    for (Map.Entry<CommonId, List<byte[]>> entry : partMap.entrySet()) {
                        CommonId regionId = entry.getKey();
                        List<byte[]> value = entry.getValue();
                        LogUtils.info(log, "RegionSplit retry {}-{}, txnCommitRegionSplitRetry...", tableId, regionId);
                        StoreInstance store = Services.KV_STORE.getInstance(tableId, regionId);
                        commitRequest.setKeys(value);
                        boolean result = store.txnCommit(commitRequest);
                        if (!result) {
                            LogUtils.warn(log, "txnCommitRegionSplitRetry failed for region: {}", regionId);
                            break;
                        }
                    }
                    LogUtils.info(log, "txnCommitRegionSplitRetry successful after retry {}", retry);
                    return true;
                } catch (RegionSplitException re) {
                    LogUtils.warn(log, "txnCommitRegionSplitRetry Retry:" + retry + " failed", re);
                    if (sleep()) {
                        return false;
                    }
                } catch (Exception ex) {
                    LogUtils.error(log, "txnCommitRegionSplitRetry unexpected error during retry", ex);
                    return false;
                }
            }
            LogUtils.error(log, "Failed to txnCommitRegionSplitRetry after {} retries", retry);
            return false;
        }
    }

    private static TxnCommit buildCommitRequest(@Nullable List<byte[]> keys, @NonNull TwoPhaseCommitData twoPhaseCommitData) {
        TxnCommit commitRequest = TxnCommit.builder()
            .isolationLevel(IsolationLevel.of(twoPhaseCommitData.getIsolationLevel()))
            .startTs(twoPhaseCommitData.getTxnId().seq)
            .commitTs(twoPhaseCommitData.getCommitTs())
            .keys(keys)
            .build();
        return commitRequest;
    }

    private static boolean sleep() {
        try {
            Thread.sleep(RETRY_INTERVAL_MS);
        } catch (InterruptedException ie) {
            LogUtils.warn(log, "Interrupted during retry sleep", ie);
            Thread.currentThread().interrupt();
            return true;
        }
        return false;
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
        assert twoPhaseCommitData != null;
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
            TxnBatchRollBack rollBackRequest = buildTxnBatchRollBackRequest(keys, twoPhaseCommitData);
            final int MAX_RETRY_TIMES = Optional.mapOrGet(
                DingoConfiguration.instance().find("retry", int.class),
                __ -> __,
                () -> 60);
            try {
                StoreInstance store = Services.KV_STORE.getInstance(tableId, newPartId);
                return store.txnBatchRollback(rollBackRequest);
            } catch (RegionSplitException e) {
                LogUtils.error(log, "txnRollBack regionSplitException occurred, retrying...", e);
                for (int retry = 1; retry < MAX_RETRY_TIMES; retry++) {
                    try {
                        // 2、regin split
                        Map<CommonId, List<byte[]>> partMap = TransactionUtil.multiKeySplitRegionId(tableId, txnId, keys);
                        for (Map.Entry<CommonId, List<byte[]>> entry : partMap.entrySet()) {
                            CommonId regionId = entry.getKey();
                            List<byte[]> value = entry.getValue();
                            boolean result = txnRollBackRegionSplitRetry(
                                txnId,
                                tableId,
                                regionId,
                                value,
                                twoPhaseCommitData,
                                MAX_RETRY_TIMES
                            );
                            if (!result) {
                                return false;
                            }
                        }
                        LogUtils.info(log, "txnRollBack successful after retry {}", retry);
                        return true;
                    } catch (RegionSplitException re) {
                        LogUtils.warn(log, "txnRollBack retry:" + retry + " failed", re);
                        if (sleep()) {
                            return false;
                        }
                    } catch (Exception ex) {
                        LogUtils.error(log, "txnRollBack unexpected error during retry :" + retry, ex);
                        return false;
                    }
                }
            }
            LogUtils.error(log, "Failed to txnRollBack after {} retries", MAX_RETRY_TIMES);
            return false;
        }
    }

    private static boolean txnRollBackRegionSplitRetry(@Nullable CommonId txnId, @Nullable CommonId tableId,
                                                       @Nullable CommonId newPartId, @Nullable List<byte[]> keys,
                                                       @Nullable TwoPhaseCommitData twoPhaseCommitData,
                                                       int retry) {
        // 1、Async call sdk TxnRollBack
        assert twoPhaseCommitData != null;
        TxnBatchRollBack rollBackRequest = buildTxnBatchRollBackRequest(keys, twoPhaseCommitData);
        try {
            if (sleep()) {
                return false;
            }
            StoreInstance store = Services.KV_STORE.getInstance(tableId, newPartId);
            return store.txnBatchRollback(rollBackRequest);
        } catch (RegionSplitException e) {
            LogUtils.error(log, "txnRollBackRegionSplitRetry regionSplitException occurred, retrying...", e);
            while (retry-- > 0) {
                try {
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
                    LogUtils.info(log, "txnRollBackRegionSplitRetry successful after retry {}", retry);
                    return true;
                } catch (RegionSplitException re) {
                    LogUtils.warn(log, "txnRollBackRegionSplitRetry retry:" + retry + " failed", re);
                    if (sleep()) {
                        return false;
                    }
                } catch (Exception ex) {
                    LogUtils.error(log, "txnRollBackRegionSplitRetry unexpected error during retry", ex);
                    return false;
                }
            }
        } catch (Exception ex) {
            LogUtils.error(log, "txnRollBackRegionSplitRetry unexpected error retry:" + retry , ex);
            return false;
        }
        LogUtils.error(log, "Failed to txnRollBackRegionSplitRetry after {} retries", retry);
        return false;
    }

    private static TxnBatchRollBack buildTxnBatchRollBackRequest(@Nullable List<byte[]> keys,
                                                                 @NonNull TwoPhaseCommitData twoPhaseCommitData) {
        return TxnBatchRollBack.builder()
            .isolationLevel(IsolationLevel.of(twoPhaseCommitData.getIsolationLevel()))
            .startTs(twoPhaseCommitData.getTxnId().seq)
            .keys(keys)
            .build();
    }

    public static boolean txnPessimisticRollBack(@NonNull byte[] key,
                                                 long startTs,
                                                 long forUpdateTs,
                                                 int isolationLevel,
                                                 @NonNull CommonId txnId,
                                                 @NonNull CommonId tableId,
                                                 @NonNull CommonId newPartId) {
        Integer retry = Optional.mapOrGet(
            DingoConfiguration.instance().find("retry", int.class),
            __ -> __,
            () -> 60);
        while (retry-- > 0) {
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
                LogUtils.error(log, "txnPessimisticRollBack regionSplitException occurred, retry:" + retry, e);
                // 2、regin split
                newPartId = TransactionUtil.singleKeySplitRegionId(
                    tableId,
                    txnId,
                    key
                );
            } catch (Exception e) {
                LogUtils.error(log, "txnPessimisticRollBack exception occurred, retry:" + retry, e);
                return false;
            }
        }
        return false;
    }
}
