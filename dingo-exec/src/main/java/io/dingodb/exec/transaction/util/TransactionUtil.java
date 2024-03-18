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
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.Services;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.Mutation;
import io.dingodb.store.api.transaction.data.pessimisticlock.TxnPessimisticLock;
import io.dingodb.store.api.transaction.data.prewrite.ForUpdateTsCheck;
import io.dingodb.store.api.transaction.data.prewrite.LockExtraData;
import io.dingodb.store.api.transaction.data.prewrite.LockExtraDataList;
import io.dingodb.store.api.transaction.data.prewrite.PessimisticCheck;
import io.dingodb.store.api.transaction.data.rollback.TxnPessimisticRollBack;
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


@Slf4j
public class TransactionUtil {
    public static final long lock_ttl = 60000L;
    public static final int max_pre_write_count = 1024;
    public static final String snapshotIsolation = "REPEATABLE-READ";
    public static final String readCommitted = "READ-COMMITTED";

    public static int convertIsolationLevel(String transactionIsolation) {
        // for local test
        if (transactionIsolation == null) {
            return 1;
        }
        if (transactionIsolation.equalsIgnoreCase(snapshotIsolation)) {
            return 1;
        } else if (transactionIsolation.equalsIgnoreCase(readCommitted)) {
            return 2;
        } else {
            throw new RuntimeException("The set transaction isolation level is not currently supported.");
        }
    }
    public static <T> List<Set<T>> splitSetIntoSubsets(Set<T> set, int batchSize) {
        List<T> tempList = new ArrayList<>(set);
        List<Set<T>> subsets = new ArrayList<>();
        for (int i = 0; i < tempList.size(); i += batchSize) {
            subsets.add(new HashSet<>(tempList.subList(i, Math.min(i + batchSize, tempList.size()))));
        }
        return subsets;
    }

    public static CommonId singleKeySplitRegionId(CommonId tableId, CommonId txnId, byte[] key) {
        // 2、regin split
        MetaService root = MetaService.root();
        Table table = root.getTable(tableId);
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution = root.getRangeDistribution(tableId);
        if (Optional.ofNullable(table.getPartitionStrategy())
            .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME)
            .equalsIgnoreCase(DingoPartitionServiceProvider.RANGE_FUNC_NAME)) {
            CodecService.getDefault().setId(key, 0L);
        }
        CommonId regionId = PartitionService.getService(
                Optional.ofNullable(table.getPartitionStrategy())
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
            .calcPartId(key, rangeDistribution);
        log.info("{} regin split retry tableId:{} regionId:{}", txnId, tableId, regionId);
        return regionId;
    }

    public static Map<CommonId, List<byte[]>> multiKeySplitRegionId(CommonId tableId, CommonId txnId, List<byte[]> keys) {
        // 2、regin split
        MetaService root = MetaService.root();
        Table table = root.getTable(tableId);
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution = root.getRangeDistribution(tableId);
        final PartitionService ps = PartitionService.getService(
            Optional.ofNullable(table.getPartitionStrategy())
                .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
        if (Optional.ofNullable(table.getPartitionStrategy())
            .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME)
            .equalsIgnoreCase(DingoPartitionServiceProvider.RANGE_FUNC_NAME)) {
            keys.forEach( k -> CodecService.getDefault().setId(k, 0L));
        }
        Map<CommonId, List<byte[]>> partMap = ps.partKeys(keys, rangeDistribution);
        log.info("{} regin split retry tableId:{}", txnId, tableId);
        return partMap;
    }

    public static IndexTable getIndexDefinitions(CommonId tableId) {
        MetaService root = MetaService.root();
        return (IndexTable) root.getTable(tableId);
    }
    public static List<byte[]> mutationToKey(List<Mutation> mutations) {
        List<byte[]> keys = new ArrayList<>(mutations.size());
        for (Mutation mutation:mutations) {
            keys.add(mutation.getKey());
        }
        return keys;
    }

    public static List<Mutation> keyToMutation(List<byte[]> keys, List<Mutation> srcMutations) {
        List<Mutation> mutations = new ArrayList<>(keys.size());
        for (Mutation mutation: srcMutations) {
            if (keys.contains(mutation.getKey())) {
                mutations.add(mutation);
            }
        }
        return mutations;
    }

    public static List<LockExtraData> toLockExtraDataList(CommonId tableId, CommonId partId, CommonId txnId,
                                                          int transactionType, int size) {
        LockExtraDataList lockExtraData = LockExtraDataList.builder()
            .tableId(tableId)
            .partId(partId)
            .serverId(TransactionManager.getServerId())
            .txnId(txnId)
            .transactionType(transactionType).build();
        byte[] encode = lockExtraData.encode();
        List<LockExtraData> lockExtraDataList = IntStream.range(0, size)
            .mapToObj(i -> new LockExtraData(i, encode))
            .collect(Collectors.toList());
        return lockExtraDataList;
    }

    public static byte[] toLockExtraData(CommonId tableId, CommonId partId,
                                         CommonId txnId, int transactionType) {
        LockExtraDataList lockExtraData = LockExtraDataList.builder()
            .tableId(tableId)
            .partId(partId)
            .serverId(TransactionManager.getServerId())
            .txnId(txnId)
            .transactionType(transactionType).build();
        return lockExtraData.encode();
    }

    public static List<PessimisticCheck> toPessimisticCheck(int size) {
        return IntStream.range(0, size)
            .mapToObj(i -> PessimisticCheck.DO_PESSIMISTIC_CHECK)
            .collect(Collectors.toList());
    }

    public static List<ForUpdateTsCheck> toForUpdateTsChecks(List<Mutation> mutations) {
        List<ForUpdateTsCheck> forUpdateTsChecks = IntStream.range(0, mutations.size())
            .mapToObj(i -> new ForUpdateTsCheck(i, mutations.get(i).getForUpdateTs()))
            .collect(Collectors.toList());
        return forUpdateTsChecks;
    }

    public static TxnPessimisticLock pessimisticLock(long timeOut, CommonId txnId, CommonId tableId,
                                                     CommonId partId, byte[] primaryLockKey,
                                                     byte[] key, long startTs,
                                                     long forUpdateTs, int isolationLevel) {
        TxnPessimisticLock txnPessimisticLock = TxnPessimisticLock.builder()
            .isolationLevel(IsolationLevel.of(isolationLevel))
            .primaryLock(primaryLockKey)
            .mutations(Collections.singletonList(
                TransactionCacheToMutation.cacheToPessimisticLockMutation(
                    key,
                    toLockExtraData(
                        tableId,
                        partId,
                        txnId,
                        TransactionType.PESSIMISTIC.getCode()
                    ),
                    forUpdateTs
                )
            ))
            .lockTtl(TransactionManager.lockTtlTm())
            .startTs(startTs)
            .forUpdateTs(forUpdateTs)
            .build();
        try {
            StoreInstance store = Services.KV_STORE.getInstance(tableId, partId);
            boolean result = store.txnPessimisticLock(txnPessimisticLock, timeOut);
            if (!result) {
                throw new RuntimeException(txnId + " " + partId + ",txnPessimisticLock false, txnPessimisticLock: "
                    + txnPessimisticLock.toString());
            }
        } catch (RegionSplitException e) {
            log.error(e.getMessage(), e);
            CommonId regionId = singleKeySplitRegionId(tableId, txnId, key);
            StoreInstance store = Services.KV_STORE.getInstance(tableId, regionId);
            boolean result = store.txnPessimisticLock(txnPessimisticLock, timeOut);
            if (!result) {
                throw new RuntimeException(txnId + " " + partId + ",txnPessimisticLock false, txnPessimisticLock: "
                    + txnPessimisticLock.toString());
            }
        }
        return txnPessimisticLock;
    }

    public static boolean pessimisticPrimaryLockRollBack(CommonId txnId, CommonId tableId,
                                                         CommonId partId, int isolationLevel,
                                                         long startTs, long forUpdateTs, byte[] primaryKey) {
        // primaryKeyLock rollback
        TxnPessimisticRollBack pessimisticRollBack = TxnPessimisticRollBack.builder()
            .isolationLevel(IsolationLevel.of(isolationLevel))
            .startTs(startTs)
            .forUpdateTs(forUpdateTs)
            .keys(Collections.singletonList(primaryKey))
            .build();
        try {
            StoreInstance store = Services.KV_STORE.getInstance(tableId, partId);
            return store.txnPessimisticLockRollback(pessimisticRollBack);
        } catch (RegionSplitException e) {
            log.error(e.getMessage(), e);
            // 2、regin split
            CommonId regionId = TransactionUtil.singleKeySplitRegionId(tableId, txnId, primaryKey);
            StoreInstance store = Services.KV_STORE.getInstance(tableId, regionId);
            return store.txnPessimisticLockRollback(pessimisticRollBack);
        }
    }

    private static String joinPrimaryKey(Object[] keyValues, TupleMapping mapping) {

        if (keyValues == null || mapping == null) {
            throw new IllegalArgumentException("Parameters cannot be null");
        }
        StringJoiner joiner = new StringJoiner("-");
        try {
            mapping.stream().forEach(index -> {
                joiner.add(keyValues[index].toString());
            });
        } catch (Exception e) {
            throw new RuntimeException("Error joining primary key", e);
        }
        return Optional.ofNullable(joiner.toString())
            .map(str -> "'" + str + "'")
            .orElse("");
    }

    public static String duplicateEntryKey(CommonId tableId, byte[] key) {
        Table table = MetaService.root().getTable(tableId);
        KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(table.tupleType(), table.keyMapping());
        TupleMapping keyMapping = table.keyMapping();
        return joinPrimaryKey(codec.decodeKeyPrefix(key), keyMapping);
    }


    public static void resolvePessimisticLock(int isolationLevel, CommonId txnId, CommonId tableId,
                                              CommonId partId, byte[] deadLockKeyBytes, byte[] primaryKey,
                                              long startTs, long forUpdateTs,
                                              boolean hasException, Throwable e) {
        StoreInstance store;
        try {
            // primaryKeyLock rollback
            TransactionUtil.pessimisticPrimaryLockRollBack(
                txnId,
                tableId,
                partId,
                isolationLevel,
                startTs,
                forUpdateTs,
                primaryKey
            );
        } catch (Throwable throwable) {
            log.error(e.getMessage(), e);
            store = Services.LOCAL_STORE.getInstance(tableId, partId);
            // delete deadLockKey
            store.delete(deadLockKeyBytes);
        }
        if (hasException){
            throw new RuntimeException(e.getMessage());
        }
    }

}
