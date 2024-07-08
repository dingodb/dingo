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

package io.dingodb.store.service;

import io.dingodb.codec.CodecService;
import io.dingodb.common.CommonId;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.MetaService;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.StoreService;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
import io.dingodb.sdk.service.entity.store.TxnBatchRollbackResponse;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.Mutation;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.data.commit.TxnCommit;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;
import io.dingodb.store.api.transaction.data.rollback.TxnBatchRollBack;
import io.dingodb.store.api.transaction.exception.DuplicateEntryException;
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import io.dingodb.store.api.transaction.exception.WriteConflictException;
import io.dingodb.store.proxy.Configuration;
import io.dingodb.store.proxy.service.TransactionStoreInstance;
import io.dingodb.tso.TsoService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

import static io.dingodb.store.proxy.mapper.Mapper.MAPPER;

@Slf4j
public class StoreKvTxn implements io.dingodb.store.api.transaction.StoreKvTxn {
    CommonId tableId;
    CommonId partId;
    @Getter
    CommonId regionId;
    StoreService storeService;
    Set<Location> coordinators = Services.parse(Configuration.coordinators());
    long statementTimeout = 50000;
    int isolationLevel = 2;

    public StoreKvTxn(CommonId tableId, CommonId regionId) {
        this.tableId = tableId;
        this.partId = new CommonId(CommonId.CommonType.PARTITION, tableId.seq, regionId.domain);
        this.regionId = regionId;
        storeService = Services.storeRegionService(coordinators, regionId.seq, 60);
    }

    public void del(byte[] key) {
        commit(key, null, Op.DELETE.getCode());
    }

    public void insert(byte[] startKey, byte[] endKey) {
        commit(startKey, endKey, Op.PUTIFABSENT.getCode());
    }

    public void update(byte[] key, byte[] value) {
        commit(key, value, Op.PUT.getCode());
    }

    public KeyValue get(byte[] key) {
        long startTs = TsoService.getDefault().tso();;
        List<byte[]> keys = Collections.singletonList(key);
        TransactionStoreInstance storeInstance = new TransactionStoreInstance(storeService, null, partId);
        List<KeyValue> keyValueList = storeInstance.txnGet(startTs, keys, statementTimeout);
        if (keyValueList.isEmpty()) {
            return null;
        } else {
            return keyValueList.get(0);
        }
    }

    public Iterator<KeyValue> range(byte[] start, byte[] end) {
        boolean withEnd = ByteArrayUtils.compare(end, start) <= 0;
        long startTs = TsoService.getDefault().tso();
        TransactionStoreInstance storeInstance = new TransactionStoreInstance(storeService, null, partId);
        StoreInstance.Range range = new StoreInstance.Range(start, end, true, withEnd);
        return storeInstance.txnScan(startTs, range, statementTimeout, null);
    }

    public void commit(byte[] key, byte[] value, int opCode) {
        CommonId txnId = getTxnId();
        long startTs = TsoService.getDefault().tso();
        try {
            Mutation mutation = new Mutation(
                io.dingodb.store.api.transaction.data.Op.forNumber(opCode), key, value, 0, null
            );
            preWritePrimaryKey(mutation, startTs);
        } catch (WriteConflictException e) {
            LogUtils.error(log, e.getMessage(), e);
            // rollback or retry
            throw e;
        } catch (DuplicateEntryException e) {
            LogUtils.error(log, e.getMessage(), e);
            // rollback
            List<byte[]> keys = new ArrayList<>();
            keys.add(key);
            txnRollBack(isolationLevel, startTs, keys, txnId);
            throw e;
        }
        long commitTs = TsoService.getDefault().tso();
        boolean result = commitPrimaryData(isolationLevel, startTs, commitTs, key);
        if (!result) {
            throw new RuntimeException("txnCommitPrimaryKey false,commit_ts:" + commitTs);
        }
    }

    private void preWritePrimaryKey(
        Mutation mutation,
        long startTs
    ) {
        byte[] primaryKey = mutation.getKey();
        // 2、call sdk preWritePrimaryKey
        long lockTtl = TsoService.getDefault().timestamp() + 60;

        TxnPreWrite txnPreWrite = TxnPreWrite.builder()
            .isolationLevel(IsolationLevel.of(
                IsolationLevel.ReadCommitted.getCode()
            ))
            .mutations(Collections.singletonList(mutation))
            .primaryLock(primaryKey)
            .startTs(startTs)
            .lockTtl(lockTtl)
            .txnSize(1L)
            .tryOnePc(false)
            .maxCommitTs(0L)
            .build();
        try {
            TransactionStoreInstance storeInstance = new TransactionStoreInstance(storeService, null, partId);
            storeInstance.txnPreWrite(txnPreWrite, statementTimeout);
        } catch (RegionSplitException e) {
            LogUtils.error(log, e.getMessage(), e);

            boolean prewriteResult = false;
            int i = 0;
            while (!prewriteResult) {
                i++;
                try {
                    CommonId regionIdNew = refreshRegionId(tableId, primaryKey);
                    StoreService serviceNew = Services.storeRegionService(coordinators, regionIdNew.seq, 60);
                    TransactionStoreInstance storeInstanceNew = new TransactionStoreInstance(serviceNew, null, partId);
                    storeInstanceNew.txnPreWrite(txnPreWrite, statementTimeout);
                    prewriteResult = true;
                } catch (RegionSplitException e1) {
                    sleep100();
                    LogUtils.error(log, "preWrite primary region split, retry count:" + i);
                }
            }
        }
    }

    public static CommonId refreshRegionId(CommonId tableId, byte[] key) {
        MetaService root = MetaService.root();
        InfoSchemaService infoSchemaService = InfoSchemaService.ROOT;
        TableDefinitionWithId tableDefinitionWithId = (TableDefinitionWithId) infoSchemaService
            .getTable(0, tableId.domain, tableId.seq);
        int strategyNumber = tableDefinitionWithId.getTableDefinition().getTablePartition().getStrategy().number();
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution
            = root.getRangeDistribution(tableId);
        if (strategyNumber == 0) {
            CodecService.getDefault().setId(key, 0L);
        }
        String strategy = DingoPartitionServiceProvider.RANGE_FUNC_NAME;
        if (strategyNumber != 0) {
            strategy = DingoPartitionServiceProvider.HASH_FUNC_NAME;
        }

        return PartitionService.getService(
                strategy)
            .calcPartId(key, rangeDistribution);
    }

    public boolean commitPrimaryData(
        int isolationLevel,
        long startTs,
        long commitTs,
        byte[] primaryKey
    ) {
        // 1、call sdk commitPrimaryKey
        TxnCommit commitRequest = TxnCommit.builder()
            .isolationLevel(IsolationLevel.of(isolationLevel))
            .startTs(startTs)
            .commitTs(commitTs)
            .keys(Collections.singletonList(primaryKey))
            .build();
        try {
            TransactionStoreInstance storeInstance = new TransactionStoreInstance(storeService, null, partId);
            return storeInstance.txnCommit(commitRequest);
        } catch (RuntimeException e) {
            LogUtils.error(log, e.getMessage(), e);
            // 2、regin split
            boolean commitResult = false;
            int i = 0;
            while (!commitResult) {
                i++;
                try {
                    CommonId regionIdNew = refreshRegionId(tableId, primaryKey);
                    StoreService serviceNew = Services.storeRegionService(coordinators, regionIdNew.seq, 60);
                    TransactionStoreInstance storeInstanceNew = new TransactionStoreInstance(serviceNew, null, partId);
                    storeInstanceNew.txnCommit(commitRequest);
                    commitResult = true;
                } catch (RegionSplitException e1) {
                    sleep100();
                    LogUtils.error(log, "commit primary region split, retry count:" + i);
                }
            }
            return true;
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            return false;
        }
    }

    private static void sleep100() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    private synchronized void txnRollBack(int isolationLevel, long startTs, List<byte[]> keys, CommonId txnId) {
        // 1、Async call sdk TxnRollBack
        TxnBatchRollBack rollBackRequest = TxnBatchRollBack.builder()
            .isolationLevel(IsolationLevel.of(isolationLevel))
            .startTs(startTs)
            .keys(keys)
            .build();
        try {
            TxnBatchRollbackResponse response
                = storeService.txnBatchRollback(startTs, MAPPER.rollbackTo(rollBackRequest));
            if (response.getTxnResult() != null) {
                LogUtils.error(log, "txnBatchRollback txnResult:{}", response.getTxnResult().toString());
                throw new RuntimeException(txnId + ",txnBatchRollback false");
            }
        } catch (RuntimeException e) {
            LogUtils.error(log, e.getMessage(), e);
            // 2、regin split
            CommonId regionIdNew = refreshRegionId(tableId, keys.get(0));
            StoreService serviceNew = Services.storeRegionService(coordinators, regionIdNew.seq, 60);
            TransactionStoreInstance storeInstanceNew = new TransactionStoreInstance(serviceNew, null, partId);
            if (!storeInstanceNew.txnBatchRollback(rollBackRequest)) {
                throw new RuntimeException("txn rollback fail");
            }
        }
    }

    public static CommonId getTxnId() {
        return new CommonId(CommonId.CommonType.TRANSACTION,
            0, TsoService.getDefault().tso());
    }

}
