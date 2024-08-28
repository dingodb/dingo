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
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Utils;
import io.dingodb.meta.MetaService;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.StoreService;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;
import io.dingodb.sdk.service.entity.store.TxnBatchRollbackResponse;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.Mutation;
import io.dingodb.store.api.transaction.data.commit.TxnCommit;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;
import io.dingodb.store.api.transaction.data.rollback.TxnBatchRollBack;
import io.dingodb.store.api.transaction.exception.CommitTsExpiredException;
import io.dingodb.store.api.transaction.exception.DuplicateEntryException;
import io.dingodb.store.api.transaction.exception.OnePcDegenerateTwoPcException;
import io.dingodb.store.api.transaction.exception.OnePcNeedTwoPcCommit;
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import io.dingodb.store.api.transaction.exception.WriteConflictException;
import io.dingodb.store.proxy.Configuration;
import io.dingodb.store.proxy.service.TransactionStoreInstance;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

import static io.dingodb.store.proxy.mapper.Mapper.MAPPER;

@Slf4j
public abstract class CommitBase {
    StoreService storeService;
    int isolationLevel = 2;
    CommonId partId;

    static Set<Location> coordinators = Services.parse(Configuration.coordinators());
    long statementTimeout = 50000;

    public CommitBase(StoreService storeService, CommonId partId) {
        this.storeService = storeService;
        this.partId = partId;
    }

    public abstract TransactionStoreInstance refreshRegion(byte[] key);

    public void commit(byte[] key, byte[] value, int opCode, long startTs) {
        CommonId txnId = getTxnId(startTs);
        boolean need2PcPreWrite = false;
        boolean need2PcCommit = false;

        try {
            Mutation mutation = new Mutation(
                io.dingodb.store.api.transaction.data.Op.forNumber(opCode), key, value, 0, null, null
            );

            if(ScopeVariables.transaction1Pc()) {
                try {
                    preWritePrimaryKey(mutation, startTs, true);
                    return;
                } catch (OnePcNeedTwoPcCommit e) {
                    LogUtils.info(log, e.getMessage());
                    need2PcCommit = true;
                } catch (OnePcDegenerateTwoPcException e) {
                    LogUtils.info(log, e.getMessage());
                    need2PcPreWrite = true;
                    need2PcCommit = true;
                }
            } else {
                need2PcPreWrite = true;
                need2PcCommit = true;
            }

            if(need2PcPreWrite) {
                preWritePrimaryKey(mutation, startTs, false);
            }
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

        if(need2PcCommit) {
            long commitTs = TsoService.getDefault().tso();
            boolean result = commitPrimaryData(isolationLevel, startTs, commitTs, key);
            if (!result) {
                throw new RuntimeException("txnCommitPrimaryKey false,commit_ts:" + commitTs);
            }
        }
    }

    private void preWritePrimaryKey(
        Mutation mutation,
        long startTs,
        boolean enableOnePc
    ) {
        byte[] primaryKey = mutation.getKey();
        // 2、call sdk preWritePrimaryKey
        long lockTtl = TsoService.getDefault().timestamp() + 60000;

        TxnPreWrite txnPreWrite = TxnPreWrite.builder()
            .isolationLevel(IsolationLevel.of(
                IsolationLevel.ReadCommitted.getCode()
            ))
            .mutations(Collections.singletonList(mutation))
            .primaryLock(primaryKey)
            .startTs(startTs)
            .lockTtl(lockTtl)
            .txnSize(1L)
            .tryOnePc(enableOnePc)
            .maxCommitTs(0L)
            .build();
        try {
            TransactionStoreInstance storeInstance = new TransactionStoreInstance(storeService, null, partId);
            storeInstance.txnPreWrite(txnPreWrite, statementTimeout);
        } catch (RegionSplitException e) {
            LogUtils.error(log, e.getMessage(), e);

            if(enableOnePc) {
                throw new OnePcDegenerateTwoPcException("1PC degenerate to 2PC, startTs:" + startTs);
            }else {
                boolean prewriteResult = false;
                int i = 0;
                while (!prewriteResult) {
                    i++;
                    try {
                        TransactionStoreInstance storeInstanceNew = refreshRegion(primaryKey);
                        storeInstanceNew.txnPreWrite(txnPreWrite, statementTimeout);
                        prewriteResult = true;
                    } catch (RegionSplitException e1) {
                        Utils.sleep(100);
                        LogUtils.error(log, "preWrite primary region split, retry count:" + i);
                    }
                }
            }
        }
    }

    public static CommonId refreshRegionId(CommonId tableId, byte[] key) {
        MetaService root = MetaService.root();
        InfoSchemaService infoSchemaService = InfoSchemaService.ROOT;
        TableDefinitionWithId tableDefinitionWithId = (TableDefinitionWithId) infoSchemaService
            .getTable(tableId.domain, tableId.seq);
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
        try {
            long start = System.currentTimeMillis();
            while (true) {
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
                } catch (RegionSplitException e) {
                    LogUtils.error(log, e.getMessage(), e);
                    // 2、regin split
                    boolean commitResult = false;
                    int i = 0;
                    while (!commitResult) {
                        i++;
                        try {
                            TransactionStoreInstance storeInstanceNew = refreshRegion(primaryKey);
                            storeInstanceNew.txnCommit(commitRequest);
                            commitResult = true;
                        } catch (RegionSplitException e1) {
                            Utils.sleep(100);
                            LogUtils.error(log, "commit primary region split, retry count:" + i);
                        }
                    }
                    return true;
                } catch (CommitTsExpiredException e) {
                    LogUtils.error(log, e.getMessage(), e);
                    commitTs = TsoService.getDefault().tso();
                }
                long elapsed = System.currentTimeMillis() - start;
                if (elapsed > statementTimeout) {
                    return false;
                }
            }
        } catch (Throwable throwable) {
            LogUtils.error(log, throwable.getMessage(), throwable);
        }
        return false;
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
            TransactionStoreInstance storeInstanceNew = refreshRegion(keys.get(0));
            if (!storeInstanceNew.txnBatchRollback(rollBackRequest)) {
                throw new RuntimeException("txn rollback fail");
            }
        }
    }

    public static CommonId getTxnId(long startTs) {
        return new CommonId(CommonId.CommonType.TRANSACTION,
            0, startTs);
    }

}
