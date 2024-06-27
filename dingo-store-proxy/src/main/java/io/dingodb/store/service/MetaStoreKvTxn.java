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

import io.dingodb.common.CommonId;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.store.KeyValue;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.sdk.common.serial.BufImpl;
import io.dingodb.sdk.service.CoordinatorService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.StoreService;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.common.Range;
import io.dingodb.sdk.service.entity.common.RawEngine;
import io.dingodb.sdk.service.entity.common.RegionType;
import io.dingodb.sdk.service.entity.common.StorageEngine;
import io.dingodb.sdk.service.entity.coordinator.CreateRegionRequest;
import io.dingodb.sdk.service.entity.coordinator.CreateRegionResponse;
import io.dingodb.sdk.service.entity.coordinator.ScanRegionInfo;
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
import io.dingodb.store.proxy.service.CodecService;
import io.dingodb.store.proxy.service.TransactionStoreInstance;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static io.dingodb.store.proxy.mapper.Mapper.MAPPER;

@Slf4j
public class MetaStoreKvTxn {
    CommonId metaId = null;
    static final byte namespace = (byte) 't';
    private static MetaStoreKvTxn instance;
    Set<Location> coordinators = Services.parse(Configuration.coordinators());
    int isolationLevel = 2;
    // putAbsent
    StoreService storeService;

    long statementTimeout = 50000;

    public static void init() {
        instance = new MetaStoreKvTxn();
    }

    public static synchronized MetaStoreKvTxn getInstance() {
        if (instance == null) {
            init();
        }
        return instance;
    }

    private MetaStoreKvTxn() {
        long metaRegionId = checkMetaRegion();
        metaId = new CommonId(CommonId.CommonType.META, 0, metaRegionId);
        storeService = Services.storeRegionService(coordinators, metaRegionId, 60);
    }

    public long checkMetaRegion() {
        CoordinatorService coordinatorService = Services.coordinatorService(coordinators);
        long startTs = TsoService.getDefault().tso();
        byte[] startKey = getMetaRegionKey();
        byte[] endKey = getMetaRegionEndKey();

        long regionId = getScanRegionId(startKey, endKey);
        if (regionId > 0) {
            return regionId;
        }
        Range range = Range.builder().startKey(startKey).endKey(endKey).build();
        CreateRegionRequest createRegionRequest = CreateRegionRequest.builder()
            .regionName("meta")
            .range(range)
            .replicaNum(3)
            .rawEngine(RawEngine.RAW_ENG_ROCKSDB)
            .storeEngine(StorageEngine.STORE_ENG_RAFT_STORE)
            .regionType(RegionType.STORE_REGION)
            .tenantId(0)
            .build();
        try {
            CreateRegionResponse response = coordinatorService.createRegion(startTs, createRegionRequest);
            return response.getRegionId();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return 0;
    }

    public static long getScanRegionId(byte[] start, byte[] end) {
        List<Object> regionList = InfoSchemaService.root().scanRegions(start, end);
        if (regionList == null || regionList.isEmpty()) {
            return 0;
        } else {
            ScanRegionInfo scanRegionInfo = (ScanRegionInfo) regionList.get(0);
            return scanRegionInfo.getRegionId();
        }
    }


    public byte[] mGet(byte[] key) {
        long startTs = TsoService.getDefault().tso();
        key = getMetaDataKey(key);

        List<byte[]> keys = Collections.singletonList(key);
        TransactionStoreInstance storeInstance = new TransactionStoreInstance(storeService, null, metaId);
        List<KeyValue> keyValueList = storeInstance.getKeyValues(startTs, keys, statementTimeout);
        if (keyValueList.isEmpty()) {
            return null;
        } else {
            return keyValueList.get(0).getValue();
        }
    }

    public List<byte[]> mRange(byte[] start, byte[] end) {
        start = getMetaDataKey(start);
        end = getMetaDataKey(end);
        long startTs = TsoService.getDefault().tso();
        CommonId EMPTY = new CommonId(CommonId.CommonType.SCHEMA, 0, 0);
        TransactionStoreInstance storeInstance = new TransactionStoreInstance(storeService, null, EMPTY);
        StoreInstance.Range range = new StoreInstance.Range(start, end, true, false);
        Iterator<KeyValue> scanIterator = storeInstance.getScanIterator(startTs, range, statementTimeout, null);
        List<byte[]> values = new ArrayList<>();
        while (scanIterator.hasNext()) {
            values.add(scanIterator.next().getValue());
        }
        return values;
    }

    public void mDel(byte[] key) {
        key = getMetaDataKey(key);
        commit(key, null, Op.DELETE.getCode());
    }

    public void mInsert(byte[] key, byte[] value) {
        key = getMetaDataKey(key);
        commit(key, value, Op.PUTIFABSENT.getCode());
    }

    public void mUpdate(byte[] key, byte[] value) {
        key = getMetaDataKey(key);
        commit(key, value, Op.PUT.getCode());
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

    private byte[] getMetaDataKey(byte[] key) {
        byte[] bytes = new byte[9 + key.length];
        byte[] regionKey = getMetaRegionKey();
        System.arraycopy(regionKey, 0, bytes, 0, regionKey.length);
        System.arraycopy(key, 0, bytes, 9, key.length);
        return bytes;
    }

    private static byte[] getMetaRegionEndKey() {
        byte[] bytes = new byte[9];
        BufImpl buf = new BufImpl(bytes);
        // skip namespace
        buf.skip(1);
        // reset id
        buf.writeLong(1);
        bytes[0] = namespace;
        return bytes;
    }

    private byte[] getMetaRegionKey() {
        byte[] key = new byte[9];
        CodecService.INSTANCE.setId(key, 0);
        key[0] = namespace;
        return key;
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
            TransactionStoreInstance storeInstance = new TransactionStoreInstance(storeService, null, metaId);
            storeInstance.txnPreWriteRealKey(txnPreWrite, statementTimeout);
        } catch (RegionSplitException e) {
            LogUtils.error(log, e.getMessage(), e);

            boolean prewriteResult = false;
            int i = 0;
            while (!prewriteResult) {
                i++;
                try {
                    // todo
                    prewriteResult = true;
                } catch (RegionSplitException e1) {
                    sleep100();
                    LogUtils.error(log, "prewrite primary region split, retry count:" + i);
                }
            }
        }
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
            TransactionStoreInstance storeInstance = new TransactionStoreInstance(storeService, null, metaId);
            return storeInstance.txnCommitRealKey(commitRequest);
        } catch (RuntimeException e) {
            LogUtils.error(log, e.getMessage(), e);
            // 2、regin split
            boolean commitResult = false;
            int i = 0;
            while (!commitResult) {
                i++;
                try {
                    // todo
                    // get new region
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
            // todo
        }
    }

    public static CommonId getTxnId() {
        return new CommonId(CommonId.CommonType.TRANSACTION,
            0, TsoService.getDefault().tso());
    }

}
