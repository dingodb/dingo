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
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Utils;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
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
import io.dingodb.sdk.service.entity.coordinator.ScanRegionsRequest;
import io.dingodb.sdk.service.entity.coordinator.ScanRegionsResponse;
import io.dingodb.sdk.service.entity.store.TxnBatchRollbackResponse;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.Mutation;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.data.commit.TxnCommit;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;
import io.dingodb.store.api.transaction.data.rollback.TxnBatchRollBack;
import io.dingodb.store.api.transaction.exception.CommitTsExpiredException;
import io.dingodb.store.api.transaction.exception.DuplicateEntryException;
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import io.dingodb.store.api.transaction.exception.WriteConflictException;
import io.dingodb.store.proxy.Configuration;
import io.dingodb.store.proxy.meta.ScanRegionWithPartId;
import io.dingodb.store.proxy.service.CodecService;
import io.dingodb.store.proxy.service.TransactionStoreInstance;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import static io.dingodb.store.proxy.mapper.Mapper.MAPPER;

@Slf4j
public class MetaStoreKvTxn {
    boolean ddl;
    CommonId metaId = null;
    CommonId partId;
    static final byte namespace = (byte) 't';
    private static MetaStoreKvTxn instance;
    private static MetaStoreKvTxn instanceDdl;
    Set<Location> coordinators = Services.parse(Configuration.coordinators());
    int isolationLevel = 2;
    // putAbsent
    StoreService storeService;

    long statementTimeout = 50000;

    public static void init() {
        instance = new MetaStoreKvTxn(false);
        instanceDdl = new MetaStoreKvTxn(true);
    }

    public static synchronized MetaStoreKvTxn getInstance() {
        if (instance == null) {
            init();
        }
        return instance;
    }

    public static synchronized MetaStoreKvTxn getDdlInstance() {
        if (instanceDdl == null) {
            init();
        }
        return instanceDdl;
    }

    private MetaStoreKvTxn(boolean ddl) {
        if (!ddl) {
            partId = new CommonId(CommonId.CommonType.PARTITION, 0, 0);
            long metaPartId = checkMetaRegion();
            metaId = new CommonId(CommonId.CommonType.META, 0, metaPartId);
            storeService = Services.storeRegionService(coordinators, metaPartId, 60);
        } else {
            partId = new CommonId(CommonId.CommonType.PARTITION, 0, 3);
            long metaPartId = checkMetaRegion();
            metaId = new CommonId(CommonId.CommonType.META, 0, metaPartId);
            storeService = Services.storeRegionService(coordinators, metaPartId, 60);
        }
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
        String regionName = "meta";
        if (ddl) {
            regionName = "ddl";
        }
        CreateRegionRequest createRegionRequest = CreateRegionRequest.builder()
            .regionName(regionName)
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

    public long getScanRegionId(byte[] start, byte[] end) {
        long startTs = io.dingodb.tso.TsoService.getDefault().tso();
        ScanRegionsRequest request = ScanRegionsRequest.builder()
            .key(start)
            .rangeEnd(end)
            .limit(0)
            .build();
        CoordinatorService coordinatorService = Services.coordinatorService(coordinators);
        ScanRegionsResponse response = coordinatorService.scanRegions(startTs, request);
        if (response.getRegions() == null || response.getRegions().isEmpty()) {
            return 0;
        }
        return response.getRegions().get(0).getRegionId();
    }

    public byte[] mGet(byte[] key, long startTs) {
        key = getMetaDataKey(key);

        List<byte[]> keys = Collections.singletonList(key);
        TransactionStoreInstance storeInstance = new TransactionStoreInstance(storeService, null, partId);
        List<KeyValue> keyValueList = storeInstance.getKeyValues(startTs, keys, statementTimeout);
        if (keyValueList.isEmpty()) {
            return null;
        } else {
            return keyValueList.get(0).getValue();
        }
    }

    public byte[] mGetImmediately(byte[] key, long startTs) {
        key = getMetaDataKey(key);

        List<byte[]> keys = Collections.singletonList(key);
        TransactionStoreInstance storeInstance = new TransactionStoreInstance(storeService, null, partId);
        try {
            List<KeyValue> keyValueList = storeInstance.getKeyValues(startTs, keys, 1000);
            if (keyValueList.isEmpty()) {
                return null;
            } else {
                return keyValueList.get(0).getValue();
            }
        } catch (Exception e) {
            return null;
        }
    }

    public List<byte[]> mRange(byte[] start, byte[] end, long startTs) {
        start = getMetaDataKey(start);
        end = getMetaDataKey(end);
        TransactionStoreInstance storeInstance = new TransactionStoreInstance(storeService, null, partId);
        StoreInstance.Range range = new StoreInstance.Range(start, end, true, false);
        Iterator<KeyValue> scanIterator = storeInstance.getScanIterator(startTs, range, statementTimeout, null);
        List<byte[]> values = new ArrayList<>();
        while (scanIterator.hasNext()) {
            values.add(scanIterator.next().getValue());
        }
        return values;
    }

    public void mDel(byte[] key, long startTs) {
        key = getMetaDataKey(key);
        commit(key, null, Op.DELETE.getCode(), startTs);
    }

    public void mInsert(byte[] key, byte[] value, long startTs) {
        key = getMetaDataKey(key);
        commit(key, value, Op.PUTIFABSENT.getCode(), startTs);
    }

    public void put(byte[] key, byte[] value, long startTs) {
        key = getMetaDataKey(key);
        commit(key, value, Op.PUT.getCode(), startTs);
    }

    public void commit(byte[] key, byte[] value, int opCode, long startTs) {
        CommonId txnId = getTxnId(startTs);
        try {
            Mutation mutation = new Mutation(
                io.dingodb.store.api.transaction.data.Op.forNumber(opCode), key, value, 0, null, null
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

    private byte[] getMetaRegionEndKey() {
        byte[] bytes = new byte[9];
        BufImpl buf = new BufImpl(bytes);
        // skip namespace
        buf.skip(1);
        // reset id
        long part = partId.seq;
        buf.writeLong(part + 1);
        bytes[0] = namespace;
        return bytes;
    }

    private byte[] getMetaRegionKey() {
        byte[] key = new byte[9];
        CodecService.INSTANCE.setId(key, partId.seq);
        key[0] = namespace;
        return key;
    }

    private void preWritePrimaryKey(
        Mutation mutation,
        long startTs
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
            .tryOnePc(false)
            .maxCommitTs(0L)
            .build();
        try {
            TransactionStoreInstance storeInstance = new TransactionStoreInstance(storeService, null, partId);
            storeInstance.txnPreWriteRealKey(txnPreWrite, statementTimeout);
        } catch (RegionSplitException e) {
            LogUtils.error(log, e.getMessage(), e);

            boolean prewriteResult = false;
            int i = 0;
            while (!prewriteResult) {
                i++;
                try {
                    CommonId regionIdNew = refreshRegionId(getMetaRegionKey(), getMetaRegionEndKey(), primaryKey);
                    StoreService serviceNew = Services.storeRegionService(coordinators, regionIdNew.seq, 60);
                    TransactionStoreInstance storeInstanceNew = new TransactionStoreInstance(serviceNew, null, partId);
                    storeInstanceNew.txnPreWrite(txnPreWrite, statementTimeout);
                    prewriteResult = true;
                } catch (RegionSplitException e1) {
                    Utils.sleep(100);
                    LogUtils.error(log, "preWrite primary region split, retry count:" + i);
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
        TransactionStoreInstance storeInstance = new TransactionStoreInstance(storeService, null, partId);
        try {
            //TransactionStoreInstance storeInstance = new TransactionStoreInstance(storeService, null, partId);
            return storeInstance.txnCommitRealKey(commitRequest);
        } catch (RuntimeException e) {
            LogUtils.error(log, e.getMessage(), e);
            if (e instanceof CommitTsExpiredException) {
                int retry = 3;
                boolean retryRes = false;
                while (retry -- > 0) {
                    Utils.sleep(1000);
                    try {
                        commitRequest.setCommitTs(TsoService.getDefault().tso());
                        storeInstance.txnCommitRealKey(commitRequest);
                        retryRes = true;
                        break;
                    } catch (CommitTsExpiredException ignored) {
                        continue;
                    }
                }
                if (!retryRes) {
                    throw e;
                }
            } else {
                // 2、regin split
                boolean commitResult = false;
                int i = 0;
                while (!commitResult) {
                    i++;
                    try {
                        CommonId regionIdNew = refreshRegionId(getMetaRegionKey(), getMetaRegionEndKey(), primaryKey);
                        StoreService serviceNew = Services.storeRegionService(coordinators, regionIdNew.seq, 60);
                        TransactionStoreInstance storeInstanceNew = new TransactionStoreInstance(serviceNew, null, partId);
                        storeInstanceNew.txnCommit(commitRequest);
                        commitResult = true;
                    } catch (RegionSplitException e1) {
                        Utils.sleep(100);
                        LogUtils.error(log, "commit primary region split, retry count:" + i);
                    }
                }
            }
            return true;
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            return false;
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
            CommonId regionIdNew = refreshRegionId(getMetaRegionKey(), getMetaRegionEndKey(), keys.get(0));
            StoreService serviceNew = Services.storeRegionService(coordinators, regionIdNew.seq, 60);
            TransactionStoreInstance storeInstanceNew
               = new TransactionStoreInstance(serviceNew, null, partId);
            if (!storeInstanceNew.txnBatchRollback(rollBackRequest)) {
                throw new RuntimeException("txn rollback fail");
            }
        }
    }

    public static CommonId getTxnId(long startTs) {
        return new CommonId(CommonId.CommonType.TRANSACTION,
            0, startTs);
    }

    public static CommonId refreshRegionId(byte[] startKey, byte[] endKey, byte[] key) {
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution
            = loadDistribution(startKey, endKey);
        String strategy = DingoPartitionServiceProvider.RANGE_FUNC_NAME;

        return PartitionService.getService(
                strategy)
            .calcPartId(key, rangeDistribution);
    }

    private static NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> loadDistribution(
            byte[] startKey, byte[] endKey
    ) {
        InfoSchemaService infoSchemaService = io.dingodb.store.service.InfoSchemaService.ROOT;
        List<Object> regionList = infoSchemaService
            .scanRegions(startKey, endKey);
        List<ScanRegionWithPartId> rangeDistributionList = new ArrayList<>();
        regionList
            .forEach(object -> {
                ScanRegionInfo scanRegionInfo = (ScanRegionInfo) object;
                rangeDistributionList.add(
                    new ScanRegionWithPartId(scanRegionInfo, 0)
                );
            });
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> result = new TreeMap<>();

        rangeDistributionList.forEach(scanRegionWithPartId -> {
            ScanRegionInfo scanRegionInfo = scanRegionWithPartId.getScanRegionInfo();
            byte[] startInner = scanRegionInfo.getRange().getStartKey();
            byte[] endInner = scanRegionInfo.getRange().getEndKey();
            RangeDistribution distribution = RangeDistribution.builder()
                .id(new CommonId(CommonId.CommonType.DISTRIBUTION, scanRegionWithPartId.getPartId(), scanRegionInfo.getRegionId()))
                .startKey(startInner)
                .endKey(endInner)
                .build();
            result.put(new ByteArrayUtils.ComparableByteArray(distribution.getStartKey(), 1), distribution);
        });
        return result;
    }

}
