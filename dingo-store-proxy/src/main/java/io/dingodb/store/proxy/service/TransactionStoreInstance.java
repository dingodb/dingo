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

package io.dingodb.store.proxy.service;

import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.CoprocessorV2;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.log.MdcUtils;
import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.profile.Profile;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.meta.entity.Table;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.DingoClientException.RequestErrorException;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.service.DocumentService;
import io.dingodb.sdk.service.IndexService;
import io.dingodb.sdk.service.StoreService;
import io.dingodb.sdk.service.entity.common.KeyValue;
import io.dingodb.sdk.service.entity.store.Action;
import io.dingodb.sdk.service.entity.store.AlreadyExist;
import io.dingodb.sdk.service.entity.store.LockInfo;
import io.dingodb.sdk.service.entity.store.Mutation;
import io.dingodb.sdk.service.entity.store.Op;
import io.dingodb.sdk.service.entity.store.TxnBatchGetRequest;
import io.dingodb.sdk.service.entity.store.TxnBatchGetResponse;
import io.dingodb.sdk.service.entity.store.TxnBatchRollbackResponse;
import io.dingodb.sdk.service.entity.store.TxnCheckTxnStatusResponse;
import io.dingodb.sdk.service.entity.store.TxnCommitResponse;
import io.dingodb.sdk.service.entity.store.TxnHeartBeatRequest;
import io.dingodb.sdk.service.entity.store.TxnPessimisticLockResponse;
import io.dingodb.sdk.service.entity.store.TxnPessimisticRollbackResponse;
import io.dingodb.sdk.service.entity.store.TxnPrewriteRequest;
import io.dingodb.sdk.service.entity.store.TxnPrewriteResponse;
import io.dingodb.sdk.service.entity.store.TxnResolveLockResponse;
import io.dingodb.sdk.service.entity.store.TxnResultInfo;
import io.dingodb.sdk.service.entity.store.TxnScanRequest;
import io.dingodb.sdk.service.entity.store.TxnScanResponse;
import io.dingodb.sdk.service.entity.store.WriteConflict;
import io.dingodb.sdk.service.entity.stream.StreamRequestMeta;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.ProfileScanIterator;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.TxnVariables;
import io.dingodb.store.api.transaction.data.checkstatus.AsyncResolveData;
import io.dingodb.store.api.transaction.data.checkstatus.TxnCheckStatus;
import io.dingodb.store.api.transaction.data.commit.TxnCommit;
import io.dingodb.store.api.transaction.data.pessimisticlock.TxnPessimisticLock;
import io.dingodb.store.api.transaction.data.prewrite.LockExtraDataList;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;
import io.dingodb.store.api.transaction.data.resolvelock.ResolveLockStatus;
import io.dingodb.store.api.transaction.data.resolvelock.TxnResolveLock;
import io.dingodb.store.api.transaction.data.rollback.TxnBatchRollBack;
import io.dingodb.store.api.transaction.data.rollback.TxnPessimisticRollBack;
import io.dingodb.store.api.transaction.exception.CommitTsExpiredException;
import io.dingodb.store.api.transaction.exception.DuplicateEntryException;
import io.dingodb.store.api.transaction.exception.LockWaitException;
import io.dingodb.store.api.transaction.exception.NonAsyncCommitLockException;
import io.dingodb.store.api.transaction.exception.OnePcMaxSizeExceedException;
import io.dingodb.store.api.transaction.exception.OnePcNeedTwoPcCommit;
import io.dingodb.store.api.transaction.exception.PrimaryMismatchException;
import io.dingodb.store.api.transaction.exception.WriteConflictException;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.dingodb.store.proxy.mapper.Mapper.MAPPER;
import static io.dingodb.store.utils.ResolveLockUtil.checkSecondaryAllLocks;
import static io.dingodb.store.utils.ResolveLockUtil.resolveAsyncResolveData;
import static io.dingodb.store.utils.ResolveLockUtil.txnCheckTxnStatus;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public class TransactionStoreInstance {

    private final StoreService storeService;
    private final IndexService indexService;
    private final CommonId partitionId;
    private final DocumentService documentService;

    private static final int VectorKeyLen = 17;

    public TransactionStoreInstance(StoreService storeService, IndexService indexService, CommonId partitionId) {
        this(storeService, indexService, null, partitionId);
    }

    public TransactionStoreInstance(
        StoreService storeService,
        IndexService indexService,
        DocumentService documentService,
        CommonId partitionId
    ) {
        this.storeService = storeService;
        this.partitionId = partitionId;
        this.indexService = indexService;
        this.documentService = documentService;
    }

    private byte[] setId(byte[] key) {
        return CodecService.getDefault().setId(key, partitionId);
    }

    public void heartbeat(TxnPreWrite txnPreWrite) {
        LogUtils.info(log, "pre write optimistic heartbeat startTs:{}", txnPreWrite.getStartTs());
        heartBeat(txnPreWrite.getStartTs(), txnPreWrite.getPrimaryLock(), false);
    }

    public void heartbeat(TxnPessimisticLock txnPessimisticLock) {
        LogUtils.info(log, "pre write pessimistic heartbeat startTs:{}", txnPessimisticLock.getStartTs());
        heartBeat(txnPessimisticLock.getStartTs(), txnPessimisticLock.getPrimaryLock(), true);
    }

    public void heartBeat(long startTs, byte[] primaryLock, boolean pessimistic) {
        try {
            TxnHeartBeatRequest request = TxnHeartBeatRequest.builder()
                .primaryLock(primaryLock)
                .startTs(startTs)
                .adviseLockTtl(TsoService.INSTANCE.timestamp() + SECONDS.toMillis(TransactionUtil.heartBeatLockTtl))
                .build();
            if (indexService != null) {
                indexService.txnHeartBeat(request.getStartTs(), request);
            } else if (documentService != null) {
                documentService.txnHeartBeat(request.getStartTs(), request);
            } else {
                storeService.txnHeartBeat(request.getStartTs(), request);
            }
        } catch (Exception e) {
            LogUtils.error(log, "txn heartbeat, pessimistic:{}, startTs:{}, error:{}", pessimistic, startTs, e);
            throw e;
        }
    }

    public boolean txnPreWrite(TxnPreWrite txnPreWrite, long timeOut) {
        txnPreWrite.getMutations().stream().peek($ -> $.setKey(setId($.getKey()))).forEach($ -> $.getKey()[0] = 't');
        return txnPreWriteRealKey(txnPreWrite, timeOut);
    }

    public boolean txnPreWriteRealKey(TxnPreWrite txnPreWrite, long timeOut) {
        long start = System.currentTimeMillis();
        long startTs = txnPreWrite.getStartTs();
        CommonId txnId = new CommonId(CommonId.CommonType.TRANSACTION, TransactionManager.getServerId().seq, startTs);
        MdcUtils.setTxnId(txnId.toString());
        try {
            int n = 1;
            IsolationLevel isolationLevel = txnPreWrite.getIsolationLevel();
            List<Long> resolvedLocks = new ArrayList<>();
            while (true) {
                TxnPrewriteRequest request = MAPPER.preWriteTo(txnPreWrite);
                TxnPrewriteResponse response;

                if (request.isTryOnePc() && request.sizeOf() > TransactionUtil.maxRpcDataSize) {
                    throw new OnePcMaxSizeExceedException("one pc phase Data size exceed in 1pc, "
                        + "max:" + TransactionUtil.maxRpcDataSize + " cur:" + request.sizeOf());
                }

                long start1 = System.currentTimeMillis();
                Mutation mutation = request.getMutations().get(0);
                if (mutation.getVector() == null && mutation.getDocument() == null) {
                    response = storeService.txnPrewrite(startTs, request);
                } else if (mutation.getDocument() != null) {
                    response = documentService.txnPrewrite(startTs, request);
                } else {
                    response = indexService.txnPrewrite(startTs, request);
                }
                long sub = System.currentTimeMillis() - start1;
                DingoMetrics.timer("txnPreWriteRpc").update(sub, TimeUnit.MILLISECONDS);
                if (response.getKeysAlreadyExist() != null && !response.getKeysAlreadyExist().isEmpty()) {
                    getJoinedPrimaryKey(txnPreWrite, response.getKeysAlreadyExist());
                }
                if (response.getTxnResult() == null || response.getTxnResult().isEmpty()) {
                    if (request.isTryOnePc() && response.getOnePcCommitTs() == 0) {
                        //1pc failed, Need 2pc commit, but not 2pc pre-write.
                        throw new OnePcNeedTwoPcCommit("one pc phase 1pc commit ts is 0 in response, "
                            + "so need 2pc commit, ts:" + response.getOnePcCommitTs());
                    }
                    if (txnPreWrite.isUseAsyncCommit()) {
                        LogUtils.info(log, "UseAsyncCommit txnPreWrite MinCommitTs:{}, response MinCommitTs:{}",
                            txnPreWrite.getMinCommitTs(), response.getMinCommitTs());
                        txnPreWrite.setMinCommitTs(response.getMinCommitTs());
                    }
                    return true;
                }
                ResolveLockStatus resolveLockStatus = resolveLockConflict(
                    response.getTxnResult(),
                    isolationLevel.getCode(),
                    startTs,
                    resolvedLocks,
                    "txnPreWrite",
                    false
                );
                if (resolveLockStatus == ResolveLockStatus.LOCK_TTL
                    || resolveLockStatus == ResolveLockStatus.TXN_NOT_FOUND) {
                    if (timeOut < 0) {
                        throw new RuntimeException("startTs:" + startTs + " resolve lock timeout");
                    }
                    try {
                        long lockTtl = TxnVariables.WaitFixTime;
                        if (n < TxnVariables.WaitFixNum) {
                            lockTtl = TxnVariables.WaitTime * n;
                        }
                        Thread.sleep(lockTtl);
                        n++;
                        timeOut -= lockTtl;
                        LogUtils.info(log, "txnPreWrite lockInfo wait {} ms end.", lockTtl);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        } finally {
            long sub = System.currentTimeMillis() - start;
            DingoMetrics.timer("txnPreWrite").update(sub, TimeUnit.MILLISECONDS);
        }
    }

    // Join primary key values to string by mapping
    public static String joinPrimaryKey(Object[] keyValues, TupleMapping mapping) {

        if (keyValues == null || mapping == null) {
            throw new IllegalArgumentException("Parameters cannot be null");
        }
        StringJoiner joiner = new StringJoiner("-");
        try {
            mapping.stream().forEach(index ->
                joiner.add(keyValues[index] == null ? "null" : keyValues[index].toString())
            );
        } catch (Exception e) {
            throw new RuntimeException("Error joining primary key", e);
        }
        return Optional.ofNullable(joiner.toString())
            .map(str -> "'" + str + "'")
            .orElse("");
    }

    private static String joinPrimaryKeys(String key1, String key2) {
        StringJoiner joiner = new StringJoiner(",");
        if (!key1.isEmpty()) {
            joiner.add(key1);
        }
        if (!key2.isEmpty()) {
            joiner.add(key2);
        }
        return joiner.toString();
    }

    public static void getJoinedPrimaryKey(TxnPreWrite txnPreWrite, List<AlreadyExist> keysAlreadyExist) {
        CommonId tableId = LockExtraDataList.decode(txnPreWrite.getLockExtraDatas().get(0).getExtraData()).getTableId();
        CommonId txnId = new CommonId(CommonId.CommonType.TRANSACTION,
            TransactionManager.getServerId().seq, txnPreWrite.getStartTs());
        Table table = (Table) TransactionManager.getTable(txnId, tableId);
        assert table != null;
        KeyValueCodec codec = CodecService.getDefault()
            .createKeyValueCodec(table.getCodecVersion(), table.version, table.tupleType(), table.keyMapping());
        AtomicReference<String> joinedKey = new AtomicReference<>("");
        TupleMapping keyMapping = table.keyMapping();
        keysAlreadyExist.forEach(
            i -> Optional.ofNullable(codec.decodeKeyPrefix(i.getKey()))
                .ifPresent(keyValues ->
                    joinedKey.set(joinPrimaryKeys(joinedKey.get(), joinPrimaryKey(keyValues, keyMapping)))
                )
        );
        throw new DuplicateEntryException("Duplicate entry " + joinedKey.get()
            + " for key '" + table.getName() + ".PRIMARY'");
    }

    public Future<?> txnPreWritePrimaryKey(TxnPreWrite txnPreWrite, long timeOut) {
        if (txnPreWrite(txnPreWrite, timeOut)) {
            LogUtils.info(log, "txn heartbeat, startTs:{}", txnPreWrite.getStartTs());
            return Executors.scheduleWithFixedDelayAsync(
                "txn-heartbeat-" + txnPreWrite.getStartTs(),
                () -> heartbeat(txnPreWrite),
                30,
                10,
                SECONDS
            );
        }
        throw new WriteConflictException();
    }

    public boolean txnCommit(TxnCommit txnCommit) {
        txnCommit.getKeys().stream().peek(this::setId).forEach($ -> $[0] = 't');
        return txnCommitRealKey(txnCommit);
    }

    public boolean txnCommitRealKey(TxnCommit txnCommit) {
        long start = System.currentTimeMillis();
        String type = "normal";
        try {
            TxnCommitResponse response;
            if (indexService != null) {
                type = "index";
                response = indexService.txnCommit(txnCommit.getStartTs(), MAPPER.commitTo(txnCommit));
            } else if (documentService != null) {
                type = "document";
                response = documentService.txnCommit(txnCommit.getStartTs(), MAPPER.commitTo(txnCommit));
            } else {
                response = storeService.txnCommit(txnCommit.getStartTs(), MAPPER.commitTo(txnCommit));
            }
            if (response.getTxnResult() != null && response.getTxnResult().getCommitTsExpired() != null) {
                throw new CommitTsExpiredException(response.getTxnResult().getCommitTsExpired().toString());
            }
            return response.getTxnResult() == null;
        } finally {
            long sub = System.currentTimeMillis() - start;
            DingoMetrics.timer("txnCommitRpc" + type).update(sub, TimeUnit.MILLISECONDS);
        }
    }

    public Future txnPessimisticLockPrimaryKey(
        TxnPessimisticLock txnPessimisticLock, long timeOut, boolean ignoreLockWait,
        List<io.dingodb.common.store.KeyValue> kvRet
    ) {
        if (txnPessimisticLock(txnPessimisticLock, timeOut, ignoreLockWait, kvRet)) {
            LogUtils.info(log, "txn pessimistic heartbeat, startTs:{}, primaryKey is {}",
                txnPessimisticLock.getStartTs(), Arrays.toString(txnPessimisticLock.getPrimaryLock()));
            return Executors.scheduleWithFixedDelayAsync(
                "txn-pessimistic-heartbeat-" + txnPessimisticLock.getStartTs(),
                () -> heartbeat(txnPessimisticLock),
                30,
                10,
                SECONDS
            );
        }
        throw new WriteConflictException();
    }

    public boolean txnPessimisticLock(
        TxnPessimisticLock txnPessimisticLock, long timeOut, boolean ignoreLockWait,
        List<io.dingodb.common.store.KeyValue> kvRet
    ) {
        long start = System.currentTimeMillis();
        long startTs = txnPessimisticLock.getStartTs();
        CommonId txnId = new CommonId(CommonId.CommonType.TRANSACTION, TransactionManager.getServerId().seq, startTs);
        MdcUtils.setTxnId(txnId.toString());
        try {
            txnPessimisticLock.getMutations().stream()
                .peek($ -> $.setKey(setId($.getKey()))).forEach($ -> $.getKey()[0] = 't');
            IsolationLevel isolationLevel = txnPessimisticLock.getIsolationLevel();
            int n = 1;
            List<Long> resolvedLocks = new ArrayList<>();
            ResolveLockStatus resolveLockFlag = ResolveLockStatus.NONE;
            while (true) {
                TxnPessimisticLockResponse response;
                if (indexService != null) {
                    txnPessimisticLock.getMutations().forEach($ -> $.setKey(Arrays.copyOf($.getKey(), VectorKeyLen)));
                    response = indexService.txnPessimisticLock(
                        startTs, MAPPER.pessimisticLockTo(txnPessimisticLock)
                    );
                } else if (documentService != null) {
                    txnPessimisticLock.getMutations().forEach($ -> $.setKey(Arrays.copyOf($.getKey(), VectorKeyLen)));
                    response = documentService.txnPessimisticLock(
                        startTs, MAPPER.pessimisticLockTo(txnPessimisticLock)
                    );
                } else {
                    response = storeService.txnPessimisticLock(
                        startTs, MAPPER.pessimisticLockTo(txnPessimisticLock)
                    );
                }
                if (response.getTxnResult() == null || response.getTxnResult().isEmpty()) {
                    if (resolveLockFlag == ResolveLockStatus.LOCK_TTL && ignoreLockWait) {
                        LogUtils.warn(log, "txnPessimisticLock lock wait end...");
                        throw new LockWaitException("Lock wait");
                    }

                    if (response.getKvs() != null) {
                        kvRet.addAll(response.getKvs().stream().map(MAPPER::kvFrom).collect(Collectors.toList()));
                    } else if (response.getVector() != null) {
                        kvRet.addAll(response.getVector().stream()
                            .map(vectorWithId -> vectorWithId != null
                                ? new io.dingodb.common.store.KeyValue(vectorWithId.getTableData().getTableKey(),
                                    vectorWithId.getTableData().getTableValue()) : null)
                            .collect(Collectors.toList()));
                    } else if (response.getDocuments() != null) {
                        kvRet.addAll(response.getDocuments().stream()
                                .map(documentWithId -> documentWithId != null
                                    ? new io.dingodb.common.store.KeyValue(
                                        documentWithId.getDocument().getTableData().getTableKey(),
                                        documentWithId.getDocument().getTableData().getTableValue()
                                ) : null)
                                .collect(Collectors.toList()));
                    }
                    return true;
                }
                ResolveLockStatus resolveLockStatus = resolveLockConflict(
                    response.getTxnResult(),
                    isolationLevel.getCode(),
                    startTs,
                    resolvedLocks,
                    "txnPessimisticLock",
                    false
                );
                if (resolveLockStatus == ResolveLockStatus.LOCK_TTL
                    || resolveLockStatus == ResolveLockStatus.TXN_NOT_FOUND) {
                    if (timeOut < 0) {
                        throw new RuntimeException("Lock wait timeout exceeded; try restarting transaction");
                    }
                    try {
                        resolveLockFlag = resolveLockStatus;
                        long lockTtl = TxnVariables.WaitFixTime;
                        if (n < TxnVariables.WaitFixNum) {
                            lockTtl = TxnVariables.WaitTime * n;
                        }
                        Thread.sleep(lockTtl);
                        n++;
                        timeOut -= lockTtl;
                        LogUtils.info(log, "txnPessimisticLock lockInfo wait {} ms end.", lockTtl);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                long forUpdateTs = TsoService.INSTANCE.tso();
                txnPessimisticLock.setForUpdateTs(forUpdateTs);
            }
        } finally {
            long sub = System.currentTimeMillis() - start;
            DingoMetrics.timer("txnPessimisticLock").update(sub, TimeUnit.MILLISECONDS);
        }
    }

    public boolean txnPessimisticLockRollback(TxnPessimisticRollBack txnPessimisticRollBack) {
        long start = System.currentTimeMillis();
        long startTs = txnPessimisticRollBack.getStartTs();
        CommonId txnId = new CommonId(CommonId.CommonType.TRANSACTION, TransactionManager.getServerId().seq, startTs);
        MdcUtils.setTxnId(txnId.toString());
        try {
            txnPessimisticRollBack.getKeys().stream().peek(this::setId).forEach($ -> $[0] = 't');
            TxnPessimisticRollbackResponse response;
            if (indexService != null) {
                List<byte[]> keys = txnPessimisticRollBack.getKeys();
                List<byte[]> newKeys = keys.stream()
                    .map(key -> Arrays.copyOf(key, VectorKeyLen))
                    .collect(Collectors.toList());
                txnPessimisticRollBack.setKeys(newKeys);
                response = indexService.txnPessimisticRollback(
                    startTs, MAPPER.pessimisticRollBackTo(txnPessimisticRollBack)
                );
            } else if (documentService != null) {
                List<byte[]> keys = txnPessimisticRollBack.getKeys();
                List<byte[]> newKeys = keys.stream()
                    .map(key -> Arrays.copyOf(key, VectorKeyLen))
                    .collect(Collectors.toList());
                txnPessimisticRollBack.setKeys(newKeys);
                response = documentService.txnPessimisticRollback(
                    startTs, MAPPER.pessimisticRollBackTo(txnPessimisticRollBack)
                );
            } else {
                response = storeService.txnPessimisticRollback(
                    startTs, MAPPER.pessimisticRollBackTo(txnPessimisticRollBack)
                );
            }
            if (response.getTxnResult() != null && !response.getTxnResult().isEmpty()) {
                LogUtils.error(log, "txnPessimisticLockRollback txnResult:{}", response.getTxnResult().toString());
                for (TxnResultInfo txnResultInfo : response.getTxnResult()) {
                    LockInfo lockInfo = txnResultInfo.getLocked();
                    if (lockInfo != null && lockInfo.getLockTs() == startTs && lockInfo.getLockType() != Op.Lock) {
                        LogUtils.info(log, "txnPessimisticLockRollback lockInfo:{}", lockInfo.toString());
                        TxnBatchRollBack rollBackRequest = TxnBatchRollBack.builder()
                            .isolationLevel(txnPessimisticRollBack.getIsolationLevel())
                            .startTs(startTs)
                            .keys(singletonList(lockInfo.getKey()))
                            .build();
                        boolean result = txnBatchRollback(rollBackRequest);
                        if (!result) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                return true;
            }
            return response.getTxnResult() == null;
        } finally {
            long sub = System.currentTimeMillis() - start;
            DingoMetrics.timer("txnPessimisticLockRollback").update(sub, TimeUnit.MILLISECONDS);
        }
    }

    public Iterator<io.dingodb.common.store.KeyValue> txnScan(long ts, StoreInstance.Range range, long timeOut) {
        return txnScan(ts, range, timeOut, null);
    }

    public Iterator<io.dingodb.common.store.KeyValue> txnScan(
        long ts,
        StoreInstance.Range range,
        long timeOut,
        CoprocessorV2 coprocessor
    ) {
        Stream.of(range.start).peek(this::setId).forEach($ -> $[0] = 't');
        Stream.of(range.end).peek(this::setId).forEach($ -> $[0] = 't');

        if (ScopeVariables.txnScanByStream()) {
            return getScanStreamIterator(ts, range, timeOut, coprocessor);
        } else {
            return getScanIterator(ts, range, timeOut, coprocessor);
        }
    }

    public Iterator<io.dingodb.common.store.KeyValue> txnScanWithoutStream(
        long ts, StoreInstance.Range range, long timeOut
    ) {
        Stream.of(range.start).peek(this::setId).forEach($ -> $[0] = 't');
        Stream.of(range.end).peek(this::setId).forEach($ -> $[0] = 't');
        return getScanIterator(ts, range, timeOut, null);
    }

    @NonNull
    public ScanIterator getScanIterator(long ts, StoreInstance.Range range, long timeOut, CoprocessorV2 coprocessor) {
        return new ScanIterator(ts, range, timeOut, coprocessor);
    }

    @NonNull
    public ScanStreamIterator getScanStreamIterator(
        long ts, StoreInstance.Range range, long timeOut, CoprocessorV2 coprocessor
    ) {
        return new ScanStreamIterator(ts, range, timeOut, coprocessor);
    }

    public List<io.dingodb.common.store.KeyValue> txnGet(long startTs, List<byte[]> keys, long timeOut) {
        keys.stream().peek(this::setId).forEach($ -> $[0] = 't');
        return getKeyValues(startTs, keys, timeOut);
    }

    @NonNull
    public List<io.dingodb.common.store.KeyValue> getKeyValues(long startTs, List<byte[]> keys, long timeOut) {
        long start = System.currentTimeMillis();
        CommonId txnId = new CommonId(CommonId.CommonType.TRANSACTION, TransactionManager.getServerId().seq, startTs);
        MdcUtils.setTxnId(txnId.toString());
        try {
            int n = 1;
            List<Long> resolvedLocks = new ArrayList<>();
            while (true) {
                TxnBatchGetRequest txnBatchGetRequest = MAPPER.batchGetTo(
                    startTs, IsolationLevel.SnapshotIsolation, keys
                );
                txnBatchGetRequest.setResolveLocks(resolvedLocks);
                TxnBatchGetResponse response;
                if (indexService != null) {
                    txnBatchGetRequest.getKeys().forEach($ -> Arrays.copyOf($, VectorKeyLen));
                    response = indexService.txnBatchGet(startTs, txnBatchGetRequest);
                    if (response.getTxnResult() == null) {
                        return response.getVectors().stream()
                            .map(vectorWithId -> vectorWithId != null
                                ? new io.dingodb.common.store.KeyValue(vectorWithId.getTableData().getTableKey(),
                                    vectorWithId.getTableData().getTableValue()) : null)
                            .collect(Collectors.toList());
                    }
                } else if (documentService != null) {
                    txnBatchGetRequest.getKeys().forEach($ -> Arrays.copyOf($, VectorKeyLen));
                    response = documentService.txnBatchGet(startTs, txnBatchGetRequest);
                    if (response.getTxnResult() == null) {
                        return response.getDocuments().stream()
                            .map(documentWithId -> documentWithId != null
                                ? new io.dingodb.common.store.KeyValue(
                                    documentWithId.getDocument().getTableData().getTableKey(),
                                    documentWithId.getDocument().getTableData().getTableValue()
                                    ) : null
                            )
                            .collect(Collectors.toList());
                    }
                } else {
                    response = storeService.txnBatchGet(startTs, txnBatchGetRequest);
                    if (response.getTxnResult() == null) {
                        return response.getKvs().stream().map(MAPPER::kvFrom).collect(Collectors.toList());
                    }
                }
                ResolveLockStatus resolveLockStatus = resolveLockConflict(
                    singletonList(response.getTxnResult()),
                    IsolationLevel.SnapshotIsolation.getCode(),
                    startTs,
                    resolvedLocks,
                    "txnScan",
                    true
                );
                if (resolveLockStatus == ResolveLockStatus.LOCK_TTL
                    || resolveLockStatus == ResolveLockStatus.TXN_NOT_FOUND) {
                    if (timeOut < 0) {
                        throw new RuntimeException("startTs:" + startTs + " resolve lock timeout");
                    }
                    try {
                        long lockTtl = TxnVariables.WaitFixTime;
                        if (n < TxnVariables.WaitFixNum) {
                            lockTtl = TxnVariables.WaitTime * n;
                        }
                        Thread.sleep(lockTtl);
                        n++;
                        timeOut -= lockTtl;
                        LogUtils.info(log, "txnBatchGet lockInfo wait {} ms end.", lockTtl);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        } finally {
            long sub = System.currentTimeMillis() - start;
            DingoMetrics.timer("txnBatchGetRpc").update(sub, TimeUnit.MILLISECONDS);
        }
    }

    public boolean txnBatchRollback(TxnBatchRollBack txnBatchRollBack) {
        long start = System.currentTimeMillis();
        long startTs = txnBatchRollBack.getStartTs();
        CommonId txnId = new CommonId(CommonId.CommonType.TRANSACTION, TransactionManager.getServerId().seq, startTs);
        MdcUtils.setTxnId(txnId.toString());
        txnBatchRollBack.getKeys().stream().peek(this::setId).forEach($ -> $[0] = 't');
        TxnBatchRollbackResponse response;
        if (indexService != null) {
            txnBatchRollBack.getKeys().forEach($ -> Arrays.copyOf($, VectorKeyLen));
            response = indexService.txnBatchRollback(
                startTs, MAPPER.rollbackTo(txnBatchRollBack)
            );
        } else if (documentService != null) {
            txnBatchRollBack.getKeys().forEach($ -> Arrays.copyOf($, VectorKeyLen));
            response = documentService.txnBatchRollback(
                startTs, MAPPER.rollbackTo(txnBatchRollBack)
            );
        } else {
            response = storeService.txnBatchRollback(
                startTs, MAPPER.rollbackTo(txnBatchRollBack)
            );
        }
        if (response.getTxnResult() != null) {
            LogUtils.error(log, "txnBatchRollback txnResult:{}", response.getTxnResult().toString());
        }
        long sub = System.currentTimeMillis() - start;
        DingoMetrics.timer("txnBatchRollbackRpc").update(sub, TimeUnit.MILLISECONDS);
        return response.getTxnResult() == null;
    }


    public TxnResolveLockResponse txnResolveLock(TxnResolveLock txnResolveLock) {
        long start = System.currentTimeMillis();
        try {
            if (indexService != null) {
                return indexService.txnResolveLock(txnResolveLock.getStartTs(), MAPPER.resolveTxnTo(txnResolveLock));
            }
            if (documentService != null) {
                return documentService.txnResolveLock(txnResolveLock.getStartTs(), MAPPER.resolveTxnTo(txnResolveLock));
            }
            return storeService.txnResolveLock(txnResolveLock.getStartTs(), MAPPER.resolveTxnTo(txnResolveLock));
        } finally {
            long sub = System.currentTimeMillis() - start;
            DingoMetrics.timer("txnResolveLockRpc").update(sub, TimeUnit.MILLISECONDS);
        }
    }

    public ResolveLockStatus resolveLockConflict(List<TxnResultInfo> txnResult, int isolationLevel,
                                                 long startTs, List<Long> resolvedLocks, String funName,
                                                 boolean forRead) {
        long start = System.currentTimeMillis();
        ResolveLockStatus resolveLockStatus = ResolveLockStatus.NONE;
        for (TxnResultInfo txnResultInfo : txnResult) {
            LogUtils.debug(log, "startTs:{}, {} txnResultInfo : {}", startTs, funName, txnResultInfo);
            boolean forceSyncCommit = false;
            LockInfo lockInfo = txnResultInfo.getLocked();
            if (lockInfo != null) {
                try {
                    resolveLockStatus = getResolveLockStatus(
                        isolationLevel,
                        startTs,
                        funName,
                        resolveLockStatus,
                        resolvedLocks,
                        forceSyncCommit,
                        forRead,
                        lockInfo
                    );
                } catch (NonAsyncCommitLockException e) {
                    resolveLockStatus = getResolveLockStatus(
                        isolationLevel,
                        startTs,
                        funName,
                        resolveLockStatus,
                        resolvedLocks,
                        true,
                        forRead,
                        lockInfo
                    );
                }
            } else {
                WriteConflict writeConflict = txnResultInfo.getWriteConflict();
                LogUtils.info(log, "startTs:{}, {} writeConflict : {}", startTs, funName, writeConflict);
                if (writeConflict != null) {
                    //  write column exist and commit_ts > for_update_ts
                    if (funName.equalsIgnoreCase("txnPessimisticLock")) {
                        continue;
                    }
                    throw new WriteConflictException(writeConflict.toString(), writeConflict.getKey());
                }
            }
        }
        long sub = System.currentTimeMillis() - start;
        if (forRead) {
            DingoMetrics.timer("readResolveConflict").update(sub, TimeUnit.MILLISECONDS);
        } else {
            DingoMetrics.timer("writeResolveLockConflict").update(sub, TimeUnit.MILLISECONDS);
        }
        return resolveLockStatus;
    }

    private ResolveLockStatus getResolveLockStatus(int isolationLevel, long startTs, String funName,
                                                   ResolveLockStatus resolveLockStatus, List<Long> resolvedLocks,
                                                   boolean forceSyncCommit, boolean forRead, LockInfo lockInfo) {
        // CheckTxnStatus
        LogUtils.debug(log, "startTs:{}, {} lockInfo : {}", startTs, funName, lockInfo);
        long currentTs = TsoService.INSTANCE.tso();
        TxnCheckStatus txnCheckStatus = TxnCheckStatus.builder()
            .isolationLevel(IsolationLevel.of(isolationLevel))
            .primaryKey(lockInfo.getPrimaryLock())
            .lockTs(lockInfo.getLockTs())
            .callerStartTs(startTs)
            .currentTs(currentTs)
            .forceSyncCommit(forceSyncCommit)
            .build();
        TxnCheckTxnStatusResponse statusResponse = txnCheckTxnStatus(txnCheckStatus);
        LogUtils.info(log, "startTs:{}, {} txnCheckStatus : {}", startTs, funName, statusResponse);
        TxnResultInfo resultInfo = statusResponse.getTxnResult();
        // success
        Action action = statusResponse.getAction();
        if (resultInfo == null) {
            long lockTtl = statusResponse.getLockTtl();
            long commitTs = statusResponse.getCommitTs();
            if (statusResponse.getLockInfo() != null &&
                statusResponse.getLockInfo().isUseAsyncCommit() &&
                !forceSyncCommit) {
                if (lockTtl > 0 && !TsoService.INSTANCE.IsExpired(lockTtl)) {
                    LogUtils.info(log, "startTs:{}, lockTs:{} useAsyncCommit lockTtl not IsExpired, lockTtl:{}",
                        startTs, lockInfo.getLockTs(), lockTtl);
                    if (lockInfo.getMinCommitTs() >= startTs && forRead) {
                        resolvedLocks.add(lockInfo.getLockTs());
                    }
                    // wait
                    return ResolveLockStatus.LOCK_TTL;
                } else {
                    LogUtils.info(log, "startTs:{}, lockTs:{},lockTtl:{},useAsyncCommit check, minCommitTs:{}",
                        startTs, lockInfo.getLockTs(), lockTtl, statusResponse.getLockInfo().getMinCommitTs());
                    List<byte[]> secondaries = statusResponse.getLockInfo().getSecondaries();
                    AsyncResolveData asyncResolveData = AsyncResolveData.builder()
                        .missingLock(false)
                        .commitTs(statusResponse.getLockInfo().getMinCommitTs())
                        .keys(new HashSet<>(secondaries))
                        .build();
                    // checkSecondaryLocks and asyncResolveData add keys
                    checkSecondaryAllLocks(
                        isolationLevel,
                        startTs,
                        lockInfo,
                        secondaries,
                        asyncResolveData
                    );
                    asyncResolveData.getKeys().add(statusResponse.getLockInfo().getPrimaryLock());
                    Integer retry = io.dingodb.common.util.Optional.mapOrGet(
                        DingoConfiguration.instance().find("retry", int.class),
                        __ -> __,
                        () -> 30
                    );
                    // resolveAsyncResolveData
                    return resolveAsyncResolveData(
                        isolationLevel,
                        startTs,
                        funName,
                        asyncResolveData,
                        retry,
                        lockInfo
                    );
                }
            }
            if (lockInfo.getLockType() == Op.Lock && lockInfo.getForUpdateTs() != 0
                && (action == Action.LockNotExistRollback
                || action == Action.TTLExpirePessimisticRollback
                || action == Action.TTLExpireRollback)) {
                // pessimistic lock
                TxnPessimisticRollBack pessimisticRollBack = TxnPessimisticRollBack.builder()
                    .isolationLevel(IsolationLevel.of(isolationLevel))
                    .startTs(lockInfo.getLockTs())
                    .forUpdateTs(lockInfo.getForUpdateTs())
                    .keys(Collections.singletonList(lockInfo.getKey()))
                    .build();
                txnPessimisticLockRollback(pessimisticRollBack);
                resolveLockStatus = ResolveLockStatus.PESSIMISTIC_ROLLBACK;
            } else if (lockTtl > 0) {
                if (action != null && forRead) {
                    // wait
                    switch (action) {
                        case MinCommitTSPushed:
                            resolvedLocks.add(lockInfo.getLockTs());
                            resolveLockStatus = ResolveLockStatus.LOCK_TTL;
                            break;
                        default:
                            break;
                    }
                } else {
                    // wait
                    resolveLockStatus = ResolveLockStatus.LOCK_TTL;
                }
            } else if (commitTs > 0) {
                // resolveLock store commit
                TxnResolveLock resolveLockRequest = TxnResolveLock.builder()
                    .isolationLevel(IsolationLevel.of(isolationLevel))
                    .startTs(lockInfo.getLockTs())
                    .commitTs(commitTs)
                    .keys(singletonList(lockInfo.getKey()))
                    .build();
                TxnResolveLockResponse txnResolveLockRes = txnResolveLock(resolveLockRequest);
                LogUtils.info(log,
                    "startTs:{}, {} txnResolveLockResponse: {}", startTs, funName, txnResolveLockRes);
                resolveLockStatus = ResolveLockStatus.COMMIT;
            } else if (lockTtl == 0 && commitTs == 0) {
                // resolveLock store rollback
                TxnResolveLock resolveLockRequest = TxnResolveLock.builder()
                    .isolationLevel(IsolationLevel.of(isolationLevel))
                    .startTs(lockInfo.getLockTs())
                    .commitTs(commitTs)
                    .keys(singletonList(lockInfo.getKey()))
                    .build();
                TxnResolveLockResponse txnResolveLockRes = txnResolveLock(resolveLockRequest);
                LogUtils.info(log,
                    "startTs:{}, {} txnResolveLockResponse: {}", startTs, funName, txnResolveLockRes);
                resolveLockStatus = ResolveLockStatus.ROLLBACK;
            }
        } else {
            lockInfo = resultInfo.getLocked();
            if (lockInfo != null) {
                if (lockInfo.isUseAsyncCommit() && !forceSyncCommit) {
                    long lockTtl = lockInfo.getLockTtl();
                    if (lockTtl > 0 && !TsoService.INSTANCE.IsExpired(lockTtl)) {
                        LogUtils.info(log, "startTs:{}, lockTs:{} useAsyncCommit lockTtl not IsExpired, " +
                                "lockTtl:{}", startTs, lockInfo.getLockTs(), lockTtl);
                        if (lockInfo.getMinCommitTs() >= startTs && forRead) {
                            resolvedLocks.add(lockInfo.getLockTs());
                        }
                        // wait
                        return ResolveLockStatus.LOCK_TTL;
                    } else {
                        LogUtils.info(log, "startTs:{}, lockTs:{} useAsyncCommit check, minCommitTs:{}",
                            startTs, lockInfo.getLockTs(), lockInfo.getMinCommitTs());
                        List<byte[]> secondaries = lockInfo.getSecondaries();
                        AsyncResolveData asyncResolveData = AsyncResolveData.builder()
                            .missingLock(false)
                            .commitTs(lockInfo.getMinCommitTs())
                            .keys(new HashSet<>(secondaries))
                            .build();
                        // checkSecondaryLocks and asyncResolveData add keys
                        checkSecondaryAllLocks(
                            isolationLevel,
                            startTs,
                            lockInfo,
                            secondaries,
                            asyncResolveData
                        );
                        asyncResolveData.getKeys().add(lockInfo.getPrimaryLock());
                        LogUtils.info(log, "startTs:{}, asyncResolveData:{}", startTs, asyncResolveData);
                        Integer retry = io.dingodb.common.util.Optional.mapOrGet(
                            DingoConfiguration.instance().find("retry", int.class),
                            __ -> __,
                            () -> 30
                        );
                        // resolveAsyncResolveData
                        return resolveAsyncResolveData(
                            isolationLevel,
                            startTs,
                            funName,
                            asyncResolveData,
                            retry,
                            lockInfo
                        );
                    }
                }
                // success
                if (forRead && statusResponse.getAction() == Action.MinCommitTSPushed &&
                    statusResponse.getLockTtl() > 0) {
                    resolvedLocks.add(lockInfo.getLockTs());
                    return ResolveLockStatus.MIN_COMMIT_TS_PUSHED;
                }
                // pessimistic lock
                if (lockInfo.getLockType() == Op.Lock && lockInfo.getForUpdateTs() != 0) {
                    if (action == Action.LockNotExistRollback
                        || action == Action.TTLExpirePessimisticRollback
                        || action == Action.TTLExpireRollback) {
                        TxnPessimisticRollBack pessimisticRollBack = TxnPessimisticRollBack.builder()
                            .isolationLevel(IsolationLevel.of(isolationLevel))
                            .startTs(lockInfo.getLockTs())
                            .forUpdateTs(lockInfo.getForUpdateTs())
                            .keys(Collections.singletonList(lockInfo.getKey()))
                            .build();
                        txnPessimisticLockRollback(pessimisticRollBack);
                        return ResolveLockStatus.PESSIMISTIC_ROLLBACK;
                    } else {
                        if (forRead && lockInfo.getMinCommitTs() >= startTs) {
                            resolvedLocks.add(lockInfo.getLockTs());
                            return ResolveLockStatus.MIN_COMMIT_TS_PUSHED;
                        } else {
                            return ResolveLockStatus.LOCK_TTL;
                        }
                    }
                }
            }
            // 1、PrimaryMismatch  or  TxnNotFound
            if (resultInfo.getPrimaryMismatch() != null) {
                throw new PrimaryMismatchException(resultInfo.getPrimaryMismatch().toString());
            } else if (resultInfo.getTxnNotFound() != null) {
                LogUtils.warn(log, "startTs:{}, {} txnNotFound : {}", startTs, funName,
                    resultInfo.getTxnNotFound().toString());
                resolveLockStatus = ResolveLockStatus.TXN_NOT_FOUND;
            } else if (resultInfo.getLocked() != null) {
                throw new RuntimeException(resultInfo.getLocked().toString());
            }
        }
        return resolveLockStatus;
    }

    private ResolveLockStatus readResolveConflict(List<TxnResultInfo> txnResult, int isolationLevel,
                                                  long startTs, List<Long> resolvedLocks, String funName) {
        long start = System.currentTimeMillis();
        ResolveLockStatus resolveLockStatus = ResolveLockStatus.NONE;
        for (TxnResultInfo txnResultInfo : txnResult) {
            LogUtils.debug(log, "startTs:{}, {} txnResultInfo : {}", startTs, funName, txnResultInfo);
            LockInfo lockInfo = txnResultInfo.getLocked();
            if (lockInfo != null) {
                // CheckTxnStatus
                LogUtils.debug(log, "startTs:{}, {} lockInfo : {}", startTs, funName, lockInfo);
                long currentTs = TsoService.INSTANCE.tso();
                TxnCheckStatus txnCheckStatus = TxnCheckStatus.builder()
                    .isolationLevel(IsolationLevel.of(isolationLevel))
                    .primaryKey(lockInfo.getPrimaryLock())
                    .lockTs(lockInfo.getLockTs())
                    .callerStartTs(startTs)
                    .currentTs(currentTs)
                    .build();
                TxnCheckTxnStatusResponse statusResponse = txnCheckTxnStatus(txnCheckStatus);
                LogUtils.info(log, "startTs: {}, {} txnCheckStatus : {}", startTs, funName, statusResponse);
                TxnResultInfo resultInfo = statusResponse.getTxnResult();
                if (resultInfo == null) {
                    Action action = statusResponse.getAction();
                    long lockTtl = statusResponse.getLockTtl();
                    long commitTs = statusResponse.getCommitTs();
                    if (lockInfo.getLockType() == Op.Lock && lockInfo.getForUpdateTs() != 0
                        && (action == Action.LockNotExistRollback
                        || action == Action.TTLExpirePessimisticRollback
                        || action == Action.TTLExpireRollback)) {
                        // pessimistic lock
                        TxnPessimisticRollBack pessimisticRollBack = TxnPessimisticRollBack.builder()
                            .isolationLevel(IsolationLevel.of(isolationLevel))
                            .startTs(lockInfo.getLockTs())
                            .forUpdateTs(lockInfo.getForUpdateTs())
                            .keys(Collections.singletonList(lockInfo.getKey()))
                            .build();
                        txnPessimisticLockRollback(pessimisticRollBack);
                        resolveLockStatus = ResolveLockStatus.PESSIMISTIC_ROLLBACK;
                    } else if (lockTtl > 0) {
                        if (action != null) {
                            // wait
                            switch (action) {
                                case MinCommitTSPushed:
                                    resolvedLocks.add(lockInfo.getLockTs());
                                    resolveLockStatus = ResolveLockStatus.LOCK_TTL;
                                    break;
                                default:
                                    break;
                            }
                        } else {
                            // wait
                            resolveLockStatus = ResolveLockStatus.LOCK_TTL;
                        }
                    } else if (commitTs > 0) {
                        // resolveLock store commit
                        TxnResolveLock resolveLockRequest = TxnResolveLock.builder()
                            .isolationLevel(IsolationLevel.of(isolationLevel))
                            .startTs(lockInfo.getLockTs())
                            .commitTs(commitTs)
                            .keys(singletonList(lockInfo.getKey()))
                            .build();
                        TxnResolveLockResponse txnResolveLockRes = txnResolveLock(resolveLockRequest);
                        LogUtils.info(log, "startTs:{}, {} txnResolveLockResponse: {}",
                            startTs, funName, txnResolveLockRes);
                        resolveLockStatus = ResolveLockStatus.COMMIT;
                    } else if (lockTtl == 0 && commitTs == 0) {
                        // resolveLock store rollback
                        TxnResolveLock resolveLockRequest = TxnResolveLock.builder()
                            .isolationLevel(IsolationLevel.of(isolationLevel))
                            .startTs(lockInfo.getLockTs())
                            .commitTs(commitTs)
                            .keys(singletonList(lockInfo.getKey()))
                            .build();
                        TxnResolveLockResponse txnResolveLockRes = txnResolveLock(resolveLockRequest);
                        LogUtils.info(log, "startTs:{}, {} txnResolveLockResponse: {}", startTs,
                            funName, txnResolveLockRes);
                        resolveLockStatus = ResolveLockStatus.ROLLBACK;
                    }
                } else {
                    lockInfo = resultInfo.getLocked();
                    if (lockInfo != null) {
                        // success
                        if (statusResponse.getAction() == Action.MinCommitTSPushed && statusResponse.getLockTtl() > 0) {
                            resolvedLocks.add(lockInfo.getLockTs());
                            resolveLockStatus = ResolveLockStatus.MIN_COMMIT_TS_PUSHED;
                            continue;
                        }
                        // pessimistic lock
                        Action action = statusResponse.getAction();
                        if (lockInfo.getLockType() == Op.Lock && lockInfo.getForUpdateTs() != 0
                            && (action == Action.LockNotExistRollback
                            || action == Action.TTLExpirePessimisticRollback
                            || action == Action.TTLExpireRollback)) {
                            TxnPessimisticRollBack pessimisticRollBack = TxnPessimisticRollBack.builder()
                                .isolationLevel(IsolationLevel.of(isolationLevel))
                                .startTs(lockInfo.getLockTs())
                                .forUpdateTs(lockInfo.getForUpdateTs())
                                .keys(Collections.singletonList(lockInfo.getKey()))
                                .build();
                            txnPessimisticLockRollback(pessimisticRollBack);
                            resolveLockStatus = ResolveLockStatus.PESSIMISTIC_ROLLBACK;
                            continue;
                        }
                        if (lockInfo.getMinCommitTs() >= startTs) {
                            resolvedLocks.add(lockInfo.getLockTs());
                            resolveLockStatus = ResolveLockStatus.MIN_COMMIT_TS_PUSHED;
                            continue;
                        }
                    }
                    // 1、PrimaryMismatch  or  TxnNotFound
                    if (resultInfo.getPrimaryMismatch() != null) {
                        throw new PrimaryMismatchException(resultInfo.getPrimaryMismatch().toString());
                    } else if (resultInfo.getTxnNotFound() != null) {
                        LogUtils.warn(log, "startTs:{}, {} txnNotFound : {}", startTs, funName,
                            resultInfo.getTxnNotFound().toString());
                        resolveLockStatus = ResolveLockStatus.TXN_NOT_FOUND;
                    } else if (resultInfo.getLocked() != null) {
                        throw new RuntimeException(resultInfo.getLocked().toString());
                    }
                }
            } else {
                WriteConflict writeConflict = txnResultInfo.getWriteConflict();
                LogUtils.info(log, "startTs:{}, {} writeConflict : {}", startTs, funName, writeConflict);
                if (writeConflict != null) {
                    throw new WriteConflictException(writeConflict.toString(), writeConflict.getKey());
                }
            }
        }
        long sub = System.currentTimeMillis() - start;
        DingoMetrics.timer("readResolveConflict").update(sub, TimeUnit.MILLISECONDS);
        return resolveLockStatus;
    }

    public class ScanIterator implements ProfileScanIterator {
        private final long startTs;
        private final StoreInstance.Range range;
        private final long timeOut;
        private final io.dingodb.sdk.service.entity.common.CoprocessorV2 coprocessor;

        private boolean withStart;
        private boolean hasMore = true;
        private int limit;
        private StoreInstance.Range current;
        private Iterator<KeyValue> keyValues;
        private final OperatorProfile rpcProfile;
        private final OperatorProfile initRpcProfile;

        public ScanIterator(long startTs, StoreInstance.Range range, long timeOut) {
            this(startTs, range, timeOut, null);
        }

        public ScanIterator(long startTs, StoreInstance.Range range, long timeOut, CoprocessorV2 coprocessor) {
            this.startTs = startTs;
            this.range = range;
            this.current = range;
            this.withStart = range.withStart;
            this.timeOut = timeOut;
            limit = ScopeVariables.getRpcBatchSize();
            if (coprocessor != null && coprocessor.getLimit() > 0) {
                limit = coprocessor.getLimit();
            }
            this.coprocessor = MAPPER.coprocessorTo(coprocessor);
            Optional.ofNullable(this.coprocessor)
                .map(io.dingodb.sdk.service.entity.common.CoprocessorV2::getOriginalSchema)
                .ifPresent($ -> $.setCommonId(partitionId.seq));
            Optional.ofNullable(this.coprocessor)
                .map(io.dingodb.sdk.service.entity.common.CoprocessorV2::getResultSchema)
                .ifPresent($ -> $.setCommonId(partitionId.seq));
            initRpcProfile = new OperatorProfile("initTxnRpc");
            rpcProfile = new OperatorProfile("continueTxnRpc");
            initRpcProfile.start();
            long start = System.currentTimeMillis();
            fetch();
            initRpcProfile.time(start);
            initRpcProfile.end();
        }

        private synchronized void fetch() {
            if (!hasMore) {
                return;
            }
            long start = System.currentTimeMillis();
            CommonId txnId = new CommonId(
                CommonId.CommonType.TRANSACTION,
                TransactionManager.getServerId().seq,
                startTs
            );
            MdcUtils.setTxnId(txnId.toString());
            long scanTimeOut = timeOut;
            int n = 1;
            List<Long> resolvedLocks = new ArrayList<>();
            while (true) {
                TxnScanRequest txnScanRequest = MAPPER.scanTo(startTs, IsolationLevel.SnapshotIsolation, current);
                txnScanRequest.setLimit(limit);
                txnScanRequest.setResolveLocks(resolvedLocks);
                txnScanRequest.setCoprocessor(coprocessor);
                TxnScanResponse txnScanResponse;
                if (indexService != null) {
                    txnScanResponse = indexService.txnScan(startTs, txnScanRequest);
                } else if (documentService != null) {
                    txnScanResponse = documentService.txnScan(startTs, txnScanRequest);
                } else {
                    txnScanResponse = storeService.txnScan(startTs, txnScanRequest);
                }
                if (txnScanResponse.getTxnResult() != null) {
                    ResolveLockStatus resolveLockStatus = resolveLockConflict(
                        singletonList(txnScanResponse.getTxnResult()),
                        IsolationLevel.SnapshotIsolation.getCode(),
                        startTs,
                        resolvedLocks,
                        "txnScan",
                        true
                    );
                    if (resolveLockStatus == ResolveLockStatus.LOCK_TTL
                        || resolveLockStatus == ResolveLockStatus.TXN_NOT_FOUND) {
                        if (scanTimeOut < 0) {
                            LogUtils.info(log, "scanTimeOut < 0, scanTs:{}", txnScanRequest.getStartTs());
                            throw new RuntimeException("startTs:" + txnScanRequest.getStartTs()
                                + " resolve lock timeout");
                        }
                        try {
                            long lockTtl = TxnVariables.WaitFixTime;
                            if (n < TxnVariables.WaitFixNum) {
                                lockTtl = TxnVariables.WaitTime * n;
                            }
                            Thread.sleep(lockTtl);
                            n++;
                            scanTimeOut -= lockTtl;
                            LogUtils.info(log, "scanTs:{}, txnScan lockInfo wait {} ms end.",
                                txnScanRequest.getStartTs(), lockTtl);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    continue;
                }
                keyValues = Optional.ofNullable(txnScanResponse.getKvs())
                    .map(List::iterator).orElseGet(Collections::emptyIterator);
                hasMore = txnScanResponse.isHasMore();
                if (hasMore) {
                    withStart = false;
                    current = new StoreInstance.Range(txnScanResponse.getEndKey(), range.end, withStart, range.withEnd);
                }
                break;
            }
            long sub = System.currentTimeMillis() - start;
            DingoMetrics.timer("txnScanRpc").update(sub, TimeUnit.MILLISECONDS);
        }

        @Override
        public boolean hasNext() {
            while (hasMore && !keyValues.hasNext()) {
                if (rpcProfile.getStart() == 0) {
                    rpcProfile.start();
                }
                long start = System.currentTimeMillis();
                fetch();
                rpcProfile.time(start);
            }
            return keyValues.hasNext();
        }

        @Override
        public io.dingodb.common.store.KeyValue next() {
            return MAPPER.kvFrom(keyValues.next());
        }

        @Override
        public Profile getRpcProfile() {
            return rpcProfile;
        }

        @Override
        public Profile getInitRpcProfile() {
            return initRpcProfile;
        }
    }

    /**
     * To support TxnScanStream request and response.
     */
    public class ScanStreamIterator implements ProfileScanIterator {
        private final long startTs;
        private StoreInstance.Range range;
        private final long timeOut;
        private final io.dingodb.sdk.service.entity.common.CoprocessorV2 coprocessor;

        private boolean withStart;
        private boolean hasMore = true;
        private int limit;
        private String streamId;
        private boolean closeStream;
        private Iterator<KeyValue> keyValues;
        private final OperatorProfile rpcProfile;
        private final OperatorProfile initRpcProfile;

        public ScanStreamIterator(long startTs, StoreInstance.Range range, long timeOut) {
            this(startTs, range, timeOut, null);
        }

        public ScanStreamIterator(long startTs, StoreInstance.Range range, long timeOut, CoprocessorV2 coprocessor) {
            this.startTs = startTs;
            this.range = range;
            this.withStart = range.withStart;
            this.timeOut = timeOut;
            this.streamId = null;
            this.closeStream = false;
            limit = ScopeVariables.getRpcBatchSize();
            if (coprocessor != null && coprocessor.getLimit() > 0) {
                limit = coprocessor.getLimit();
            }
            this.coprocessor = MAPPER.coprocessorTo(coprocessor);
            Optional.ofNullable(this.coprocessor)
                .map(io.dingodb.sdk.service.entity.common.CoprocessorV2::getOriginalSchema)
                .ifPresent($ -> $.setCommonId(partitionId.seq));
            Optional.ofNullable(this.coprocessor)
                .map(io.dingodb.sdk.service.entity.common.CoprocessorV2::getResultSchema)
                .ifPresent($ -> $.setCommonId(partitionId.seq));
            initRpcProfile = new OperatorProfile("initTxnRpc");
            rpcProfile = new OperatorProfile("continueTxnRpc");
            initRpcProfile.start();
            long start = System.currentTimeMillis();
            fetch();
            initRpcProfile.time(start);
            initRpcProfile.end();
        }

        private synchronized void fetch() {
            if (!hasMore) {
                return;
            }
            long start = System.currentTimeMillis();
            CommonId txnId = new CommonId(
                CommonId.CommonType.TRANSACTION,
                TransactionManager.getServerId().seq,
                startTs
            );
            MdcUtils.setTxnId(txnId.toString());
            long scanTimeOut = timeOut;
            int n = 1;
            List<Long> resolvedLocks = new ArrayList<>();

            boolean closeStream = false;

            TxnScanRequest txnScanRequest = MAPPER.scanTo(startTs, IsolationLevel.SnapshotIsolation, this.range);
            txnScanRequest.setLimit(limit);
            txnScanRequest.setCoprocessor(coprocessor);
            if (txnScanRequest.getStreamMeta() == null) {
                txnScanRequest.setStreamMeta(new StreamRequestMeta());
            }
            TxnScanResponse txnScanResponse;

            //actually it is not a loop. Just run once in normal cases.
            while (true) {
                txnScanRequest.setResolveLocks(resolvedLocks);
                txnScanRequest.getStreamMeta().setStreamId(streamId);
                txnScanRequest.getStreamMeta().setClose(closeStream);

                try {
                    if (indexService != null) {
                        txnScanResponse = indexService.txnScan(startTs, txnScanRequest);
                    } else if (documentService != null) {
                        txnScanResponse = documentService.txnScan(startTs, txnScanRequest);
                    } else {
                        txnScanResponse = storeService.txnScan(startTs, txnScanRequest);
                    }

                    if (txnScanResponse.getTxnResult() != null) {
                        ResolveLockStatus resolveLockStatus = resolveLockConflict(
                            singletonList(txnScanResponse.getTxnResult()),
                            IsolationLevel.SnapshotIsolation.getCode(),
                            startTs,
                            resolvedLocks,
                            "txnScan",
                            true
                        );
                        if (resolveLockStatus == ResolveLockStatus.LOCK_TTL
                            || resolveLockStatus == ResolveLockStatus.TXN_NOT_FOUND) {
                            if (scanTimeOut < 0) {
                                throw new RuntimeException("startTs:" + txnScanRequest.getStartTs()
                                    + " resolve lock timeout");
                            }
                            try {
                                long lockTtl = TxnVariables.WaitFixTime;
                                if (n < TxnVariables.WaitFixNum) {
                                    lockTtl = TxnVariables.WaitTime * n;
                                }
                                Thread.sleep(lockTtl);
                                n++;
                                scanTimeOut -= lockTtl;
                                LogUtils.info(log, "txnScan lockInfo wait {} ms end.", lockTtl);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        continue;
                    }

                    if (txnScanResponse.getError() == null) {
                        //get and set stream id for next request.
                        if (txnScanResponse.getStreamMeta() != null) {
                            this.streamId = txnScanResponse.getStreamMeta().getStreamId();
                            keyValues = Optional.ofNullable(
                                txnScanResponse.getKvs()).map(List::iterator).orElseGet(Collections::emptyIterator
                            );
                            hasMore = txnScanResponse.getStreamMeta().isHasMore();
                            if (hasMore) {
                                withStart = false;
                                range = new StoreInstance.Range(
                                    txnScanResponse.getEndKey(), range.end, withStart, range.withEnd
                                );
                            }
                        } else {
                            keyValues = Optional.ofNullable(txnScanResponse.getKvs())
                                .map(List::iterator).orElseGet(Collections::emptyIterator);
                            hasMore = false;
                            break;
                        }
                    }
                } catch (RequestErrorException e) {
                    if (e.getErrorCode() == 10118) {
                        //ESTREAM_EXPIRED: stream id is expired.
                        this.streamId = null;
                        LogUtils.info(log, "Stream id expired, info:{}", e.getMessage());
                    } else {
                        throw e;
                    }
                } catch (DingoClientException.InvalidRouteTableException e) {
                    LogUtils.error(log, e.getMessage() ,e);
                    throw e;
                }
                break;
            }
            long sub = System.currentTimeMillis() - start;
            DingoMetrics.timer("txnScanRpc").update(sub, TimeUnit.MILLISECONDS);
        }

        @Override
        public boolean hasNext() {
            while (hasMore && !keyValues.hasNext()) {
                if (rpcProfile.getStart() == 0) {
                    rpcProfile.start();
                }
                long start = System.currentTimeMillis();
                fetch();
                rpcProfile.time(start);
            }
            return keyValues.hasNext();
        }

        @Override
        public io.dingodb.common.store.KeyValue next() {
            return MAPPER.kvFrom(keyValues.next());
        }

        @Override
        public Profile getRpcProfile() {
            return rpcProfile;
        }

        @Override
        public Profile getInitRpcProfile() {
            return initRpcProfile;
        }
    }
}
