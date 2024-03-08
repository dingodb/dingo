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
import io.dingodb.common.type.TupleMapping;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Table;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.service.IndexService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.StoreService;
import io.dingodb.sdk.service.entity.common.KeyValue;
import io.dingodb.sdk.service.entity.store.Action;
import io.dingodb.sdk.service.entity.store.AlreadyExist;
import io.dingodb.sdk.service.entity.store.LockInfo;
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
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.TxnVariables;
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
import io.dingodb.store.api.transaction.exception.PrimaryMismatchException;
import io.dingodb.store.api.transaction.exception.WriteConflictException;
import io.dingodb.store.proxy.Configuration;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.dingodb.store.proxy.mapper.Mapper.MAPPER;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public class TransactionStoreInstance {

    private final StoreService storeService;
    private final IndexService indexService;
    private final CommonId partitionId;

    private final static int VectorKeyLen = 17;

    public TransactionStoreInstance(StoreService storeService, IndexService indexService, CommonId partitionId) {
        this.storeService = storeService;
        this.partitionId = partitionId;
        this.indexService = indexService;
    }

    private byte[] setId(byte[] key) {
        return CodecService.getDefault().setId(key, partitionId);
    }

    public void heartbeat(TxnPreWrite txnPreWrite) {
        TxnHeartBeatRequest request = TxnHeartBeatRequest.builder()
            .primaryLock(txnPreWrite.getPrimaryLock())
            .startTs(txnPreWrite.getStartTs())
            .adviseLockTtl(TsoService.INSTANCE.timestamp() + SECONDS.toMillis(5))
            .build();
        if (indexService != null) {
            indexService.txnHeartBeat(request);
        } else {
            storeService.txnHeartBeat(request);
        }
    }

    public void heartbeat(TxnPessimisticLock txnPessimisticLock) {
        TxnHeartBeatRequest request = TxnHeartBeatRequest.builder()
            .primaryLock(txnPessimisticLock.getPrimaryLock())
            .startTs(txnPessimisticLock.getStartTs())
            .adviseLockTtl(TsoService.INSTANCE.timestamp() + SECONDS.toMillis(5))
            .build();
        if (indexService != null) {
            indexService.txnHeartBeat(request);
        } else {
            storeService.txnHeartBeat(request);
        }
    }

    public boolean txnPreWrite(TxnPreWrite txnPreWrite, long timeOut) {
        txnPreWrite.getMutations().stream().peek($ -> $.setKey(setId($.getKey()))).forEach($ -> $.getKey()[0] = 't');
        int n = 1;
        long start = System.currentTimeMillis();
        IsolationLevel isolationLevel = txnPreWrite.getIsolationLevel();
        List<Long> resolvedLocks = new ArrayList<>();
        while (true) {
            TxnPrewriteRequest request = MAPPER.preWriteTo(txnPreWrite);
            TxnPrewriteResponse response;
            if (request.getMutations().get(0).getVector() == null) {
                response = storeService.txnPrewrite(request);
            } else {
                response = indexService.txnPrewrite(request);
            }
            if (response.getKeysAlreadyExist() != null && response.getKeysAlreadyExist().size() > 0) {
                getJoinedPrimaryKey(txnPreWrite, response.getKeysAlreadyExist());
            }
            if (response.getTxnResult() == null || response.getTxnResult().isEmpty()) {
                return true;
            }
            long elapsed = System.currentTimeMillis() - start;
            if (elapsed > timeOut) {
                throw new RuntimeException("startTs:" + txnPreWrite.getStartTs() + " resolve lock timeout");
            }
            ResolveLockStatus resolveLockStatus = writeResolveConflict(
                response.getTxnResult(),
                isolationLevel.getCode(),
                txnPreWrite.getStartTs(),
                resolvedLocks,
                "txnPreWrite"
            );
            if (resolveLockStatus == ResolveLockStatus.LOCK_TTL) {
                try {
                    long lockTtl = TxnVariables.WaitFixTime;
                    if (n < TxnVariables.WaitFixNum) {
                        lockTtl = TxnVariables.WaitTime * n;
                    }
                    Thread.sleep(lockTtl);
                    n++;
                    log.info("txnPreWrite lockInfo wait {} ms end.", lockTtl);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    // Join primary key values to string by mapping
    private String joinPrimaryKey(Object[] keyValues, TupleMapping mapping) {

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

    private String joinPrimaryKeys(String key1, String key2) {
        StringJoiner joiner = new StringJoiner(",");
        if (!key1.equals("")) {
            joiner.add(key1);
        }
        if (!key2.equals("")) {
            joiner.add(key2);
        }
        return joiner.toString();
    }

    public void getJoinedPrimaryKey(TxnPreWrite txnPreWrite, List<AlreadyExist> keysAlreadyExist) {
        CommonId tableId = LockExtraDataList.decode(txnPreWrite.getLockExtraDatas().get(0).getExtraData()).getTableId();
        Table table = MetaService.root().getTable(tableId);
        KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(table.tupleType(), table.keyMapping());
        AtomicReference<String> joinedKey = new AtomicReference<>("");
        TupleMapping keyMapping = table.keyMapping();
        keysAlreadyExist.stream().forEach(
            i -> {
                Optional.ofNullable(codec.decodeKeyPrefix(i.getKey()))
                    .ifPresent(keyValues -> {
                        joinedKey.set(joinPrimaryKeys(joinedKey.get(), joinPrimaryKey(keyValues, keyMapping)));
                    });
            }
        );
        throw new DuplicateEntryException("Duplicate entry " + joinedKey.get() + " for key '" + table.getName() + ".PRIMARY'");
    }

    public Future txnPreWritePrimaryKey(TxnPreWrite txnPreWrite, long timeOut) {
        if (txnPreWrite(txnPreWrite, timeOut)) {
            return Executors.scheduleWithFixedDelayAsync("txn-heartbeat", () -> heartbeat(txnPreWrite), 1, 1, SECONDS);
        }
        throw new WriteConflictException();
    }

    public boolean txnCommit(TxnCommit txnCommit) {
        txnCommit.getKeys().stream().peek($ -> setId($)).forEach($ -> $[0] = 't');
        TxnCommitResponse response;
        if (indexService != null) {
            response = indexService.txnCommit(MAPPER.commitTo(txnCommit));
        } else {
            response = storeService.txnCommit(MAPPER.commitTo(txnCommit));
        }
        if (response.getTxnResult() != null && response.getTxnResult().getCommitTsExpired() != null) {
            throw new CommitTsExpiredException(response.getTxnResult().getCommitTsExpired().toString());
        }
        return response.getTxnResult() == null;
    }

    public Future txnPessimisticLockPrimaryKey(TxnPessimisticLock txnPessimisticLock, long timeOut) {
        if (txnPessimisticLock(txnPessimisticLock, timeOut)) {
            return Executors.scheduleWithFixedDelayAsync("txn-pessimistic-heartbeat", () -> heartbeat(txnPessimisticLock), 1, 1, SECONDS);
        }
        throw new WriteConflictException();
    }

    public boolean txnPessimisticLock(TxnPessimisticLock txnPessimisticLock, long timeOut) {
        txnPessimisticLock.getMutations().stream().peek($ -> $.setKey(setId($.getKey()))).forEach($ -> $.getKey()[0] = 't');
        IsolationLevel isolationLevel = txnPessimisticLock.getIsolationLevel();
        int n = 1;
        long start = System.currentTimeMillis();
        List<Long> resolvedLocks = new ArrayList<>();
        while (true) {
            TxnPessimisticLockResponse response;
            if (indexService != null) {
                txnPessimisticLock.getMutations().stream().forEach( $ -> $.setKey(Arrays.copyOf($.getKey(), VectorKeyLen)));
                response = indexService.txnPessimisticLock(MAPPER.pessimisticLockTo(txnPessimisticLock));
            } else {
                response = storeService.txnPessimisticLock(MAPPER.pessimisticLockTo(txnPessimisticLock));
            }
            if (response.getTxnResult() == null || response.getTxnResult().isEmpty()) {
                return true;
            }
            long elapsed = System.currentTimeMillis() - start;
            if (elapsed > timeOut) {
                throw new RuntimeException("Lock wait timeout exceeded; try restarting transaction");
            }
            ResolveLockStatus resolveLockStatus = writeResolveConflict(
                response.getTxnResult(),
                isolationLevel.getCode(),
                txnPessimisticLock.getStartTs(),
                resolvedLocks,
                "txnPessimisticLock"
            );
            if (resolveLockStatus == ResolveLockStatus.LOCK_TTL) {
                try {
                    long lockTtl = TxnVariables.WaitFixTime;
                    if (n < TxnVariables.WaitFixNum) {
                        lockTtl = TxnVariables.WaitTime * n;
                    }
                    Thread.sleep(lockTtl);
                    n++;
                    log.info("txnPessimisticLock lockInfo wait {} ms end.", lockTtl);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            long forUpdateTs = TsoService.INSTANCE.tso();
            txnPessimisticLock.setForUpdateTs(forUpdateTs);
        }
    }

    public boolean txnPessimisticLockRollback(TxnPessimisticRollBack txnPessimisticRollBack) {
        txnPessimisticRollBack.getKeys().stream().peek($ -> setId($)).forEach($ -> $[0] = 't');
        TxnPessimisticRollbackResponse response;
        if (indexService != null) {
            List<byte[]> keys = txnPessimisticRollBack.getKeys();
            IntStream.range(0, keys.size())
                .forEach(i -> {
                    byte[] key = keys.get(i);
                    byte[] newKey = Arrays.copyOf(key, VectorKeyLen);
                    keys.set(i, newKey);
                });
            response = indexService.txnPessimisticRollback(MAPPER.pessimisticRollBackTo(txnPessimisticRollBack));
        } else {
            response = storeService.txnPessimisticRollback(MAPPER.pessimisticRollBackTo(txnPessimisticRollBack));
        }
        List<Long> resolvedLocks = new ArrayList<>();
        if (response.getTxnResult() != null && response.getTxnResult().size() > 0) {
//            writeResolveConflict(
//                response.getTxnResult(),
//                txnPessimisticRollBack.getIsolationLevel().getCode(),
//                txnPessimisticRollBack.getStartTs(),
//                resolvedLocks,
//                "txnPessimisticLockRollback"
//            );
        }
        return response.getTxnResult() == null;
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
        return new ScanIterator(ts, range, timeOut, coprocessor);
    }

    public List<io.dingodb.common.store.KeyValue> txnGet(long startTs, List<byte[]> keys, long timeOut) {
        keys.stream().peek($ -> setId($)).forEach($ -> $[0] = 't');
        int n = 1;
        long start = System.currentTimeMillis();
        List<Long> resolvedLocks = new ArrayList<>();
        while (true) {
            TxnBatchGetRequest txnBatchGetRequest = MAPPER.batchGetTo(startTs, IsolationLevel.SnapshotIsolation, keys);
            txnBatchGetRequest.setResolveLocks(resolvedLocks);
            TxnBatchGetResponse response;
            if (indexService != null) {
                txnBatchGetRequest.getKeys().stream().forEach( $ -> Arrays.copyOf($, VectorKeyLen));
                response = indexService.txnBatchGet(txnBatchGetRequest);
                if (response.getTxnResult() == null) {
                    return response.getVectors().stream()
                        .map(vectorWithId -> vectorWithId != null ?
                            new io.dingodb.common.store.KeyValue(vectorWithId.getTableData().getTableKey(),
                                vectorWithId.getTableData().getTableValue()) : null)
                        .collect(Collectors.toList());
                }
            } else {
                response = storeService.txnBatchGet(txnBatchGetRequest);
                if (response.getTxnResult() == null) {
                    return response.getKvs().stream().map(MAPPER::kvFrom).collect(Collectors.toList());
                }
            }
            long elapsed = System.currentTimeMillis() - start;
            if (elapsed > timeOut) {
                throw new RuntimeException("startTs:" + start + " resolve lock timeout");
            }
            ResolveLockStatus resolveLockStatus = readResolveConflict(
                singletonList(response.getTxnResult()),
                IsolationLevel.SnapshotIsolation.getCode(),
                startTs,
                resolvedLocks,
                "txnScan"
            );
            if (resolveLockStatus == ResolveLockStatus.LOCK_TTL) {
                try {
                    long lockTtl = TxnVariables.WaitFixTime;
                    if (n < TxnVariables.WaitFixNum) {
                        lockTtl = TxnVariables.WaitTime * n;
                    }
                    Thread.sleep(lockTtl);
                    n++;
                    log.info("txnBatchGet lockInfo wait {} ms end.", lockTtl);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public boolean txnBatchRollback(TxnBatchRollBack txnBatchRollBack) {
        txnBatchRollBack.getKeys().stream().peek($ -> setId($)).forEach($ -> $[0] = 't');
        TxnBatchRollbackResponse response;
        if (indexService != null) {
            txnBatchRollBack.getKeys().stream().forEach( $ -> Arrays.copyOf($, VectorKeyLen));
            response = indexService.txnBatchRollback(MAPPER.rollbackTo(txnBatchRollBack));
        } else {
            response = storeService.txnBatchRollback(MAPPER.rollbackTo(txnBatchRollBack));
        }
        return response.getTxnResult() == null;
    }

    public TxnCheckTxnStatusResponse txnCheckTxnStatus(TxnCheckStatus txnCheckStatus) {
        byte[] primaryKey = txnCheckStatus.getPrimaryKey();
        StoreService storeService = Services.storeRegionService(Configuration.coordinatorSet(), primaryKey, 30);
        return storeService.txnCheckTxnStatus(MAPPER.checkTxnTo(txnCheckStatus));
    }

    public TxnResolveLockResponse txnResolveLock(TxnResolveLock txnResolveLock) {
        if (indexService != null) {
            return indexService.txnResolveLock(MAPPER.resolveTxnTo(txnResolveLock));
        }
        return storeService.txnResolveLock(MAPPER.resolveTxnTo(txnResolveLock));
    }

    private ResolveLockStatus writeResolveConflict(List<TxnResultInfo> txnResult, int isolationLevel,
                                                   long startTs, List<Long> resolvedLocks, String funName) {
        ResolveLockStatus resolveLockStatus = ResolveLockStatus.NONE;
        for (TxnResultInfo txnResultInfo : txnResult) {
            log.info("{} txnResultInfo : {}", funName, txnResultInfo);
            LockInfo lockInfo = txnResultInfo.getLocked();
            if (lockInfo != null) {
                // CheckTxnStatus
                log.info("{} lockInfo : {}", funName, lockInfo);
                long currentTs = TsoService.INSTANCE.tso();
                TxnCheckStatus txnCheckStatus = TxnCheckStatus.builder().
                    isolationLevel(IsolationLevel.of(isolationLevel)).
                    primaryKey(lockInfo.getPrimaryLock()).
                    lockTs(lockInfo.getLockTs()).
                    callerStartTs(startTs).
                    currentTs(currentTs).
                    build();
                TxnCheckTxnStatusResponse statusResponse = txnCheckTxnStatus(txnCheckStatus);
                log.info("{} txnCheckStatus : {}", funName, statusResponse);
                TxnResultInfo resultInfo = statusResponse.getTxnResult();
                // success
                Action action = statusResponse.getAction();
                if (resultInfo == null) {
                    long lockTtl = statusResponse.getLockTtl();
                    long commitTs = statusResponse.getCommitTs();
                    if (lockInfo.getLockType() == Op.Lock && lockInfo.getForUpdateTs() != 0
                        && (action == Action.LockNotExistRollback
                        || action == Action.TTLExpirePessimisticRollback
                        || action == Action.TTLExpireRollback)) {
                        // pessimistic lock
                        TxnPessimisticRollBack pessimisticRollBack = TxnPessimisticRollBack.builder().
                            isolationLevel(IsolationLevel.of(isolationLevel))
                            .startTs(lockInfo.getLockTs())
                            .forUpdateTs(lockInfo.getForUpdateTs())
                            .keys(Collections.singletonList(lockInfo.getKey()))
                            .build();
                        txnPessimisticLockRollback(pessimisticRollBack);
                        resolveLockStatus = ResolveLockStatus.PESSIMISTIC_ROLLBACK;
                    } else if (lockTtl > 0) {
                        // wait
                        resolveLockStatus = ResolveLockStatus.LOCK_TTL;
                    } else if (commitTs > 0) {
                        // resolveLock store commit
                        TxnResolveLock resolveLockRequest = TxnResolveLock.builder().
                            isolationLevel(IsolationLevel.of(isolationLevel)).
                            startTs(lockInfo.getLockTs()).
                            commitTs(commitTs).
                            keys(singletonList(lockInfo.getKey())).
                            build();
                        TxnResolveLockResponse txnResolveLockRes = txnResolveLock(resolveLockRequest);
                        log.info("{} txnResolveLockResponse: {}", funName, txnResolveLockRes);
                        resolveLockStatus = ResolveLockStatus.COMMIT;
                    } else if (lockTtl == 0 && commitTs == 0) {
                        // resolveLock store rollback
                        TxnResolveLock resolveLockRequest = TxnResolveLock.builder().
                            isolationLevel(IsolationLevel.of(isolationLevel)).
                            startTs(lockInfo.getLockTs()).
                            commitTs(commitTs).
                            keys(singletonList(lockInfo.getKey())).
                            build();
                        TxnResolveLockResponse txnResolveLockRes = txnResolveLock(resolveLockRequest);
                        log.info("{} txnResolveLockResponse: {}", funName, txnResolveLockRes);
                        resolveLockStatus = ResolveLockStatus.ROLLBACK;
                    }
                } else {
                    lockInfo = resultInfo.getLocked();
                    if (lockInfo != null) {
                        // pessimistic lock
                        if (lockInfo.getLockType() == Op.Lock && lockInfo.getForUpdateTs() != 0) {
                            if (action == Action.LockNotExistRollback
                                || action == Action.TTLExpirePessimisticRollback
                                || action == Action.TTLExpireRollback) {
                                TxnPessimisticRollBack pessimisticRollBack = TxnPessimisticRollBack.builder().
                                    isolationLevel(IsolationLevel.of(isolationLevel))
                                    .startTs(lockInfo.getLockTs())
                                    .forUpdateTs(lockInfo.getForUpdateTs())
                                    .keys(Collections.singletonList(lockInfo.getKey()))
                                    .build();
                                txnPessimisticLockRollback(pessimisticRollBack);
                                resolveLockStatus = ResolveLockStatus.PESSIMISTIC_ROLLBACK;
                            } else {
                                resolveLockStatus = ResolveLockStatus.LOCK_TTL;
                            }
                            continue;
                        }
                    }
                    // 1、PrimaryMismatch  or  TxnNotFound
                    if (resultInfo.getPrimaryMismatch() != null) {
                        throw new PrimaryMismatchException(resultInfo.getPrimaryMismatch().toString());
                    } else if (resultInfo.getTxnNotFound() != null) {
                        throw new RuntimeException(resultInfo.getTxnNotFound().toString());
                    } else if (resultInfo.getLocked() != null) {
                        throw new RuntimeException(resultInfo.getLocked().toString());
                    }
                }
            } else {
                WriteConflict writeConflict = txnResultInfo.getWriteConflict();
                log.info("{} writeConflict : {}", funName, writeConflict);
                if (writeConflict != null) {
                    //  write column exist and commit_ts > for_update_ts
                    if (funName.equalsIgnoreCase("txnPessimisticLock")) {
                        continue;
                    }
                    throw new WriteConflictException(writeConflict.toString());
                }
            }
        }
        return resolveLockStatus;
    }

    private ResolveLockStatus readResolveConflict(List<TxnResultInfo> txnResult, int isolationLevel,
                                                  long startTs, List<Long> resolvedLocks, String funName) {
        ResolveLockStatus resolveLockStatus = ResolveLockStatus.NONE;
        for (TxnResultInfo txnResultInfo : txnResult) {
            log.info("{} txnResultInfo : {}", funName, txnResultInfo);
            LockInfo lockInfo = txnResultInfo.getLocked();
            if (lockInfo != null) {
                // CheckTxnStatus
                log.info("{} lockInfo : {}", funName, lockInfo);
                long currentTs = TsoService.INSTANCE.tso();
                TxnCheckStatus txnCheckStatus = TxnCheckStatus.builder().
                    isolationLevel(IsolationLevel.of(isolationLevel)).
                    primaryKey(lockInfo.getPrimaryLock()).
                    lockTs(lockInfo.getLockTs()).
                    callerStartTs(startTs).
                    currentTs(currentTs).
                    build();
                TxnCheckTxnStatusResponse statusResponse = txnCheckTxnStatus(txnCheckStatus);
                log.info("{} txnCheckStatus : {}", funName, statusResponse);
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
                        TxnPessimisticRollBack pessimisticRollBack = TxnPessimisticRollBack.builder().
                            isolationLevel(IsolationLevel.of(isolationLevel))
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
                                    resolvedLocks.add(currentTs);
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
                        TxnResolveLock resolveLockRequest = TxnResolveLock.builder().
                            isolationLevel(IsolationLevel.of(isolationLevel)).
                            startTs(lockInfo.getLockTs()).
                            commitTs(commitTs).
                            keys(singletonList(lockInfo.getKey())).
                            build();
                        TxnResolveLockResponse txnResolveLockRes = txnResolveLock(resolveLockRequest);
                        log.info("{} txnResolveLockResponse: {}", funName, txnResolveLockRes);
                        resolveLockStatus = ResolveLockStatus.COMMIT;
                    } else if (lockTtl == 0 && commitTs == 0) {
                        // resolveLock store rollback
                        TxnResolveLock resolveLockRequest = TxnResolveLock.builder().
                            isolationLevel(IsolationLevel.of(isolationLevel)).
                            startTs(lockInfo.getLockTs()).
                            commitTs(commitTs).
                            keys(singletonList(lockInfo.getKey())).
                            build();
                        TxnResolveLockResponse txnResolveLockRes = txnResolveLock(resolveLockRequest);
                        log.info("{} txnResolveLockResponse: {}", funName, txnResolveLockRes);
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
                            TxnPessimisticRollBack pessimisticRollBack = TxnPessimisticRollBack.builder().
                                isolationLevel(IsolationLevel.of(isolationLevel))
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
                        throw new RuntimeException(resultInfo.getTxnNotFound().toString());
                    } else if (resultInfo.getLocked() != null) {
                        throw new RuntimeException(resultInfo.getLocked().toString());
                    }
                }
            } else {
                WriteConflict writeConflict = txnResultInfo.getWriteConflict();
                log.info("{} writeConflict : {}", funName, writeConflict);
                if (writeConflict != null) {
                    throw new WriteConflictException(writeConflict.toString());
                }
            }
        }
        return resolveLockStatus;
    }

    public class ScanIterator implements Iterator<io.dingodb.common.store.KeyValue> {
        private final long startTs;
        private final StoreInstance.Range range;
        private final long timeOut;
        private final io.dingodb.sdk.service.entity.common.CoprocessorV2 coprocessor;

        private boolean withStart;
        private boolean hasMore = true;
        private StoreInstance.Range current;
        private Iterator<KeyValue> keyValues;

        public ScanIterator(long startTs, StoreInstance.Range range, long timeOut) {
            this(startTs, range, timeOut, null);
        }

        public ScanIterator(long startTs, StoreInstance.Range range, long timeOut, CoprocessorV2 coprocessor) {
            this.startTs = startTs;
            this.range = range;
            this.current = range;
            this.withStart = range.withStart;
            this.timeOut = timeOut;
            this.coprocessor = MAPPER.coprocessorTo(coprocessor);
            Optional.ofNullable(this.coprocessor)
                .map(io.dingodb.sdk.service.entity.common.CoprocessorV2::getOriginalSchema)
                .ifPresent($ -> $.setCommonId(partitionId.seq));
            Optional.ofNullable(this.coprocessor)
                .map(io.dingodb.sdk.service.entity.common.CoprocessorV2::getResultSchema)
                .ifPresent($ -> $.setCommonId(partitionId.seq));
            fetch();
        }

        private synchronized void fetch() {
            if (!hasMore) {
                return;
            }
            int n = 1;
            long start = System.currentTimeMillis();
            List<Long> resolvedLocks = new ArrayList<>();
            while (true) {
                TxnScanRequest txnScanRequest = MAPPER.scanTo(startTs, IsolationLevel.SnapshotIsolation, current);
                txnScanRequest.setLimit(1024);
                txnScanRequest.setResolveLocks(resolvedLocks);
                txnScanRequest.setCoprocessor(coprocessor);
                TxnScanResponse txnScanResponse;
                if (indexService != null) {
                    txnScanResponse = indexService.txnScan(TsoService.INSTANCE.tso(), txnScanRequest);
                } else {
                    txnScanResponse = storeService.txnScan(TsoService.INSTANCE.tso(), txnScanRequest);
                }
                if (txnScanResponse.getTxnResult() != null) {
                    long elapsed = System.currentTimeMillis() - start;
                    if (elapsed > timeOut) {
                        throw new RuntimeException("startTs:" + txnScanRequest.getStartTs() + " resolve lock timeout");
                    }
                    ResolveLockStatus resolveLockStatus = readResolveConflict(
                        singletonList(txnScanResponse.getTxnResult()),
                        IsolationLevel.SnapshotIsolation.getCode(),
                        startTs,
                        resolvedLocks,
                        "txnScan"
                    );
                    if (resolveLockStatus == ResolveLockStatus.LOCK_TTL) {
                        try {
                            long lockTtl = TxnVariables.WaitFixTime;
                            if (n < TxnVariables.WaitFixNum) {
                                lockTtl = TxnVariables.WaitTime * n;
                            }
                            Thread.sleep(lockTtl);
                            n++;
                            log.info("txnScan lockInfo wait {} ms end.", lockTtl);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    continue;
                }
                keyValues = Optional.ofNullable(txnScanResponse.getKvs()).map(List::iterator).orElseGet(Collections::emptyIterator);
                hasMore = txnScanResponse.isHasMore();
                if (hasMore) {
                    withStart = false;
                    current = new StoreInstance.Range(txnScanResponse.getEndKey(), range.end, withStart, range.withEnd);
                }
                break;
            }
        }

        @Override
        public boolean hasNext() {
            while (hasMore && !keyValues.hasNext()) {
                fetch();
            }
            return keyValues.hasNext();
        }

        @Override
        public io.dingodb.common.store.KeyValue next() {
            return MAPPER.kvFrom(keyValues.next());
        }
    }

}
