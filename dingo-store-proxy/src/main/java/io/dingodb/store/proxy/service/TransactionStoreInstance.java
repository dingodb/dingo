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
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Table;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.StoreService;
import io.dingodb.sdk.service.entity.common.KeyValue;
import io.dingodb.sdk.service.entity.store.AlreadyExist;
import io.dingodb.sdk.service.entity.store.LockInfo;
import io.dingodb.sdk.service.entity.store.Op;
import io.dingodb.sdk.service.entity.store.TxnBatchGetResponse;
import io.dingodb.sdk.service.entity.store.TxnBatchRollbackResponse;
import io.dingodb.sdk.service.entity.store.TxnCheckTxnStatusResponse;
import io.dingodb.sdk.service.entity.store.TxnCommitResponse;
import io.dingodb.sdk.service.entity.store.TxnHeartBeatRequest;
import io.dingodb.sdk.service.entity.store.TxnPessimisticLockResponse;
import io.dingodb.sdk.service.entity.store.TxnPessimisticRollbackResponse;
import io.dingodb.sdk.service.entity.store.TxnPrewriteResponse;
import io.dingodb.sdk.service.entity.store.TxnResolveLockResponse;
import io.dingodb.sdk.service.entity.store.TxnResultInfo;
import io.dingodb.sdk.service.entity.store.TxnScanRequest;
import io.dingodb.sdk.service.entity.store.TxnScanResponse;
import io.dingodb.sdk.service.entity.store.WriteConflict;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.checkstatus.TxnCheckStatus;
import io.dingodb.store.api.transaction.data.commit.TxnCommit;
import io.dingodb.store.api.transaction.data.pessimisticlock.TxnPessimisticLock;
import io.dingodb.store.api.transaction.data.prewrite.LockExtraDataList;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;
import io.dingodb.store.api.transaction.data.resolvelock.TxnResolveLock;
import io.dingodb.store.api.transaction.data.rollback.TxnBatchRollBack;
import io.dingodb.store.api.transaction.data.rollback.TxnPessimisticRollBack;
import io.dingodb.store.api.transaction.exception.DuplicateEntryException;
import io.dingodb.store.api.transaction.exception.PrimaryMismatchException;
import io.dingodb.store.api.transaction.exception.WriteConflictException;
import io.dingodb.store.proxy.Configuration;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.StringJoiner;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.dingodb.store.proxy.mapper.Mapper.MAPPER;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public class TransactionStoreInstance {

    private final StoreService storeService;
    private final CommonId partitionId;

    public TransactionStoreInstance(StoreService storeService, CommonId partitionId) {
        this.storeService = storeService;
        this.partitionId = partitionId;
    }

    private byte[] setId(byte[] key) {
        return CodecService.getDefault().setId(key, partitionId);
    }

    public void heartbeat(TxnPreWrite txnPreWrite) {
        storeService.txnHeartBeat(TxnHeartBeatRequest.builder()
            .primaryLock(txnPreWrite.getPrimaryLock())
            .startTs(txnPreWrite.getStartTs())
            .adviseLockTtl(TsoService.INSTANCE.timestamp() + SECONDS.toMillis(5))
            .build()
        );
    }

    public void heartbeat(TxnPessimisticLock txnPessimisticLock) {
        storeService.txnHeartBeat(TxnHeartBeatRequest.builder()
            .primaryLock(txnPessimisticLock.getPrimaryLock())
            .startTs(txnPessimisticLock.getStartTs())
            .adviseLockTtl(TsoService.INSTANCE.timestamp() + SECONDS.toMillis(5))
            .build()
        );
    }

    public boolean txnPreWrite(TxnPreWrite txnPreWrite) {
        txnPreWrite.getMutations().stream().peek($ -> $.setKey(setId($.getKey()))).forEach($ -> $.getKey()[0] = 't');
        int retry = 30;
        IsolationLevel isolationLevel = txnPreWrite.getIsolationLevel();
        while (retry-- > 0) {
            TxnPrewriteResponse response = storeService.txnPrewrite(MAPPER.preWriteTo(txnPreWrite));
            if (response.getKeysAlreadyExist() != null && response.getKeysAlreadyExist().size() > 0) {
                getJoinedPrimaryKey(txnPreWrite, response.getKeysAlreadyExist());
            }
            if (response.getTxnResult() == null || response.getTxnResult().isEmpty()) {
                return true;
            }
            resolveConflict(response.getTxnResult(), isolationLevel.getCode(), txnPreWrite.getStartTs(), "txnPreWrite");
        }
        return false;
    }

    // Join primary key values to string by mapping
    private String joinPrimaryKey(Object[] keyValues, TupleMapping mapping) {

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

    public Future txnPreWritePrimaryKey(TxnPreWrite txnPreWrite) {
        if (txnPreWrite(txnPreWrite)) {
            return Executors.scheduleWithFixedDelayAsync("txn-heartbeat", () -> heartbeat(txnPreWrite), 1, 1, SECONDS);
        }
        throw new WriteConflictException();
    }

    public boolean txnCommit(TxnCommit txnCommit) {
        txnCommit.getKeys().stream().peek($ -> setId($)).forEach($ -> $[0] = 't');
        TxnCommitResponse response = storeService.txnCommit(MAPPER.commitTo(txnCommit));
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
        long start = System.currentTimeMillis();
        while (true) {
            TxnPessimisticLockResponse response = storeService.txnPessimisticLock(MAPPER.pessimisticLockTo(txnPessimisticLock));
            if (response.getTxnResult() == null || response.getTxnResult().isEmpty()) {
                return true;
            }
            long elapsed = System.currentTimeMillis() - start;
            if (elapsed > timeOut) {
                throw new RuntimeException("Lock wait timeout exceeded; try restarting transaction");
            }
            resolveConflict(response.getTxnResult(), isolationLevel.getCode(), txnPessimisticLock.getStartTs(), "txnPessimisticLock");
            long forUpdateTs = TsoService.INSTANCE.tso();
            txnPessimisticLock.setForUpdateTs(forUpdateTs);
        }
    }

    public boolean txnPessimisticLockRollback(TxnPessimisticRollBack txnPessimisticRollBack) {
        txnPessimisticRollBack.getKeys().stream().peek($ -> setId($)).forEach($ -> $[0] = 't');
        TxnPessimisticRollbackResponse response = storeService.txnPessimisticRollback(MAPPER.pessimisticRollBackTo(txnPessimisticRollBack));
        if (response.getTxnResult() != null && response.getTxnResult().size() > 0) {
            resolveConflict(response.getTxnResult(), txnPessimisticRollBack.getIsolationLevel().getCode(), txnPessimisticRollBack.getStartTs(), "txnPessimisticLockRollback");
        }
        return response.getTxnResult() == null;
    }

    public Iterator<io.dingodb.common.store.KeyValue> txnScan(long ts, StoreInstance.Range range) {
        setId(range.start);
        setId(range.end);
        return new ScanIterator(ts, range);
    }

    List<io.dingodb.common.store.KeyValue> txnGet(long startTs, List<byte[]> keys) {
        keys.forEach(this::setId);
        int retry = 30;
        while (retry-- > 0) {
            TxnBatchGetResponse response = storeService.txnBatchGet(MAPPER.batchGetTo(startTs, IsolationLevel.SnapshotIsolation, keys));
            if (response.getTxnResult() == null) {
                return response.getKvs().stream().map(MAPPER::kvFrom).collect(Collectors.toList());
            }
            throw new RuntimeException("Txn get conflict startTs:" + startTs);
//            resolveConflict(singletonList(response.getTxnResult()), IsolationLevel., startTs , "txnGet");
        }
        throw new RuntimeException("Txn get conflict startTs:" + startTs);
    }

    public boolean txnBatchRollback(TxnBatchRollBack txnBatchRollBack) {
        txnBatchRollBack.getKeys().stream().peek($ -> setId($)).forEach($ -> $[0] = 't');
        TxnBatchRollbackResponse response = storeService.txnBatchRollback(MAPPER.rollbackTo(txnBatchRollBack));
        return response.getTxnResult() == null;
    }

    public TxnCheckTxnStatusResponse txnCheckTxnStatus(TxnCheckStatus txnCheckStatus) {
        byte[] primaryKey = txnCheckStatus.getPrimaryKey();
        StoreService storeService = Services.storeRegionService(Configuration.coordinatorSet(), primaryKey, 30);
        return storeService.txnCheckTxnStatus(MAPPER.checkTxnTo(txnCheckStatus));
    }

    public TxnResolveLockResponse txnResolveLock(TxnResolveLock txnResolveLock) {
        return storeService.txnResolveLock(MAPPER.resolveTxnTo(txnResolveLock));
    }

    private void resolveConflict(List<TxnResultInfo> txnResult, int isolationLevel, long startTs, String funName) {
        for (TxnResultInfo txnResultInfo : txnResult) {
            log.info("{} txnResultInfo : {}", funName, txnResultInfo);
            LockInfo lockInfo = txnResultInfo.getLocked();
            if (lockInfo != null) {
                // CheckTxnStatus
                log.info("{} lockInfo : {}", funName,lockInfo);
                // pessimistic lock
                if (lockInfo.getLockType() == Op.Lock && lockInfo.getForUpdateTs() != 0) {
                    TxnPessimisticRollBack pessimisticRollBack = TxnPessimisticRollBack.builder().
                        isolationLevel(IsolationLevel.of(isolationLevel))
                        .startTs(lockInfo.getLockTs())
                        .forUpdateTs(lockInfo.getForUpdateTs())
                        .keys(Collections.singletonList(lockInfo.getKey()))
                        .build();
                    txnPessimisticLockRollback(pessimisticRollBack);
                } else {
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
                    if (resultInfo == null) {
                        long lockTtl = statusResponse.getLockTtl();
                        long commitTs = statusResponse.getCommitTs();
                        if (lockTtl > 0) {
                            // wait
                            try {
                                Thread.sleep(lockTtl);
                                log.info("{} lockInfo wait {} ms end.", funName, lockTtl);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
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
                        }
                    } else {
                        // 1、PrimaryMismatch  or  TxnNotFound
                        if (resultInfo.getPrimaryMismatch() != null) {
                            throw new PrimaryMismatchException(resultInfo.getPrimaryMismatch().toString());
                        } else if (resultInfo.getTxnNotFound() != null) {
                            throw new RuntimeException(resultInfo.getTxnNotFound().toString());
                        } else if (resultInfo.getLocked() != null) {
                            throw new RuntimeException(resultInfo.getLocked().toString());
                        }
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
    }

    public class ScanIterator implements Iterator<io.dingodb.common.store.KeyValue> {
        private final long startTs;
        private final StoreInstance.Range range;

        private boolean withStart;
        private boolean hasMore = true;
        private StoreInstance.Range current;
        private Iterator<KeyValue> keyValues;

        public ScanIterator(long startTs, StoreInstance.Range range) {
            this.startTs = startTs;
            this.range = range;
            this.current = range;
            this.withStart = range.withStart;
            fetch();
        }

        private void fetch() {
            if (!hasMore) {
                return;
            }
            int retry = 30;
            while (retry-- > 0) {
                TxnScanRequest txnScanRequest = MAPPER.scanTo(startTs, IsolationLevel.SnapshotIsolation, current);
                txnScanRequest.setLimit(1024);
                TxnScanResponse txnScanResponse = storeService.txnScan(txnScanRequest);
                if (txnScanResponse.getTxnResult() != null) {
                    throw new RuntimeException("Txn get conflict startTs:" + startTs);
                }
                keyValues = Optional.ofNullable(txnScanResponse.getKvs()).map(List::iterator).orElseGet(Collections::emptyIterator);
                hasMore = txnScanResponse.isHasMore();
                if (hasMore) {
                    current = new StoreInstance.Range(txnScanResponse.getEndKey(), range.end, withStart, range.withEnd);
                }
                withStart = false;
                break;
            }
        }

        @Override
        public boolean hasNext() {
            if (keyValues.hasNext()) {
                return true;
            }
            if (hasMore) {
                fetch();
                return keyValues.hasNext();
            }
            return false;
        }

        @Override
        public io.dingodb.common.store.KeyValue next() {
            if (keyValues.hasNext()) {
                return MAPPER.kvFrom(keyValues.next());
            }
            if (hasMore) {
                fetch();
                return MAPPER.kvFrom(keyValues.next());
            }
            throw new NoSuchElementException();
        }
    }

}
