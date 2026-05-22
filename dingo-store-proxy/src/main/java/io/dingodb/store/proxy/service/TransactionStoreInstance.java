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
import io.dingodb.common.profile.RpcProfile;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.Pair;
import io.dingodb.common.util.Utils;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.meta.entity.Table;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.DingoClientException.RequestErrorException;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.service.DocumentService;
import io.dingodb.sdk.service.IndexService;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.StoreService;
import io.dingodb.sdk.service.entity.common.Document;
import io.dingodb.sdk.service.entity.common.DocumentWithScore;
import io.dingodb.sdk.service.entity.common.KeyValue;
import io.dingodb.sdk.service.entity.common.TableData;
import io.dingodb.sdk.service.entity.document.DocumentSearchAllRequest;
import io.dingodb.sdk.service.entity.document.DocumentSearchAllResponse;
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
import io.dingodb.sdk.service.entity.store.TxnScanEntry;
import io.dingodb.sdk.service.entity.store.TxnScanRequest;
import io.dingodb.sdk.service.entity.store.TxnScanResponse;
import io.dingodb.sdk.service.entity.store.WriteConflict;
import io.dingodb.sdk.service.entity.stream.StreamRequestMeta;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.ProfileScanIterator;
import io.dingodb.store.api.transaction.data.DocumentSearchParameter;
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
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import io.dingodb.store.api.transaction.exception.WriteConflictException;
import io.dingodb.store.proxy.Configuration;
import io.dingodb.store.proxy.common.transaction.ResolveLockResult;
import io.dingodb.store.proxy.common.transaction.ResolveLocksOptions;
import io.dingodb.store.proxy.common.transaction.TxnExpireTime;
import io.dingodb.store.proxy.common.transaction.TxnStatus;
import io.dingodb.store.utils.ResolveLockUtil;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.dingodb.store.proxy.mapper.Mapper.MAPPER;
import static io.dingodb.store.utils.ResolveLockUtil.checkSecondaryAllLocks;
import static io.dingodb.store.utils.ResolveLockUtil.extractLockInfos;
import static io.dingodb.store.utils.ResolveLockUtil.resolveAsyncCommitLock;
import static io.dingodb.store.utils.ResolveLockUtil.resolveAsyncResolveData;
import static io.dingodb.store.utils.ResolveLockUtil.txnCheckTxnStatus;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public class TransactionStoreInstance {

    private static final int LOCK_COLLECTION_REFILL_BATCH_DIVISOR = 4;

    private final StoreService storeService;
    private final IndexService indexService;
    private final CommonId partitionId;
    private final DocumentService documentService;

    private static final int VectorKeyLen = 17;

    private final Map<Long, List<List<LockInfo>>> resolvingLocks = new ConcurrentHashMap<>();
    private final Map<Long, Integer> resolvingConcurrency = new ConcurrentHashMap<>();

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

    private class IteratorProxy implements InvocationHandler {

        private final Iterator iterator;

        private IteratorProxy(Iterator iterator) {
            this.iterator = iterator;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            try {
                return method.invoke(iterator, args);
            } catch (Exception e) {
                Throwable throwable = Utils.extractThrowable(e);
                if (throwable instanceof DingoClientException.InvalidRouteTableException) {
                    throw new RegionSplitException(throwable);
                }
                throw throwable;
            }
        }
    }

    private byte[] setId(byte[] key) {
        return CodecService.getDefault().setId(key, partitionId);
    }

    public void heartbeat(TxnPreWrite txnPreWrite) {
        LogUtils.info(log, "pre write optimistic heartbeat startTs:{}", txnPreWrite.getStartTs());
        heartBeat(txnPreWrite.getStartTs(), txnPreWrite.getPrimaryLock(), false);
    }

    public void heartbeat(TxnPessimisticLock txnPessimisticLock) {
        LogUtils.info(log, "pessimistic heartbeat startTs:{}", txnPessimisticLock.getStartTs());
        heartBeat(txnPessimisticLock.getStartTs(), txnPessimisticLock.getPrimaryLock(), true);
    }

    public void heartBeat(long startTs, byte[] primaryLock, boolean pessimistic) {
        Integer retry = io.dingodb.common.util.Optional.mapOrGet(
            DingoConfiguration.instance().find("retry", int.class),
            __ -> __,
            () -> 30
        );
        boolean getService = false;
        TxnHeartBeatRequest request = TxnHeartBeatRequest.builder()
            .primaryLock(primaryLock)
            .startTs(startTs)
            .adviseLockTtl(TsoService.INSTANCE.timestamp() + SECONDS.toMillis(TransactionUtil.heartBeatLockTtl))
            .build();
        while (retry-- > 0) {
            try {
                if (indexService != null) {
                    if (getService) {
                        Services.indexRegionService(
                                Configuration.coordinatorSet(),
                                primaryLock,
                                30)
                            .txnHeartBeat(startTs, request);
                    } else {
                        indexService.txnHeartBeat(request.getStartTs(), request);
                    }
                } else if (documentService != null) {
                    if (getService) {
                        Services.documentRegionService(
                                Configuration.coordinatorSet(),
                                primaryLock,
                                30)
                            .txnHeartBeat(startTs, request);
                    } else {
                        documentService.txnHeartBeat(request.getStartTs(), request);
                    }
                } else {
                    if (getService) {
                        Services.storeRegionService(
                                Configuration.coordinatorSet(),
                                primaryLock,
                                30)
                            .txnHeartBeat(startTs, request);
                    } else {
                        storeService.txnHeartBeat(request.getStartTs(), request);
                    }
                }
                break;
            } catch (RegionSplitException | DingoClientException.InvalidRouteTableException e) {
                LogUtils.error(log, e.getMessage(), e);
                getService = true;
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            } catch (Exception e) {
                LogUtils.error(log, "txn heartbeat, pessimistic:{}, startTs:{}, error:{}", pessimistic, startTs, e);
                throw e;
            }
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

                try {
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
                    ResolveLockStatus resolveLockStatus = resolveLockConflictNew(
                        response.getTxnResult(),
                        isolationLevel.getCode(),
                        startTs,
                        resolvedLocks,
                        "txnPreWrite",
                        false,
                        txnPreWrite.getPessimisticChecks().isEmpty()
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
                    } else if (resolveLockStatus ==  ResolveLockStatus.UNKNOWN) {
                        throw new RuntimeException("startTs:" + startTs + " resolve lock status is unknown");
                    }
                } catch (RequestErrorException e) {
                    if ((request.isTryOnePc() || request.isUseAsyncCommit()) &&
                        (e.getErrorCode() == 50002 || e.getErrorCode() == 50003)) {
                        LogUtils.error(log, "txnPreWrite not leader error:" + e.getMessage(), e);
                        if (timeOut < 0) {
                            throw new RuntimeException("startTs:" + startTs + " txnPreWrite not leader error:" + e);
                        }
                        try {
                            long lockTtl = TxnVariables.WaitFixTime;
                            if (n < TxnVariables.WaitFixNum) {
                                lockTtl = TxnVariables.WaitTime * n;
                            }
                            Thread.sleep(lockTtl);
                            n++;
                            timeOut -= lockTtl;
                            LogUtils.info(log, "txnPreWrite not leader error wait {} ms end.", lockTtl);
                        } catch (InterruptedException e1) {
                            throw new RuntimeException(e1);
                        }
                        long commitTs = TsoService.INSTANCE.tso();
                        LogUtils.info(log, "txnPreWrite not leader error retry commitTs:{}.", commitTs);
                        txnPreWrite.setMinCommitTs(commitTs);
                    } else {
                        LogUtils.error(log, "txnPreWrite Error:" + e.getMessage(), e);
                        throw e;
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
        if (table == null) {
            throw new DuplicateEntryException("Duplicate entry for meta key ");
        }
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
        DuplicateEntryException duplicateEntryException = new DuplicateEntryException("Duplicate entry "
            + joinedKey.get()
            + " for key '" + table.getName() + ".PRIMARY'");
        duplicateEntryException.keys = keysAlreadyExist.stream().map(AlreadyExist::getKey).collect(Collectors.toList());
        throw duplicateEntryException;
    }

    public Future<?> txnPreWritePrimaryKey(TxnPreWrite txnPreWrite, long timeOut) {
        if (txnPreWrite(txnPreWrite, timeOut)) {
            LogUtils.info(log, "txn heartbeat, startTs:{}", txnPreWrite.getStartTs());
            return Executors.scheduleWithFixedDelayAsync(
                "txn-heartbeat-" + txnPreWrite.getStartTs(),
                () -> heartbeat(txnPreWrite),
                5,
                10,
                SECONDS
            );
        }
        throw new WriteConflictException();
    }

    public Future<?> txnHeartBeat(long startTs, byte[] primaryLock) {
        LogUtils.info(log, "txn heartbeat, startTs:{}", startTs);
        primaryLock[0] = 't';
        return Executors.scheduleWithFixedDelayAsync(
            "txn-heartbeat-" + startTs,
            () -> heartBeat(startTs, primaryLock, false),
            5,
            10,
            SECONDS
        );
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
            if (response.getTxnResult() != null) {
                LogUtils.error(log, "commit failed, result:{}", response.getTxnResult());
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
                5,
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
                ResolveLockStatus resolveLockStatus = resolveLockConflictNew(
                    response.getTxnResult(),
                    isolationLevel.getCode(),
                    startTs,
                    resolvedLocks,
                    "txnPessimisticLock",
                    false,
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
                } else if (resolveLockStatus ==  ResolveLockStatus.UNKNOWN) {
                    throw new RuntimeException("startTs:" + startTs + " resolve lock status is unknown");
                }
                long forUpdateTs = TsoService.INSTANCE.tso();
                txnPessimisticLock.setForUpdateTs(forUpdateTs);
                txnPessimisticLock.setLockTtl(TransactionManager.lockTtlTm());
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
            Integer retry = io.dingodb.common.util.Optional.mapOrGet(
                DingoConfiguration.instance().find("retry", int.class),
                __ -> __,
                () -> 30
            );
            TxnPessimisticRollbackResponse response = null;
            boolean getService = false;
            while (retry-- > 0) {
                try {
                    if (indexService != null) {
                        List<byte[]> keys = txnPessimisticRollBack.getKeys();
                        List<byte[]> newKeys = keys.stream()
                            .map(key -> Arrays.copyOf(key, VectorKeyLen))
                            .collect(Collectors.toList());
                        txnPessimisticRollBack.setKeys(newKeys);
                        if (getService) {
                            response = Services.indexRegionService(
                                    Configuration.coordinatorSet(),
                                    txnPessimisticRollBack.getKeys().get(0),
                                    30)
                                .txnPessimisticRollback(txnPessimisticRollBack.getStartTs(),
                                    MAPPER.pessimisticRollBackTo(txnPessimisticRollBack));
                        } else {
                            response = indexService.txnPessimisticRollback(
                                startTs, MAPPER.pessimisticRollBackTo(txnPessimisticRollBack)
                            );
                        }
                    } else if (documentService != null) {
                        List<byte[]> keys = txnPessimisticRollBack.getKeys();
                        List<byte[]> newKeys = keys.stream()
                            .map(key -> Arrays.copyOf(key, VectorKeyLen))
                            .collect(Collectors.toList());
                        txnPessimisticRollBack.setKeys(newKeys);
                        if (getService) {
                            response = Services.documentRegionService(
                                Configuration.coordinatorSet(),
                                txnPessimisticRollBack.getKeys().get(0),
                                30)
                                .txnPessimisticRollback(txnPessimisticRollBack.getStartTs(),
                                    MAPPER.pessimisticRollBackTo(txnPessimisticRollBack));
                        } else {
                            response = documentService.txnPessimisticRollback(
                                startTs, MAPPER.pessimisticRollBackTo(txnPessimisticRollBack)
                            );
                        }
                    } else {
                        if (getService) {
                            response = Services.storeRegionService(
                                Configuration.coordinatorSet(),
                                txnPessimisticRollBack.getKeys().get(0),
                                30)
                                .txnPessimisticRollback(txnPessimisticRollBack.getStartTs(),
                                    MAPPER.pessimisticRollBackTo(txnPessimisticRollBack));
                        } else {
                            response = storeService.txnPessimisticRollback(
                                startTs, MAPPER.pessimisticRollBackTo(txnPessimisticRollBack)
                            );
                        }
                    }
                    break;
                } catch (RegionSplitException | DingoClientException.InvalidRouteTableException e) {
                    LogUtils.error(log, e.getMessage(), e);
                    getService = true;
                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
            if (response != null && response.getTxnResult() != null && !response.getTxnResult().isEmpty()) {
                LogUtils.error(log, "txnPessimisticLockRollback txnResult:{}",
                    response.getTxnResult().toString());
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
            return (Iterator<io.dingodb.common.store.KeyValue>) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class[]{Iterator.class},
                new IteratorProxy(getScanStreamIterator(ts, range, timeOut, coprocessor))
            );
        } else {
            return (Iterator<io.dingodb.common.store.KeyValue>) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class[]{Iterator.class},
                new IteratorProxy(getScanIterator(ts, range, timeOut, coprocessor))
            );
        }
    }

    public Pair<Iterator<io.dingodb.common.store.KeyValue>, RpcProfile> txnScanWithProfile(
        long ts,
        StoreInstance.Range range,
        long timeOut,
        CoprocessorV2 coprocessor
    ) {
        Stream.of(range.start).peek(this::setId).forEach($ -> $[0] = 't');
        Stream.of(range.end).peek(this::setId).forEach($ -> $[0] = 't');

        if (ScopeVariables.txnScanByStream()) {
            return Pair.of((Iterator<io.dingodb.common.store.KeyValue>) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class[]{Iterator.class},
                new IteratorProxy(getScanStreamIterator(ts, range, timeOut, coprocessor))
            ), null);
        } else {
            ScanIterator scanIterator = getScanIterator(ts, range, timeOut, coprocessor);
            return Pair.of((Iterator<io.dingodb.common.store.KeyValue>) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class[]{Iterator.class},
                new IteratorProxy(scanIterator)
            ), new RpcProfile(scanIterator.initRpcProfile, scanIterator.rpcProfile));
        }
    }

    public Pair<Iterator<io.dingodb.common.store.KeyValue>, RpcProfile> txnScanWithProfile(
        long ts, StoreInstance.Range range, long timeOut
    ) {
        return txnScanWithProfile(ts, range, timeOut, null);
    }

    public Iterator<io.dingodb.common.store.KeyValue> documentScanFilter(
        long ts,
        DocumentSearchParameter documentSearchParameter,
        long timeout
    ) {
        return getDocumentScanFilterStreamIterator(ts, documentSearchParameter, timeout);
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

    @NonNull
    public DocumentScanFilterStreamIterator getDocumentScanFilterStreamIterator(
        long ts, DocumentSearchParameter documentSearchParameter, long timeout
    ) {
        return new DocumentScanFilterStreamIterator(ts, documentSearchParameter, timeout);
    }

    public List<io.dingodb.common.store.KeyValue> txnGet(long startTs, List<byte[]> keys, long timeOut) {
        keys.stream().peek(this::setId).forEach($ -> $[0] = 't');
        return getKeyValues(startTs, keys, timeOut);
    }

    @NonNull
    public List<io.dingodb.common.store.KeyValue> getKeyValues(long startTs, List<byte[]> keys, long timeOut) {
        return getKeyValues(startTs, keys, timeOut, new ArrayList<>());
    }

    @NonNull
    private List<io.dingodb.common.store.KeyValue> getKeyValues(
        long startTs,
        List<byte[]> keys,
        long timeOut,
        List<Long> resolvedLocks
    ) {
        long start = System.currentTimeMillis();
        CommonId txnId = new CommonId(CommonId.CommonType.TRANSACTION, TransactionManager.getServerId().seq, startTs);
        MdcUtils.setTxnId(txnId.toString());
        try {
            int n = 1;
            while (true) {
                TxnBatchGetRequest txnBatchGetRequest = MAPPER.batchGetTo(
                    startTs, IsolationLevel.SnapshotIsolation, keys
                );
                txnBatchGetRequest.setResolveLocks(resolvedLocks);
                TxnBatchGetResponse response;

                try {
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
                    ResolveLockStatus resolveLockStatus = resolveLockConflictNew(
                        singletonList(response.getTxnResult()),
                        IsolationLevel.SnapshotIsolation.getCode(),
                        startTs,
                        resolvedLocks,
                        "txnBatchGet",
                        true,
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
                            LogUtils.info(log, "txnBatchGet lockInfo wait {} ms end.", lockTtl);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (resolveLockStatus ==  ResolveLockStatus.UNKNOWN) {
                        throw new RuntimeException("startTs:" + startTs + " resolve lock status is unknown");
                    }
                } catch (RequestErrorException e) {
                    if (e.getErrorCode() == 130003) {
                        LogUtils.error(log, "ETXN_MEMORY_LOCK_CONFLICT, Error:" + e.getMessage(), e);
                        if (timeOut < 0) {
                            throw new RuntimeException("startTs:" + startTs + " txnBatchGet error:" + e);
                        }
                        try {
                            long lockTtl = TxnVariables.WaitFixTime;
                            if (n < TxnVariables.WaitFixNum) {
                                lockTtl = TxnVariables.WaitTime * n;
                            }
                            Thread.sleep(lockTtl);
                            n++;
                            timeOut -= lockTtl;
                            LogUtils.info(log, "txnBatchGet ETXN_MEMORY_LOCK_CONFLICT wait {} ms end.", lockTtl);
                        } catch (InterruptedException e1) {
                            throw new RuntimeException(e1);
                        }
                    } else {
                        LogUtils.error(log, "txnBatchGet Error:" + e.getMessage(), e);
                        throw e;
                    }
                }
            }
        } finally {
            long sub = System.currentTimeMillis() - start;
            DingoMetrics.timer("txnBatchGetRpc").update(sub, TimeUnit.MILLISECONDS);
        }
    }

    private List<KeyValue> collectLockCollectionKeyValues(
        long startTs,
        List<TxnScanEntry> entries,
        long timeOut
    ) {
        List<KeyValue> result = new ArrayList<>(entries.size());
        List<LockInfo> locks = new ArrayList<>();
        List<byte[]> lockedKeys = new ArrayList<>();
        List<Integer> lockedPositions = new ArrayList<>();
        for (TxnScanEntry entry : entries) {
            TxnScanEntry.EntryNest entryNest = entry.getEntry();
            if (entryNest == null) {
                continue;
            }
            switch (entryNest.nest()) {
                case KV:
                    result.add((KeyValue) entryNest);
                    break;
                case LOCKED:
                    LockInfo lockInfo = (LockInfo) entryNest;
                    if (lockInfo.getKey() == null) {
                        LogUtils.warn(log, "txnScan lock collection entry has no locked key, startTs:{}", startTs);
                        break;
                    }
                    locks.add(lockInfo);
                    lockedKeys.add(lockInfo.getKey());
                    lockedPositions.add(result.size());
                    result.add(null);
                    break;
                default:
                    break;
            }
        }
        if (!lockedKeys.isEmpty()) {
            refillLockedKeys(startTs, locks, lockedKeys, lockedPositions, timeOut, result);
        }
        List<KeyValue> orderedResult = new ArrayList<>(result.size());
        for (KeyValue kv : result) {
            if (kv != null) {
                orderedResult.add(kv);
            }
        }
        return orderedResult;
    }

    private void refillLockedKeys(
        long startTs,
        List<LockInfo> locks,
        List<byte[]> lockedKeys,
        List<Integer> lockedPositions,
        long timeOut,
        List<KeyValue> result
    ) {
        long refillStart = System.currentTimeMillis();
        int refillCount = 0;
        List<Long> resolvedLocks = new ArrayList<>();
        HashSet<Long> resolvedLockSet = new HashSet<>();
        int n = 1;
        int lockIndex = 0;
        while (lockIndex < locks.size()) {
            LockInfo lock = locks.get(lockIndex);
            if (resolvedLockSet.contains(lock.getLockTs())) {
                lockIndex++;
                continue;
            }

            TxnResultInfo txnResultInfo = new TxnResultInfo();
            txnResultInfo.setLocked(lock);
            ResolveLockStatus resolveLockStatus = resolveLockConflictNew(
                singletonList(txnResultInfo),
                IsolationLevel.SnapshotIsolation.getCode(),
                startTs,
                resolvedLocks,
                "txnScan",
                true,
                false
            );
            resolvedLockSet.addAll(resolvedLocks);
            if (resolveLockStatus == ResolveLockStatus.LOCK_TTL
                || resolveLockStatus == ResolveLockStatus.TXN_NOT_FOUND) {
                if (timeOut < 0) {
                    LogUtils.info(log, "timeOut < 0, startTs:{}", startTs);
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
                    LogUtils.info(log, "scanTs:{}, txnScan lockInfo wait {} ms end.", startTs, lockTtl);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else if (resolveLockStatus == ResolveLockStatus.UNKNOWN) {
                throw new RuntimeException("startTs:" + startTs + " resolve lock status is unknown");
            } else {
                lockIndex++;
            }
        }

        // lookupBatchSize is tuned for executor-side lookup, where keys are grouped by region before txnGet.
        // This refill runs inside a region-bound TransactionStoreInstance, so use a smaller chunk to keep
        // a single-region txnBatchGet conservative in request size and transient memory usage.
        int batchLimit = Math.max(1, ScopeVariables.lookupBatchSize() / LOCK_COLLECTION_REFILL_BATCH_DIVISOR);
        Map<ByteBuffer, KeyValue> valuesByKey = new HashMap<>();
        for (int start = 0; start < lockedKeys.size(); start += batchLimit) {
            int end = Math.min(start + batchLimit, lockedKeys.size());
            List<byte[]> chunk = lockedKeys.subList(start, end);
            List<io.dingodb.common.store.KeyValue> batchValues = getKeyValues(
                startTs, chunk, timeOut, resolvedLocks
            );
            if (batchValues != null) {
                for (io.dingodb.common.store.KeyValue kv : batchValues) {
                    if (kv != null && kv.getKey() != null && kv.getValue() != null) {
                        KeyValue refill = new KeyValue();
                        refill.setKey(kv.getKey());
                        refill.setValue(kv.getValue());
                        valuesByKey.put(ByteBuffer.wrap(kv.getKey()), refill);
                        refillCount++;
                    }
                }
            }
        }

        // Update the result list with the refilled values for locked keys
        for (int i = 0; i < lockedKeys.size(); i++) {
            KeyValue refill = valuesByKey.get(ByteBuffer.wrap(lockedKeys.get(i)));
            if (refill != null) {
                result.set(lockedPositions.get(i), refill);
            }
        }

        long refillCost = System.currentTimeMillis() - refillStart;
        LogUtils.info(log, "refillLockedKeys lockedKeys:{}, refilled:{}, cost:{}ms",
            lockedKeys.size(), refillCount, refillCost);
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

    public void txnResolveLockNew(TxnResolveLock txnResolveLock, long startTs, String funName) {
        long start = System.currentTimeMillis();
        Integer retry = io.dingodb.common.util.Optional.mapOrGet(
            DingoConfiguration.instance().find("retry", int.class),
            __ -> __,
            () -> 30
        );
        try {
            TxnResolveLockResponse txnResolveLockResponse;
            boolean getService = false;
            while (retry-- > 0) {
                try {
                    if (indexService != null) {
                        if (getService) {
                            txnResolveLockResponse = Services.indexRegionService(
                                    Configuration.coordinatorSet(),
                                    txnResolveLock.getKeys().get(0),
                                    30)
                                .txnResolveLock(txnResolveLock.getStartTs(), MAPPER.resolveTxnTo(txnResolveLock));
                        } else {
                            txnResolveLockResponse = indexService.txnResolveLock(
                                txnResolveLock.getStartTs(),
                                MAPPER.resolveTxnTo(txnResolveLock)
                            );
                        }
                    } else if (documentService != null) {
                        if (getService) {
                            txnResolveLockResponse = Services.documentRegionService(
                                    Configuration.coordinatorSet(),
                                    txnResolveLock.getKeys().get(0),
                                    30)
                                .txnResolveLock(txnResolveLock.getStartTs(), MAPPER.resolveTxnTo(txnResolveLock));
                        } else {
                            txnResolveLockResponse = documentService.txnResolveLock(
                                txnResolveLock.getStartTs(),
                                MAPPER.resolveTxnTo(txnResolveLock)
                            );
                        }
                    } else {
                        if (getService) {
                            txnResolveLockResponse = Services.storeRegionService(
                                    Configuration.coordinatorSet(),
                                    txnResolveLock.getKeys().get(0),
                                    30)
                                .txnResolveLock(txnResolveLock.getStartTs(), MAPPER.resolveTxnTo(txnResolveLock));
                        } else {
                            txnResolveLockResponse = storeService.txnResolveLock(
                                txnResolveLock.getStartTs(),
                                MAPPER.resolveTxnTo(txnResolveLock)
                            );
                        }
                    }
                    LogUtils.debug(log,
                        "startTs:{}, {} txnResolveLockResponse: {}", startTs,
                        funName, txnResolveLockResponse);
                } catch (RegionSplitException | DingoClientException.InvalidRouteTableException e) {
                    LogUtils.error(log, e.getMessage(), e);
                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                    getService = true;
                }
            }
        } finally {
            long sub = System.currentTimeMillis() - start;
            DingoMetrics.timer("txnResolveLockRpc").update(sub, TimeUnit.MILLISECONDS);
        }
    }

    private int recordResolvingLocks(List<LockInfo> locks, long callerStartTS) {
        List<LockInfo> resolving = new ArrayList<>(locks);
        resolvingConcurrency.merge(callerStartTS, 1, Integer::sum);
        int token = resolvingLocks.computeIfAbsent(callerStartTS, k -> new ArrayList<>()).size();
        resolvingLocks.get(callerStartTS).add(resolving);

        return token;
    }

    private void resolveLocksDone(long callerStartTS, int token) {
        List<List<LockInfo>> resolving = resolvingLocks.get(callerStartTS);
        if (resolving != null && token < resolving.size()) {
            resolving.set(token, null);
        }

        resolvingConcurrency.merge(callerStartTS, -1, Integer::sum);
        if (resolvingConcurrency.getOrDefault(callerStartTS, 0) == 0) {
            resolvingLocks.remove(callerStartTS);
            resolvingConcurrency.remove(callerStartTS);
        }
    }


    private TxnStatus resolveSingleLock(ResolveLocksOptions opts, LockInfo lock) {
        TxnStatus status = ResolveLockUtil.getTxnStatusFromLock(
            lock,
            TsoService.INSTANCE.tso(),
            opts
        );

        if (status.getTtl() != 0) {
            return status;
        }

        // Handling asynchronous commits
        if (status.getPrimaryLock() != null && status.getPrimaryLock().isUseAsyncCommit() &&
            !opts.isForceSyncCommit()) {
            return resolveAsyncCommitLock(opts, lock, status);
        }
        Action action = status.getAction();
        // Dealing with pessimism
        if (lock.getLockType() == Op.Lock && lock.getForUpdateTs() != 0
            && (action == Action.LockNotExistRollback
            || action == Action.TTLExpirePessimisticRollback
            || action == Action.TTLExpireRollback)) {
            // pessimistic lock
            TxnPessimisticRollBack pessimisticRollBack = TxnPessimisticRollBack.builder()
                .isolationLevel(IsolationLevel.of(opts.getIsolationLevel()))
                .startTs(lock.getLockTs())
                .forUpdateTs(lock.getForUpdateTs())
                .keys(Collections.singletonList(lock.getKey()))
                .build();
            txnPessimisticLockRollback(pessimisticRollBack);
            status.setResolveLockStatus(ResolveLockStatus.PESSIMISTIC_ROLLBACK);
            LogUtils.debug(log,
                "startTs:{}, {} txnPessimisticLockRollback end", opts.getCallerStartTS(), opts.getFunName());
        } else {
            if (opts.isForRead()) {
                // Asynchronous read lock resolution
                // resolveLock store commit
                Executors.execute("for-read-async-resolve-lock-" + opts.getCallerStartTS(), () -> {
                    try {
                        MdcUtils.removeTxnId();
                        TxnResolveLock resolveLockRequest = TxnResolveLock.builder()
                            .isolationLevel(IsolationLevel.of(opts.getIsolationLevel()))
                            .startTs(lock.getLockTs())
                            .commitTs(status.getCommitTs())
                            .keys(singletonList(lock.getKey()))
                            .build();
                        txnResolveLockNew(resolveLockRequest, opts.getCallerStartTS(), opts.getFunName());
                        LogUtils.info(log, "Async resolveAsyncLock end for read, lockTs:{}, commitTs:{}, " +
                            "key:{}", lock.getLockTs(), status.getCommitTs(), Arrays.toString(lock.getKey()));
                    } catch (Exception e) {
                        LogUtils.error(log, "Async resolve lock failed for read, startTS:"
                            + opts.getCallerStartTS(), e);
                    }
                });
                ResolveLockStatus resolveLockStatus = ResolveLockStatus.ROLLBACK;
                if (status.getCommitTs() > 0) {
                    resolveLockStatus = ResolveLockStatus.COMMIT;
                }
                status.setResolveLockStatus(resolveLockStatus);
                LogUtils.debug(log,
                    "startTs:{}, {} txnResolveLock end status: {}", opts.getCallerStartTS(),
                    opts.getFunName(), resolveLockStatus);
            } else {
                // resolveLock store commit
                TxnResolveLock resolveLockRequest = TxnResolveLock.builder()
                    .isolationLevel(IsolationLevel.of(opts.getIsolationLevel()))
                    .startTs(lock.getLockTs())
                    .commitTs(status.getCommitTs())
                    .keys(singletonList(lock.getKey()))
                    .build();
                txnResolveLockNew(resolveLockRequest, opts.getCallerStartTS(), opts.getFunName());
                ResolveLockStatus resolveLockStatus = ResolveLockStatus.ROLLBACK;
                if (status.getCommitTs() > 0) {
                    resolveLockStatus = ResolveLockStatus.COMMIT;
                }
                status.setResolveLockStatus(resolveLockStatus);
                LogUtils.info(log,
                    "startTs:{}, {} txnResolveLock end status: {}", opts.getCallerStartTS(),
                    opts.getFunName(), resolveLockStatus);
            }
        }

        return status;
    }

    public ResolveLockResult resolveLocksWithOpts(ResolveLocksOptions opts) {
        long callerStartTS = opts.getCallerStartTS();
        List<LockInfo> locks = opts.getLocks();
        boolean forRead = opts.isForRead();

        if (locks.isEmpty()) {
            return ResolveLockResult.builder().ttl(0).build();
        }

        TxnExpireTime txnExpire = new TxnExpireTime();
        List<Long> canIgnore = new ArrayList<>();
//        List<Long> canAccess = new ArrayList<>();

        // Locks in record parsing
//        int token = recordResolvingLocks(locks, callerStartTS);

        try {
            for (LockInfo lock : locks) {
                try {
                    TxnStatus status;
                    try {
                        status = resolveSingleLock(opts, lock);
                    } catch (NonAsyncCommitLockException e) {
                        opts.setForceSyncCommit(true);
                        status = resolveSingleLock(opts, lock);
                    }
                    if (!forRead) {
                        if (status.getTtl() > 0) {
                            long msBeforeExpired = TsoService.INSTANCE.untilExpired(status.getTtl());
                            txnExpire.update(msBeforeExpired);
                            continue;
                        }
                    }
                    if (status.isDoneStatus()) {
                        continue;
                    }

                    // Handling lock states in read scenarios
                    // Concurrent reads may occur when other regions have already been MinCommitTSPushed,
                    // causing subsequent actions to be null, In this case, as long as the returned MinCommitTs
                    // is greater than or equal to callerStartTS, it can be considered as MinCommitTSPushed.

                    // Dingo store may return NoAction without lockInfo when a live lock's minCommitTs
                    // already >= callerStartTS. Reads can ignore that lock.
                    // (Note: This is a compatibility optimization; improving the RPC protocol would be better.)
                    boolean noActionReadCanIgnore = forRead
                        && status.getTtl() > 0
                        && status.getCommitTs() == 0
                        && (status.getAction() == Action.NoAction || status.getAction() == null)
                        && status.getPrimaryLock() == null;

                    if (status.getAction() == Action.MinCommitTSPushed ||
                        (status.getPrimaryLock() != null &&
                            status.getPrimaryLock().getMinCommitTs() >= callerStartTS) ||
                        noActionReadCanIgnore) {
                        canIgnore.add(lock.getLockTs());
                        if (forRead) {
                            LogUtils.debug(log, "resolveSingleLock ignore lock status:{}", status);
                            continue;
                        }
                        status.setResolveLockStatus(ResolveLockStatus.LOCK_TTL);
                    }
//                    else if (status.isCommitted() && status.getCommitTs() <= callerStartTS) {
//                        canAccess.add(lock.getLockTs());
//                    }
                    long msBeforeExpired = TsoService.INSTANCE.untilExpired(status.getTtl());
                    txnExpire.update(msBeforeExpired);
                    if (msBeforeExpired > 0) {
                        status.setResolveLockStatus(ResolveLockStatus.LOCK_TTL);
                    }
                    LogUtils.debug(log,"resolveSingleLock status:{}", status);
                } catch (Exception e) {
                    LogUtils.error(log, "Resolve lock error: {}", lock, e);
                    txnExpire.update(0);
                    if (ResolveLockUtil.isTxnNotFoundError(e)) {
                        return ResolveLockResult.builder()
                            .ttl(txnExpire.getValue())
                            .resolveLockStatus(ResolveLockStatus.TXN_NOT_FOUND)
                            .build();
                    }
                    return ResolveLockResult.builder()
                        .ttl(txnExpire.getValue())
                        .resolveLockStatus(ResolveLockStatus.UNKNOWN)
                        .build();
                }
            }

            ResolveLockStatus resolveLockStatus = ResolveLockStatus.NONE;
            if (txnExpire.getValue() > 0) {
                resolveLockStatus = ResolveLockStatus.LOCK_TTL;
            } else if (!canIgnore.isEmpty()) {
                resolveLockStatus = ResolveLockStatus.MIN_COMMIT_TS_PUSHED;
            }
            return ResolveLockResult.builder()
                .ttl(txnExpire.getValue())
                .ignoreLocks(canIgnore)
                .resolveLockStatus(resolveLockStatus)
//                .accessLocks(canAccess)
                .build();

        } finally {
//            resolveLocksDone(callerStartTS, token);
        }
    }

    public ResolveLockStatus resolveLockConflictNew(List<TxnResultInfo> txnResult, int isolationLevel,
                                                 long startTs, List<Long> resolvedLocks, String funName,
                                                 boolean forRead, boolean isOptimistic) {
        long start = System.currentTimeMillis();
        try {
            List<LockInfo> lockInfos = extractLockInfos(txnResult, startTs, funName, isOptimistic);
            if (lockInfos.isEmpty()) {
                return ResolveLockStatus.NONE;
            }
            ResolveLocksOptions opts = ResolveLocksOptions.builder()
                .callerStartTS(startTs)
                .locks(lockInfos)
                .forRead(forRead)
                .lite(true)
                .isolationLevel(isolationLevel)
                .funName(funName)
                .build();

            ResolveLockResult result = resolveLocksWithOpts(opts);

            if (result.getIgnoreLocks() != null && !result.getIgnoreLocks().isEmpty()) {
                resolvedLocks.addAll(result.getIgnoreLocks());
            }

            return result.getResolveLockStatus();
        } finally {
            long sub = System.currentTimeMillis() - start;
            if (forRead) {
                DingoMetrics.timer("readResolveConflict").update(sub, TimeUnit.MILLISECONDS);
            } else {
                DingoMetrics.timer("writeResolveLockConflict").update(sub, TimeUnit.MILLISECONDS);
            }
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
            // not used
            if (statusResponse.getLockInfo() != null && statusResponse.getLockInfo().isUseAsyncCommit()
                && !forceSyncCommit) {
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
            } else if (commitTs > 0 && action == Action.LockNotExistDoNothing) {
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
            // lockInfo is null ,return Action.LockNotExistRollback(rollback)
            // LockNotExistDoNothing (commit)
            // txnNotFound
            // lockInfo is not null
            //Action.TTLExpireRollback(store :pessimistic primary key rollback)
            if (lockInfo != null) {
                if (lockInfo.isUseAsyncCommit() && !forceSyncCommit) {
                    long lockTtl = lockInfo.getLockTtl();
                    if (lockTtl > 0 && !TsoService.INSTANCE.IsExpired(lockTtl)) {
                        LogUtils.info(log, "startTs:{}, lockTs:{} useAsyncCommit lockTtl not IsExpired, "
                            + "lockTtl:{}", startTs, lockInfo.getLockTs(), lockTtl);
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
                // success not used
                if (forRead && statusResponse.getAction() == Action.MinCommitTSPushed
                    && statusResponse.getLockTtl() > 0) {
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

    public class ScanIterator implements ProfileScanIterator {
        private final long startTs;
        private final StoreInstance.Range range;
        private final long timeOut;
        private final io.dingodb.sdk.service.entity.common.CoprocessorV2 coprocessor;
        private final boolean enableLockCollection;

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
            this.enableLockCollection = ScopeVariables.enableTxnScanLockCollection() && this.coprocessor == null;
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
                try {
                    if (indexService != null) {
                        txnScanResponse = indexService.txnScan(startTs, txnScanRequest);
                    } else if (documentService != null) {
                        txnScanResponse = documentService.txnScan(startTs, txnScanRequest);
                    } else {
                        txnScanRequest.setEnableLockCollection(enableLockCollection);
                        txnScanResponse = storeService.txnScan(startTs, txnScanRequest);
                    }
                    if (enableLockCollection
                        && txnScanResponse.getEntries() != null
                        && !txnScanResponse.getEntries().isEmpty()) {
                        if (txnScanResponse.getTxnResult() != null) {
                            throw new RuntimeException(
                                "txnScan lock collection response has both entries and txnResult, startTs:" + startTs
                            );
                        }
                        keyValues = collectLockCollectionKeyValues(
                            startTs, txnScanResponse.getEntries(), scanTimeOut
                        ).iterator();
                        hasMore = txnScanResponse.isHasMore();
                        if (hasMore) {
                            withStart = false;
                            current = new StoreInstance.Range(
                                txnScanResponse.getEndKey(), range.end, withStart, range.withEnd
                            );
                        }
                        break;
                    }
                    if (txnScanResponse.getTxnResult() != null) {
                        ResolveLockStatus resolveLockStatus = resolveLockConflictNew(
                            singletonList(txnScanResponse.getTxnResult()),
                            IsolationLevel.SnapshotIsolation.getCode(),
                            startTs,
                            resolvedLocks,
                            "txnScan",
                            true,
                            false
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
                        } else if (resolveLockStatus ==  ResolveLockStatus.UNKNOWN) {
                            throw new RuntimeException("startTs:" + startTs + " resolve lock status is unknown");
                        }
                        continue;
                    }
                    keyValues = Optional.ofNullable(txnScanResponse.getKvs())
                        .map(List::iterator).orElseGet(Collections::emptyIterator);
                    hasMore = txnScanResponse.isHasMore();
                    if (hasMore) {
                        withStart = false;
                        current = new StoreInstance.Range(
                            txnScanResponse.getEndKey(), range.end, withStart, range.withEnd
                        );
                    }
                    break;
                } catch (RequestErrorException e) {
                    if (e.getErrorCode() == 130003) {
                        LogUtils.error(log, "ETXN_MEMORY_LOCK_CONFLICT, Error:" + e.getMessage(), e);
                        if (scanTimeOut < 0) {
                            throw new RuntimeException("startTs:" + startTs + " txnScan error:" + e);
                        }
                        try {
                            long lockTtl = TxnVariables.WaitFixTime;
                            if (n < TxnVariables.WaitFixNum) {
                                lockTtl = TxnVariables.WaitTime * n;
                            }
                            Thread.sleep(lockTtl);
                            n++;
                            scanTimeOut -= lockTtl;
                            LogUtils.info(log, "txnScan ETXN_MEMORY_LOCK_CONFLICT wait {} ms end.", lockTtl);
                        } catch (InterruptedException e1) {
                            throw new RuntimeException(e1);
                        }
                    } else {
                        LogUtils.error(log, "txnScan Error:" + e.getMessage(), e);
                        throw e;
                    }
                }
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
        private final boolean enableLockCollection;

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
            this.enableLockCollection = ScopeVariables.enableTxnScanLockCollection() && this.coprocessor == null;
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
                        txnScanRequest.setEnableLockCollection(enableLockCollection);
                        txnScanResponse = storeService.txnScan(startTs, txnScanRequest);
                    }

                    if (enableLockCollection
                        && txnScanResponse.getEntries() != null
                        && !txnScanResponse.getEntries().isEmpty()) {
                        if (txnScanResponse.getTxnResult() != null) {
                            throw new RuntimeException(
                                "txnScan stream lock collection response has both entries and txnResult, startTs:"
                                    + startTs
                            );
                        }
                        keyValues = collectLockCollectionKeyValues(
                            startTs, txnScanResponse.getEntries(), scanTimeOut
                        ).iterator();
                        if (txnScanResponse.getStreamMeta() != null) {
                            this.streamId = txnScanResponse.getStreamMeta().getStreamId();
                            hasMore = txnScanResponse.getStreamMeta().isHasMore();
                            if (hasMore) {
                                withStart = false;
                                range = new StoreInstance.Range(
                                    txnScanResponse.getEndKey(), range.end, withStart, range.withEnd
                                );
                            }
                        } else {
                            hasMore = false;
                        }
                        break;
                    }

                    if (txnScanResponse.getTxnResult() != null) {
                        ResolveLockStatus resolveLockStatus = resolveLockConflictNew(
                            singletonList(txnScanResponse.getTxnResult()),
                            IsolationLevel.SnapshotIsolation.getCode(),
                            startTs,
                            resolvedLocks,
                            "txnScan",
                            true,
                            false
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
                        } else if (resolveLockStatus ==  ResolveLockStatus.UNKNOWN) {
                            throw new RuntimeException("startTs:" + startTs + " resolve lock status is unknown");
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
                    } else if (e.getErrorCode() == 130003) {
                        LogUtils.error(log, "ETXN_MEMORY_LOCK_CONFLICT, Error:" + e.getMessage(), e);
                        if (scanTimeOut < 0) {
                            throw new RuntimeException("startTs:" + startTs + " txnScan error:" + e);
                        }
                        try {
                            long lockTtl = TxnVariables.WaitFixTime;
                            if (n < TxnVariables.WaitFixNum) {
                                lockTtl = TxnVariables.WaitTime * n;
                            }
                            Thread.sleep(lockTtl);
                            n++;
                            scanTimeOut -= lockTtl;
                            LogUtils.info(log, "txnScan ETXN_MEMORY_LOCK_CONFLICT wait {} ms end.", lockTtl);
                        } catch (InterruptedException e1) {
                            throw new RuntimeException(e1);
                        }
                        continue;
                    } else {
                        LogUtils.error(log, "txnScan Error:" + e.getMessage(), e);
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


    public class DocumentScanFilterStreamIterator implements ProfileScanIterator {
        private final long startTs;
        private final DocumentSearchParameter documentSearchParameter;
        private boolean hasMore = true;
        private String streamId;
        private boolean closeStream;
        private Iterator<io.dingodb.common.store.KeyValue> keyValues;
        private final OperatorProfile rpcProfile;
        private final OperatorProfile initRpcProfile;

        private final long timeOut;

        public DocumentScanFilterStreamIterator(long startTs,
                                                DocumentSearchParameter documentSearchParameter,
                                                long timeOut) {
            this.startTs = startTs;
            this.documentSearchParameter = documentSearchParameter;
            this.timeOut = timeOut;
            this.streamId = null;
            this.closeStream = false;
            initRpcProfile = new OperatorProfile("initDocumentScanFilterRpc");
            rpcProfile = new OperatorProfile("continueDocumentScanFilterRpc");
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
            boolean closeStream = false;

            DocumentSearchAllRequest documentSearchAllRequest = DocumentSearchAllRequest.builder().parameter(
                MAPPER.documentSearchParamTo(documentSearchParameter)
            ).build();

            if (documentSearchAllRequest.getStreamMeta() == null) {
                documentSearchAllRequest.setStreamMeta(new StreamRequestMeta());
            }
            long scanTimeOut = timeOut;
            int n = 1;
            //actually it is not a loop. Just run once in normal cases.
            while (true) {
                documentSearchAllRequest.getStreamMeta().setStreamId(streamId);
                documentSearchAllRequest.getStreamMeta().setClose(closeStream);
                documentSearchAllRequest.getStreamMeta().setLimit(ScopeVariables.getRpcBatchSize());

                try {
                    DocumentSearchAllResponse documentSearchAllResponse = documentService.documentSearchAll(
                        startTs,
                        documentSearchAllRequest
                    );
                    List<DocumentWithScore> documentWithScores = documentSearchAllResponse.getDocumentWithScores();

                    if (documentSearchAllResponse.getError() == null) {
                        //get and set stream id for next request.
                        if (documentSearchAllResponse.getStreamMeta() != null) {
                            this.streamId = documentSearchAllResponse.getStreamMeta().getStreamId();
                            hasMore = documentSearchAllResponse.getStreamMeta().isHasMore();
                        } else {
                            hasMore = false;
                        }
                        if (documentWithScores == null || documentWithScores.isEmpty()) {
                            keyValues = Collections.emptyIterator();
                        } else {
                            List<io.dingodb.common.store.KeyValue> list = new ArrayList<>();
                            for (DocumentWithScore documentWithScore : documentWithScores) {
                                Document document = documentWithScore.getDocumentWithId().getDocument();
                                if (document != null) {
                                    TableData tableData = document.getTableData();
                                    if (tableData != null) {
                                        list.add(
                                            new io.dingodb.common.store.KeyValue(
                                                tableData.getTableKey(),
                                                tableData.getTableValue()
                                            )
                                        );
                                    }
                                }
                            }
                            keyValues = list.listIterator();
                        }
                        break;
                    }
                } catch (RequestErrorException e) {
                    if (e.getErrorCode() == 10118) {
                        //ESTREAM_EXPIRED: stream id is expired.
                        this.streamId = null;
                        LogUtils.info(log, "document scan filter stream id expired, info:{}", e.getMessage());
                    } else if (e.getErrorCode() == 130003) {
                        LogUtils.error(log, "ETXN_MEMORY_LOCK_CONFLICT, Error:" + e.getMessage(), e);
                        if (scanTimeOut < 0) {
                            throw new RuntimeException("startTs:" + startTs + " documentSearchAll error:" + e);
                        }
                        try {
                            long lockTtl = TxnVariables.WaitFixTime;
                            if (n < TxnVariables.WaitFixNum) {
                                lockTtl = TxnVariables.WaitTime * n;
                            }
                            Thread.sleep(lockTtl);
                            n++;
                            scanTimeOut -= lockTtl;
                            LogUtils.info(log, "documentSearchAll ETXN_MEMORY_LOCK_CONFLICT wait {} ms end.",
                                lockTtl);
                            continue;
                        } catch (InterruptedException e1) {
                            throw new RuntimeException(e1);
                        }
                    } else {
                        LogUtils.error(log, "documentSearchAll Error:" + e.getMessage(), e);
                        throw e;
                    }
                } catch (DingoClientException.InvalidRouteTableException e) {
                    LogUtils.error(log, e.getMessage() ,e);
                    throw e;
                }
                break;
            }
            long sub = System.currentTimeMillis() - start;
            DingoMetrics.timer("documentScanFilterRpc").update(sub, TimeUnit.MILLISECONDS);
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
            return keyValues.next();
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
