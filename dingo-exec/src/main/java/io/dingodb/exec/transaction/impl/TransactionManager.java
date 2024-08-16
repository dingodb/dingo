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

package io.dingodb.exec.transaction.impl;

import io.dingodb.common.CommonId;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public final class TransactionManager {

    // connectionId -> Transaction
    private static final Map<CommonId, ITransaction> trans = new ConcurrentHashMap<>();

    private TransactionManager() {
    }

    public static @NonNull ITransaction createTransaction(@NonNull TransactionType trxType, long startTs, int isolationLevel) {
        ITransaction tran;
        switch (trxType) {
            case OPTIMISTIC:
                tran = new OptimisticTransaction(startTs, isolationLevel);
                break;
            case PESSIMISTIC:
                tran = new PessimisticTransaction(startTs, isolationLevel);
                break;
            case NONE:
                tran = new NoneTransaction(startTs, isolationLevel);
                break;
            default:
                LogUtils.error(log, "startTs:" + startTs + ", TransactionType: " + trxType.name() + " not supported");
                throw new ArithmeticException("TransactionType: " + trxType.name() + " not supported");
        }
        return tran;
    }

    public static @NonNull ITransaction createTransaction(@NonNull TransactionType trxType, @NonNull CommonId txnId, int isolationLevel) {
        ITransaction tran;
        switch (trxType) {
            case OPTIMISTIC:
                tran = new OptimisticTransaction(txnId, isolationLevel);
                break;
            case PESSIMISTIC:
                tran = new PessimisticTransaction(txnId, isolationLevel);
                break;
            case NONE:
                tran = new NoneTransaction(txnId, isolationLevel);
                break;
            default:
                LogUtils.error(log, "txnId:" + txnId + ", TransactionType: " + trxType.name() + " not supported");
                throw new ArithmeticException("TransactionType: " + trxType.name() + " not supported");
        }
        return tran;
    }

    public static long getStartTs() {
        return TsoService.getDefault().tso();
    }

    public static long getCommitTs() {
        return TsoService.getDefault().tso();
    }

    public static long nextTimestamp() {
        return TsoService.getDefault().tso();
    }

    public static void register(@NonNull CommonId txnId, @NonNull ITransaction transaction) {
        trans.put(txnId, transaction);
    }

    public static ITransaction getTransaction(@NonNull CommonId txnId) {
        return trans.get(txnId);
    }

    public static ITransaction getTransaction(@NonNull long startTs) {
        CommonId txnId = new CommonId(CommonId.CommonType.TRANSACTION, getServerId().seq, startTs);
        return Optional.ofNullable(trans.get(txnId)).get();
    }

    public static void unregister(@NonNull CommonId txnId) {
        trans.remove(txnId);
    }

    public static CommonId getServerId() {
        return DingoConfiguration.serverId() == null ? new CommonId(CommonId.CommonType.SCHEMA, 0L, 0L) : DingoConfiguration.serverId();
    }

    public static long lockTtlTm() {
        return TsoService.getDefault().timestamp() + TransactionUtil.lock_ttl;
    }

    public static Object getTable(CommonId txnId, CommonId tableId) {
        ITransaction transaction = trans.get(txnId);
        if (transaction == null) {
            return null;
        }
        InfoSchema is = transaction.getIs();
        if (is == null) {
            return null;
        }
        if (tableId.type == CommonId.CommonType.TABLE) {
            return is.getTable(tableId.seq);
        } else if (tableId.type == CommonId.CommonType.INDEX) {
            return is.getIndex(tableId.domain, tableId.seq);
        }
        return null;
    }

    public static Object getIndex(CommonId txnId, CommonId indexId) {
        ITransaction transaction = trans.get(txnId);
        if (transaction == null) {
            LogUtils.error(log, "[ddl] get index by txn, txn is null:{}", txnId);
            return null;
        }
        InfoSchema is = transaction.getIs();
        if (is == null) {
            LogUtils.error(log, "[ddl] get index by txn, info schema is null:{}", txnId);
            return null;
        }
        if (indexId.type == CommonId.CommonType.TABLE) {
            LogUtils.error(log, "get index, bug id type is table");
            return is.getTable(indexId.seq);
        } else if (indexId.type == CommonId.CommonType.INDEX) {
            return is.getIndex(indexId.domain, indexId.seq);
        }
        return null;
    }

    public static long getMinTs() {
        return trans.keySet().stream().mapToLong(txnId -> txnId.seq).min().orElse(Long.MAX_VALUE);
    }
}
