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
import io.dingodb.common.util.Optional;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class TransactionManager {

    // connectionId -> Transaction
    private static final Map<CommonId, ITransaction> trans = new ConcurrentHashMap<>();

    public static @NonNull ITransaction createTransaction(boolean pessimistic, long startTs, int isolationLevel) {
        if (pessimistic) {
            return createTransaction(TransactionType.PESSIMISTIC, startTs, isolationLevel);
        } else {
            return createTransaction(TransactionType.OPTIMISTIC, startTs, isolationLevel);
        }
    }

    public static @NonNull ITransaction createTransaction(boolean pessimistic, @NonNull CommonId txnId, int isolationLevel) {
        if (pessimistic) {
            return createTransaction(TransactionType.PESSIMISTIC, txnId, isolationLevel);
        } else {
            return createTransaction(TransactionType.OPTIMISTIC, txnId, isolationLevel);
        }
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
                log.info("startTs:" + startTs + ", TransactionType: " + trxType.name() + " not supported");
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
                log.info("txnId:" + txnId + ", TransactionType: " + trxType.name() + " not supported");
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
}
