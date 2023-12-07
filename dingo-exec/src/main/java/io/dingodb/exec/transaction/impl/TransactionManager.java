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
import io.dingodb.exec.impl.LocalTimestampOracle;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.net.Channel;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class TransactionManager {

    // connectionId -> Transaction
    private static final Map<CommonId, ITransaction> trans = new ConcurrentHashMap<>();

    public static @NonNull ITransaction createTransaction(boolean pessimistic, long start_ts) {
        if (pessimistic) {
            return createTransaction(TransactionType.PESSIMISTIC, start_ts);
        } else {
            return createTransaction(TransactionType.OPTIMISTIC, start_ts);
        }
    }

    public static @NonNull ITransaction createTransaction(boolean pessimistic, @NonNull CommonId txnId) {
        if (pessimistic) {
            return createTransaction(TransactionType.PESSIMISTIC, txnId);
        } else {
            return createTransaction(TransactionType.OPTIMISTIC, txnId);
        }
    }

    public static @NonNull ITransaction createTransaction(@NonNull TransactionType trxType, long start_ts) {
        ITransaction tran;
        switch (trxType) {
            case OPTIMISTIC:
                tran = new OptimisticTransaction(start_ts);
                break;
            case PESSIMISTIC:
                tran = new PessimisticTransaction(start_ts);
                break;
            default:
                log.info("start_ts:" + start_ts + ", TransactionType: " + trxType.name() + " not supported");
                throw new ArithmeticException("TransactionType: " + trxType.name() + " not supported");
        }
        return tran;
    }

    public static @NonNull ITransaction createTransaction(@NonNull TransactionType trxType, @NonNull CommonId txnId) {
        ITransaction tran;
        switch (trxType) {
            case OPTIMISTIC:
                tran = new OptimisticTransaction(txnId);
                break;
            case PESSIMISTIC:
                tran = new PessimisticTransaction(txnId);
                break;
            default:
                log.info("txnId:" + txnId + ", TransactionType: " + trxType.name() + " not supported");
                throw new ArithmeticException("TransactionType: " + trxType.name() + " not supported");
        }
        return tran;
    }

    public static long getStart_ts() {
        return LocalTimestampOracle.INSTANCE.nextTimestamp();
    }

    public static long getCommit_ts() {
        return LocalTimestampOracle.INSTANCE.nextTimestamp();
    }

    public static long nextTimestamp() {
        return LocalTimestampOracle.INSTANCE.nextTimestamp();
    }

    public static void register(@NonNull CommonId txnId, @NonNull ITransaction transaction) {
        trans.put(txnId, transaction);
    }

    public static ITransaction getTransaction(@NonNull CommonId txnId) {
        return trans.get(txnId);
    }

    public static ITransaction getTransaction(@NonNull long start_ts) {
        CommonId txnId = new CommonId(CommonId.CommonType.TRANSACTION, getServerId().seq, start_ts);
        return Optional.ofNullable(trans.get(txnId)).get();
    }

    public static void unregister(@NonNull CommonId txnId) {
        trans.remove(txnId);
    }

    public static CommonId getServerId() {
        return DingoConfiguration.serverId() == null ? new CommonId(CommonId.CommonType.SCHEMA, 0l, 0l) : DingoConfiguration.serverId();
    }
}
