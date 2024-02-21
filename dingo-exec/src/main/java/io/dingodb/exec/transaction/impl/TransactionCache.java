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

import com.google.common.collect.Iterators;
import io.dingodb.common.CommonId;
import io.dingodb.common.store.KeyValue;
import io.dingodb.exec.Services;
import io.dingodb.exec.transaction.base.CacheToObject;
import io.dingodb.exec.transaction.util.TransactionCacheToMutation;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.store.api.StoreInstance;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.List;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public class TransactionCache {
    private final StoreInstance cache = Services.LOCAL_STORE.getInstance(null, null);
    private final CommonId txnId;

    @Setter
    private CommonId jobId;

    private final boolean pessimisticRollback;

    private final boolean pessimisticResidualLock;

    private boolean pessimisticTransaction;

    private final boolean cleanCache;

    public TransactionCache(CommonId txnId) {
        this.txnId = txnId;
        this.pessimisticRollback = false;
        this.cleanCache = false;
        this.pessimisticResidualLock = false;
    }

    public TransactionCache(CommonId txnId, long jobSeqId) {
        this.txnId = txnId;
        this.jobId = new CommonId(CommonId.CommonType.JOB, txnId.seq, jobSeqId);
        this.pessimisticRollback = true;
        this.cleanCache = false;
        this.pessimisticResidualLock = false;
    }

    public TransactionCache(CommonId txnId, boolean cleanCache, boolean pessimisticTransaction) {
        this.txnId = txnId;
        this.pessimisticRollback = false;
        this.cleanCache = cleanCache;
        this.pessimisticTransaction = pessimisticTransaction;
        this.pessimisticResidualLock = false;
    }

    public TransactionCache(CommonId txnId, boolean pessimisticResidualLock) {
        this.txnId = txnId;
        this.pessimisticRollback = false;
        this.cleanCache = false;
        this.pessimisticTransaction = true;
        this.pessimisticResidualLock = pessimisticResidualLock;
    }


    public CacheToObject getPrimaryKey() {
        // call StoreService
        CacheToObject primaryKey = null;
        Iterator<KeyValue> iterator = cache.scan(getScanPrefix(CommonId.CommonType.TXN_CACHE_DATA, txnId));
        if (iterator.hasNext()) {
            KeyValue keyValue = iterator.next();
            Object[] tuple = ByteUtils.decode(keyValue);
            CommonId.CommonType type = CommonId.CommonType.of((byte) tuple[0]);
            CommonId txnId = (CommonId) tuple[1];
            CommonId tableId = (CommonId) tuple[2];
            CommonId newPartId = (CommonId) tuple[3];
            int op = (byte) tuple[4];
            byte[] key = (byte[]) tuple[5];
            byte[] value = (byte[]) tuple[6];
            primaryKey = new CacheToObject(TransactionCacheToMutation.cacheToMutation(
                op,
                key,
                value,
                0L,
                tableId,
                newPartId), tableId, newPartId
            );
            log.info("txnId:{} primary key is {}" , txnId, primaryKey);
        } else {
            throw new RuntimeException(txnId + ",PrimaryKey is null");
        }
        return primaryKey;
    }

    public KeyValue get(byte[] key) {
        return cache.get(key);
    }

    public List<KeyValue> getKeys(List<byte[]> keys) {
        return cache.get(keys);
    }

    public void deletePrefix(byte[] key) {
        cache.deletePrefix(key);
    }

    public byte[] getScanPrefix(CommonId.CommonType type, CommonId commonId) {
        byte[] txnByte = commonId.encode();
        byte[] result = new byte[txnByte.length + CommonId.TYPE_LEN];
        result[0] = (byte) type.getCode();
        System.arraycopy(txnByte, 0, result, CommonId.TYPE_LEN, txnByte.length);
        return result;
    }

    public boolean checkContinue() {
        Iterator<KeyValue> iterator = cache.scan(getScanPrefix(CommonId.CommonType.TXN_CACHE_DATA, txnId));
        return iterator.hasNext();
    }

    public boolean checkCleanContinue(boolean isPessimistic) {
        Iterator<KeyValue> iterator;
        if (isPessimistic) {
            iterator = cache.scan(getScanPrefix(CommonId.CommonType.TXN_CACHE_LOCK, txnId));
        } else {
            iterator = cache.scan(getScanPrefix(CommonId.CommonType.TXN_CACHE_DATA, txnId));
        }
        return iterator.hasNext();
    }

    public boolean checkPessimisticLockContinue() {
        Iterator<KeyValue> iterator = cache.scan(getScanPrefix(CommonId.CommonType.TXN_CACHE_EXTRA_DATA, jobId));
        return iterator.hasNext();
    }

    public boolean checkResidualPessimisticLockContinue() {
        Iterator<KeyValue> iterator = cache.scan(getScanPrefix(CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, txnId));
        return iterator.hasNext();
    }

    public Iterator<Object[]> iterator() {
        Iterator<KeyValue> iterator;
        if (pessimisticRollback) {
            iterator = cache.scan(getScanPrefix(CommonId.CommonType.TXN_CACHE_EXTRA_DATA, jobId));
            return Iterators.transform(iterator, wrap(ByteUtils::decode)::apply);
        } else if (pessimisticResidualLock) {
            iterator = cache.scan(getScanPrefix(CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, txnId));
            return Iterators.transform(iterator, wrap(ByteUtils::decode)::apply);
        } else if (cleanCache) {
            if (pessimisticTransaction) {
                iterator = cache.scan(getScanPrefix(CommonId.CommonType.TXN_CACHE_LOCK, txnId));
            } else {
                iterator = cache.scan(getScanPrefix(CommonId.CommonType.TXN_CACHE_DATA, txnId));
            }
            return Iterators.transform(iterator, wrap(ByteUtils::decodeTxnCleanUp)::apply);
        } else {
            iterator = cache.scan(getScanPrefix(CommonId.CommonType.TXN_CACHE_DATA, txnId));
            return Iterators.transform(iterator, wrap(ByteUtils::decode)::apply);
        }
    }
}
