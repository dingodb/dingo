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
import io.dingodb.common.util.DebugLog;
import io.dingodb.exec.Services;
import io.dingodb.exec.transaction.base.CacheToObject;
import io.dingodb.exec.transaction.base.TxnLocalData;
import io.dingodb.exec.transaction.util.TransactionCacheToMutation;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
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
        while (iterator.hasNext()) {
            KeyValue keyValue = iterator.next();
            Object[] tuple = ByteUtils.decode(keyValue);
            TxnLocalData txnLocalData = (TxnLocalData) tuple[0];
            CommonId.CommonType type = txnLocalData.getDataType();
            CommonId txnId = txnLocalData.getTxnId();
            CommonId tableId = txnLocalData.getTableId();
            CommonId newPartId = txnLocalData.getPartId();
            int op = txnLocalData.getOp().getCode();
            byte[] key = txnLocalData.getKey();
            byte[] value = txnLocalData.getValue();
            byte[] txnIdByte = txnId.encode();
            byte[] tableIdByte = tableId.encode();
            byte[] partIdByte = newPartId.encode();
            int len = txnIdByte.length + tableIdByte.length + partIdByte.length;
            byte[] checkBytes = ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_CHECK_DATA,
                key,
                Op.CheckNotExists.getCode(),
                len,
                txnIdByte, tableIdByte, partIdByte);
            keyValue = cache.get(checkBytes);
            if (keyValue != null && keyValue.getValue() != null) {
                switch (Op.forNumber(op)) {
                    case PUT:
                        op = Op.PUTIFABSENT.getCode();
                        break;
                    case DELETE:
                        op = Op.CheckNotExists.getCode();
                        break;
                    default:
                        break;
                }
            }
            primaryKey = new CacheToObject(TransactionCacheToMutation.cacheToMutation(
                op,
                key,
                value,
                0L,
                tableId,
                newPartId), tableId, newPartId
            );
            DebugLog.debugDelegate(log, "txnId:{} primary key is {}" , txnId, primaryKey);
            if (op != Op.CheckNotExists.getCode()) {
                break;
            }
        }
        if (primaryKey == null) {
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

    public void deleteKey(byte[] key) {
        cache.delete(key);
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

    public void checkCache() {
        Iterator<KeyValue> iterator = cache.scan((byte[]) null);
        Iterator<Object[]> transform = Iterators.transform(iterator, wrap(ByteUtils::decode)::apply);
        while (transform.hasNext()) {
            Object[] next = transform.next();
            TxnLocalData txnLocalData = (TxnLocalData) next[0];
            log.info("txnId:{} tableId:{} partId:{} Op:{} Key:{} ", txnLocalData.getTxnId(), txnLocalData.getTableId(),
                txnLocalData.getPartId(), txnLocalData.getOp(), txnLocalData.getKey());
        }
    }

    public void cleanCache() {
        Iterator<KeyValue> iterator = cache.scan((byte[]) null);
        Iterator<Object[]> transform = Iterators.transform(iterator, wrap(ByteUtils::decode)::apply);
        while (transform.hasNext()) {
            Object[] next = transform.next();
            TxnLocalData txnLocalData = (TxnLocalData) next[0];
            log.info("txnId:{} tableId:{} partId:{} Op:{} Key:{} ", txnLocalData.getTxnId(), txnLocalData.getTableId(),
                txnLocalData.getPartId(), txnLocalData.getOp(), txnLocalData.getKey());
        }
        while (iterator.hasNext()) {
            cache.delete(iterator.next().getKey());
            log.info("key is {}", iterator.next().getKey());
        }
    }
}
