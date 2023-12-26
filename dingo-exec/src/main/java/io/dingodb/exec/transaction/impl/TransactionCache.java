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
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public class TransactionCache {
    private final StoreInstance cache = Services.LOCAL_STORE.getInstance(null, null);
    private final CommonId txnId;

    public TransactionCache(CommonId txnId) {
        this.txnId = txnId;
    }
    public CacheToObject getPrimaryKey() {
        // call StoreService
        CacheToObject primaryKey = null;
        Iterator<KeyValue> iterator = cache.scan(txnId.encode());
        if (iterator.hasNext()) {
            KeyValue keyValue = iterator.next();
            Object[] tuple = ByteUtils.decode(keyValue);
            CommonId txnId = (CommonId) tuple[0];
            CommonId tableId = (CommonId) tuple[1];
            CommonId newPartId = (CommonId) tuple[2];
            int op = (byte) tuple[3];
            byte[] key = (byte[]) tuple[4];
            byte[] value = (byte[]) tuple[5];
            primaryKey = new CacheToObject(TransactionCacheToMutation.cacheToMutation(op, key, value, tableId, newPartId), tableId, newPartId);
            log.info("txnId:{} primary key is {}" , txnId, primaryKey);
        } else {
            throw new RuntimeException(txnId + ",PrimaryKey is null");
        }
        return primaryKey;
    }

    public boolean checkContinue() {
        Iterator<KeyValue> iterator = cache.scan(txnId.encode());
        return iterator.hasNext();
    }

    public Iterator<Object[]> iterator() {
        Iterator<KeyValue> iterator = cache.scan(txnId.encode());
        return Iterators.transform(iterator, wrap(ByteUtils::decode)::apply);
    }
}
