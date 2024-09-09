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

package io.dingodb.store.service;

import io.dingodb.common.CommonId;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.StoreService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.proxy.service.TransactionStoreInstance;
import io.dingodb.tso.TsoService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@Slf4j
public class StoreKvTxn extends CommitBase implements io.dingodb.store.api.transaction.StoreKvTxn {
    private final CommonId tableId;
    @Getter
    private final CommonId regionId;
    public StoreKvTxn(CommonId tableId, CommonId regionId) {
        super(
            Services.storeRegionService(coordinators, regionId.seq, TransactionUtil.STORE_RETRY),
            new CommonId(CommonId.CommonType.PARTITION, tableId.seq, regionId.domain)
        );
        this.tableId = tableId;
        this.regionId = regionId;
    }

    public void del(byte[] key) {
        commit(key, null, Op.DELETE.getCode(), TsoService.getDefault().tso());
    }

    public void insert(byte[] startKey, byte[] endKey) {
        commit(startKey, endKey, Op.PUTIFABSENT.getCode(), TsoService.getDefault().tso());
    }

    public void update(byte[] key, byte[] value) {
        commit(key, value, Op.PUT.getCode(), TsoService.getDefault().tso());
    }

    public KeyValue get(byte[] key) {
        long startTs = TsoService.getDefault().tso();
        List<byte[]> keys = Collections.singletonList(key);
        TransactionStoreInstance storeInstance = new TransactionStoreInstance(storeService, null, partId);
        List<KeyValue> keyValueList = storeInstance.txnGet(startTs, keys, statementTimeout);
        if (keyValueList.isEmpty()) {
            return null;
        } else {
            return keyValueList.get(0);
        }
    }

    public Iterator<KeyValue> range(byte[] start, byte[] end) {
        boolean withEnd = ByteArrayUtils.compare(end, start) <= 0;
        long startTs = TsoService.getDefault().tso();
        TransactionStoreInstance storeInstance = new TransactionStoreInstance(storeService, null, partId);
        StoreInstance.Range range = new StoreInstance.Range(start, end, true, withEnd);
        return storeInstance.txnScan(startTs, range, statementTimeout, null);
    }

    @Override
    public TransactionStoreInstance refreshRegion(byte[] key) {
        CommonId regionIdNew = refreshRegionId(tableId, key);
        StoreService serviceNew = Services.storeRegionService(coordinators, regionIdNew.seq, 60);
        return new TransactionStoreInstance(serviceNew, null, partId);
    }
}
