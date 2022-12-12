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

package io.dingodb.server.executor.api;

import io.dingodb.common.CommonId;
import io.dingodb.common.store.KeyValue;
import io.dingodb.net.NetService;
import io.dingodb.store.api.StoreService;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

@Slf4j
public class ExecutorApi implements io.dingodb.server.api.ExecutorApi {

    private byte finishedTag = '1';
    private byte unfinishTag = '0';

    private StoreService storeService;

    public ExecutorApi(NetService netService, StoreService storeService) {
        this.storeService = storeService;
        netService.apiRegistry().register(io.dingodb.server.api.ExecutorApi.class, this);
    }

    @Override
    public boolean exist(CommonId tableId, byte[] primaryKey) {
        return storeService.getInstance(tableId).exist(primaryKey);
    }

    @Override
    public boolean upsertKeyValue(CommonId tableId, KeyValue row) {
        return storeService.getInstance(tableId).upsertKeyValue(row);
    }

    @Override
    public boolean upsertKeyValue(CommonId tableId, List<KeyValue> rows) {
        return storeService.getInstance(tableId).upsertKeyValue(rows);
    }

    @Override
    public boolean upsertKeyValue(CommonId tableId, byte[] primaryKey, byte[] row) {
        return storeService.getInstance(tableId).upsertKeyValue(primaryKey, row);
    }

    @Override
    public byte[] getValueByPrimaryKey(CommonId tableId, byte[] primaryKey) {
        return storeService.getInstance(tableId).getValueByPrimaryKey(primaryKey);
    }

    @Override
    public List<KeyValue> getKeyValueByPrimaryKeys(CommonId tableId, List<byte[]> primaryKeys) {
        return storeService.getInstance(tableId).getKeyValueByPrimaryKeys(primaryKeys);
    }

    @Override
    public boolean delete(CommonId tableId, byte[] key) {
        return storeService.getInstance(tableId).delete(key);
    }

    @Override
    public boolean delete(CommonId tableId, List<byte[]> primaryKeys) {
        return storeService.getInstance(tableId).delete(primaryKeys);
    }

    @Override
    public boolean deleteRange(CommonId tableId, byte[] startPrimaryKey, byte[] endPrimaryKey) {
        return storeService.getInstance(tableId).delete(startPrimaryKey, endPrimaryKey);
    }

    @Override
    public List<KeyValue> getKeyValueByRange(CommonId tableId, byte[] startPrimaryKey, byte[] endPrimaryKey) {
        if (log.isDebugEnabled()) {
            log.info("Get Key value by range: instance:{} tableId:{}, startPrimaryKey: {}, endPrimaryKey: {}",
                storeService.getInstance(tableId).getClass().getSimpleName(),
                tableId,
                startPrimaryKey == null ? "null" : new String(startPrimaryKey),
                endPrimaryKey == null ? "null" : new String(endPrimaryKey));
        }

        Iterator<KeyValue> rows = storeService
            .getInstance(tableId).keyValueScan(startPrimaryKey, endPrimaryKey);

        List<KeyValue> keyValues = new java.util.ArrayList<>();
        while (rows.hasNext()) {
            KeyValue keyValue = rows.next();
            keyValues.add(keyValue);
        }
        return keyValues;
    }

    @Override
    public Future<Object> operator(
        CommonId tableId,
        List<byte[]> startPrimaryKey,
        List<byte[]> endPrimaryKey,
        byte[] op) {
        return CompletableFuture.completedFuture(storeService.getInstance(tableId)
            .compute(startPrimaryKey, endPrimaryKey, op));
    }

    @Override
    public KeyValue udfGet(CommonId tableId, byte[] primaryKey,
                           String udfName, String functionName, int version) {
        return storeService.getInstance(tableId).udfGet(primaryKey, udfName, functionName, version);
    }

    @Override
    public boolean udfUpdate(CommonId tableId, byte[] primaryKey,
                             String udfName, String functionName, int version) {
        return storeService.getInstance(tableId).udfUpdate(primaryKey, udfName, functionName, version);
    }

    @Override
    public boolean updateTableDefinitionVersion(CommonId tableId, int version) {
        return false;
    }

    @Override
    public Iterator<KeyValue> getAllFinishedRecord(CommonId tableId) {
        return storeService.getInstance(tableId).keyValuePrefixScan(new byte[] {finishedTag});
    }

    @Override
    public Iterator<KeyValue> getAllUnfinishedRecord(CommonId tableId) {
        return storeService.getInstance(tableId).keyValuePrefixScan(new byte[] {unfinishTag});
    }

    @Override
    public boolean insertUnfinishedRecord(CommonId tableId, KeyValue record) {
        recordToUnFinish(record);
        return storeService.getInstance(tableId).upsertKeyValue(record);
    }

    @Override
    public boolean deleteUnfinishedRecord(CommonId tableId, byte[] key) {
        return storeService.getInstance(tableId).delete(getUnfinishedKey(key));
    }

    @Override
    public boolean insertFinishedRecord(CommonId tableId, byte[] key, int tableDefinitionVersion) {
        return false;
    }

    @Override
    public byte[] getUnfinishedKey(byte[] key) {
        byte[] unfinishKey = new byte[key.length + 1];
        System.arraycopy(key, 0, unfinishKey, 1, key.length);
        unfinishKey[0] = unfinishTag;
        return unfinishKey;
    }

    @Override
    public byte[] getFinishedKey(byte[] key) {
        byte[] finishedKey = new byte[key.length + 1];
        System.arraycopy(key, 0, finishedKey, 1, key.length);
        finishedKey[0] = finishedTag;
        return finishedKey;
    }

    @Override
    public boolean insertIndex(CommonId tableId, KeyValue record) {
        return false;
    }

    @Override
    public KeyValue getRecord(CommonId tableId, byte[] key) {
        return storeService.getInstance(tableId).getKeyValueByPrimaryKey(key);
    }

    @Override
    public void deleteIndex(CommonId tableId, KeyValue keyValue) {

    }

    @Override
    public void deleteFinishedRecord(CommonId tableId, byte[] key) {
        storeService.getInstance(tableId).delete(getFinishedKey(key));
    }

    @Override
    public Iterator<KeyValue> getFinishedRecord(CommonId tableId, List<KeyValue> records) {
        return null;
    }

    @Override
    public void insertDeleteKey(CommonId tableId, KeyValue keyValue) {

    }

    @Override
    public void deleteDeleteKey(CommonId tableId, byte[] key) {

    }

    @Override
    public Iterator<KeyValue> getAllDeleteRecord(CommonId tableId) {
        return null;
    }

    private void recordToUnFinish(KeyValue record) {
        byte[] primaryKey = record.getPrimaryKey();
        byte[] unfinishKey = new byte[primaryKey.length + 1];
        System.arraycopy(primaryKey, 0, unfinishKey, 1, primaryKey.length);
        unfinishKey[0] = unfinishTag;
        record.setKey(unfinishKey);
    }

    private void recordToFinished(KeyValue record) {
        byte[] primaryKey = record.getPrimaryKey();
        byte[] finishKey = new byte[primaryKey.length + 1];
        System.arraycopy(primaryKey, 0, finishKey, 1, primaryKey.length);
        finishKey[0] = finishedTag;
        record.setKey(finishKey);
    }
}
