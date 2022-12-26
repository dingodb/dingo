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
import io.dingodb.common.privilege.DingoSqlAccessEnum;
import io.dingodb.common.store.KeyValue;
import io.dingodb.net.Channel;
import io.dingodb.net.NetService;
import io.dingodb.store.api.StoreService;
import io.dingodb.verify.privilege.PrivilegeVerify;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

@Slf4j
public class ExecutorApi implements io.dingodb.server.api.ExecutorApi {

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
    public boolean upsertKeyValue(Channel channel, CommonId schema, CommonId tableId, KeyValue row) {
        verify(channel, schema, tableId, DingoSqlAccessEnum.INSERT);
        return storeService.getInstance(tableId).upsertKeyValue(row);

    }

    @Override
    public boolean upsertKeyValue(Channel channel, CommonId schema, CommonId tableId, List<KeyValue> rows) {
        verify(channel, schema, tableId, DingoSqlAccessEnum.INSERT);
        return storeService.getInstance(tableId).upsertKeyValue(rows);
    }

    @Override
    public boolean upsertKeyValue(Channel channel, CommonId schema, CommonId tableId, byte[] primaryKey, byte[] row) {
        verify(channel, schema, tableId, DingoSqlAccessEnum.INSERT);
        return storeService.getInstance(tableId).upsertKeyValue(primaryKey, row);
    }

    @Override
    public byte[] getValueByPrimaryKey(Channel channel, CommonId schema, CommonId tableId, byte[] primaryKey) {
        verify(channel, schema, tableId, DingoSqlAccessEnum.SELECT);
        return storeService.getInstance(tableId).getValueByPrimaryKey(primaryKey);
    }

    @Override
    public List<KeyValue> getKeyValueByPrimaryKeys(Channel channel, CommonId schema, CommonId tableId,
                                                   List<byte[]> primaryKeys) {
        verify(channel, schema, tableId, DingoSqlAccessEnum.SELECT);
        return storeService.getInstance(tableId).getKeyValueByPrimaryKeys(primaryKeys);
    }

    @Override
    public boolean delete(Channel channel, CommonId schema, CommonId tableId, byte[] key) {
        verify(channel, schema, tableId, DingoSqlAccessEnum.DELETE);
        return storeService.getInstance(tableId).delete(key);
    }

    @Override
    public boolean delete(Channel channel, CommonId schema, CommonId tableId, List<byte[]> primaryKeys) {
        verify(channel, schema, tableId, DingoSqlAccessEnum.DELETE);
        return storeService.getInstance(tableId).delete(primaryKeys);
    }

    @Override
    public boolean deleteRange(Channel channel, CommonId schema, CommonId tableId,
                               byte[] startPrimaryKey, byte[] endPrimaryKey) {
        verify(channel, schema, tableId, DingoSqlAccessEnum.DELETE);
        return storeService.getInstance(tableId).delete(startPrimaryKey, endPrimaryKey);
    }

    @Override
    public List<KeyValue> getKeyValueByRange(Channel channel, CommonId schema, CommonId tableId,
                                             byte[] startPrimaryKey, byte[] endPrimaryKey) {
        verify(channel, schema, tableId, DingoSqlAccessEnum.SELECT);
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

    private void verify(Channel channel, CommonId schema, CommonId tableId, DingoSqlAccessEnum accessType) {
        boolean verify = PrivilegeVerify.verify(channel, schema, tableId, accessType);
        if (log.isDebugEnabled()) {
            log.debug("Verify:{}, access type: {}", verify, accessType.name());
        }
        if (!verify) {
            throw new RuntimeException("Access denied for user");
        }
    }

}
