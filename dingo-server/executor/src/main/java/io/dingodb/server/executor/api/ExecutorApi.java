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
import io.dingodb.common.auth.Authentication;
import io.dingodb.common.store.KeyValue;
import io.dingodb.net.Channel;
import io.dingodb.net.NetService;
import io.dingodb.store.api.StoreService;
import io.dingodb.verify.privilege.PrivilegeVerify;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
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
        if (verify(channel, schema, tableId, "insert")) {
            return storeService.getInstance(tableId).upsertKeyValue(row);
        }
        return false;
    }

    @Override
    public boolean upsertKeyValue(Channel channel, CommonId schema, CommonId tableId, List<KeyValue> rows) {
        if (verify(channel, schema, tableId, "insert")) {
            return storeService.getInstance(tableId).upsertKeyValue(rows);
        }
        return false;
    }

    @Override
    public boolean upsertKeyValue(Channel channel, CommonId schema, CommonId tableId, byte[] primaryKey, byte[] row) {
        if (verify(channel, schema, tableId, "insert")) {
            storeService.getInstance(tableId).upsertKeyValue(primaryKey, row);
        }
        return false;
    }

    @Override
    public byte[] getValueByPrimaryKey(Channel channel, CommonId schema, CommonId tableId, byte[] primaryKey) {
        if (verify(channel, schema, tableId, "select")) {
            return storeService.getInstance(tableId).getValueByPrimaryKey(primaryKey);
        }
        return new byte[]{0};
    }

    @Override
    public List<KeyValue> getKeyValueByPrimaryKeys(Channel channel, CommonId schema, CommonId tableId,
                                                   List<byte[]> primaryKeys) {
        if (verify(channel, schema, tableId, "select")) {
            storeService.getInstance(tableId).getKeyValueByPrimaryKeys(primaryKeys);
        }
        return Collections.emptyList();
    }

    @Override
    public boolean delete(Channel channel, CommonId schema, CommonId tableId, byte[] key) {
        if (verify(channel, schema, tableId, "delete")) {
            return storeService.getInstance(tableId).delete(key);
        }
        return false;
    }

    @Override
    public boolean delete(Channel channel, CommonId schema, CommonId tableId, List<byte[]> primaryKeys) {
        if (verify(channel, schema, tableId, "delete")) {
            storeService.getInstance(tableId).delete(primaryKeys);
        }
        return false;
    }

    @Override
    public boolean deleteRange(Channel channel, CommonId schema, CommonId tableId,
                               byte[] startPrimaryKey, byte[] endPrimaryKey) {
        if (verify(channel, schema, tableId, "delete")) {
            storeService.getInstance(tableId).delete(startPrimaryKey, endPrimaryKey);
        }
        return false;
    }

    @Override
    public List<KeyValue> getKeyValueByRange(Channel channel, CommonId schema, CommonId tableId,
                                             byte[] startPrimaryKey, byte[] endPrimaryKey) {
        if (verify(channel, schema, tableId, "select")) {
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
        return Collections.emptyList();
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

    private boolean verify(Channel channel, CommonId schema, CommonId tableId, String accessType) {
        Object[] objects = channel.auth().get("token");
        Authentication authentication = (Authentication) objects[0];
        boolean verify = PrivilegeVerify.verify(authentication.getUsername(), authentication.getHost(),
            schema, tableId , accessType);
        log.info("verify:" + verify);
        return verify;
    }

}
