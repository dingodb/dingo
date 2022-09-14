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
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.common.operation.DingoExecResult;
import io.dingodb.common.operation.Operation;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.net.NetService;
import io.dingodb.store.api.StoreService;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

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
    public List<DingoExecResult> operator(CommonId tableId,
                                          List<byte[]> startKeys,
                                          List<byte[]> endKeys,
                                          List<byte[]> operations) {
        List<Operation> operationList = operations.stream()
            .map(ProtostuffCodec::<Operation>read)
            .collect(Collectors.toList());
        List<DingoExecResult> results = new ArrayList<>();
        List<byte[]> writeOperations = new ArrayList<>();
        List<Operation> readOperations = new ArrayList<>();
        for (int i = 0; i < operationList.size(); i++) {
            if (operationList.get(i).operationType.isWriteable()) {
                writeOperations.add(operations.get(i));
            } else {
                readOperations.add(operationList.get(i));
            }
        }
        byte[] end = endKeys == null ? null : endKeys.get(0);
        if (writeOperations.size() > 0) {
            boolean isOK = storeService.getInstance(tableId).compute(startKeys.get(0), end, writeOperations);
            results.add(new DingoExecResult(isOK, "OK"));
        }

        for (Operation operation : readOperations) {
            Iterator<KeyValue> iterator = storeService.getInstance(tableId).keyValueScan(startKeys.get(0), end);
            DingoExecResult value = (DingoExecResult)
                operation.operationType.executive().execute(operation.operationContext, iterator);
            results.add(value);

        }
        return results;
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
}
