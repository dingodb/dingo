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

package io.dingodb.sdk.operation.impl;

import io.dingodb.common.CommonId;
import io.dingodb.common.operation.context.OperationContext;
import io.dingodb.common.store.KeyValue;
import io.dingodb.sdk.operation.ContextForStore;
import io.dingodb.sdk.operation.IStoreOperation;
import io.dingodb.sdk.operation.ResultForStore;
import io.dingodb.server.api.ExecutorApi;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class QueryOperation implements IStoreOperation {

    public static final QueryOperation instance = new QueryOperation();

    public QueryOperation() {
    }

    public static QueryOperation getInstance() {
        return instance;
    }

    @Override
    public ResultForStore doOperation(ExecutorApi executorApi,
                                      CommonId tableId,
                                      ContextForStore parameters) {
        try {
            if (parameters == null || parameters.getStartKeyListInBytes().size() < 1
                || parameters.getEndKeyListInBytes().size() < 1) {
                log.error("Parameters is null || table:{} has non key columns", tableId);
                return new ResultForStore(false, "Invalid parameters for query operation");
            }
            List<byte[]> startKeys = parameters.getStartKeyListInBytes();
            List<byte[]> endKeys = parameters.getEndKeyListInBytes();
            List<KeyValue> keyValueList = executorApi.getKeyValueByRange(tableId, startKeys.get(0), endKeys.get(0));
            OperationContext context = parameters.getContext();
            if (context == null) {
                return new ResultForStore(true, "OK", keyValueList);
            }
            List<KeyValue> recordList = keyValueList.stream()
                .filter(kv -> context.filter.filter(context, kv))
                .collect(Collectors.toList());
            return new ResultForStore(true, "OK", recordList);
        } catch (Exception e) {
            log.error("deQuery KeyValue error, tableId:{}, exception:{}", tableId, e);
            throw e;
        }

    }
}
