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
import io.dingodb.common.store.KeyValue;
import io.dingodb.sdk.operation.ContextForStore;
import io.dingodb.sdk.operation.IStoreOperation;
import io.dingodb.sdk.operation.ResultForStore;
import io.dingodb.server.api.ExecutorApi;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class PutOperation implements IStoreOperation {

    private static final PutOperation instance = new PutOperation();

    private PutOperation() {
    }

    public static PutOperation getInstance() {
        return instance;
    }

    @Override
    public ResultForStore doOperation(ExecutorApi executorApi,
                                      CommonId tableId,
                                      ContextForStore parameters) {
        try {
            if (parameters == null
                || parameters.getStartKeyListInBytes().size() != parameters.getRecordList().size()) {
                log.error("Parameters is null || table:{} has non key columns", tableId);
                String errorMsg = "Invalid parameters for put operation";
                return new ResultForStore(false, errorMsg);
            }
            List<KeyValue> keyValueList = parameters.getRecordList();
            boolean isSuccess = executorApi.upsertKeyValue(tableId, keyValueList);
            return new ResultForStore(isSuccess, "OK");
        } catch (Exception e) {
            log.error("put table:{} by KeyValue catch exception:{}", tableId, e.toString(), e);
            throw e;
        }
    }
}
