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
import io.dingodb.sdk.operation.ContextForStore;
import io.dingodb.sdk.operation.IStoreOperation;
import io.dingodb.sdk.operation.ResultForStore;
import io.dingodb.server.api.ExecutorApi;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class DeleteOperation implements IStoreOperation {

    private static final DeleteOperation instance = new DeleteOperation();

    private DeleteOperation() {
    }

    public static DeleteOperation getInstance() {
        return instance;
    }

    @Override
    public ResultForStore doOperation(ExecutorApi executorApi,
                                      CommonId tableId,
                                      ContextForStore parameters) {
        try {
            if (parameters == null
                || parameters.getKeyListInBytes().size() < 1) {
                String errorMessage = "Delete operation requires at least one key";
                return new ResultForStore(false, errorMessage);
            }
            List<byte[]> keyList = parameters.getKeyListInBytes();
            boolean result = executorApi.delete(tableId, keyList);
            return new ResultForStore(result, "OK");
        } catch (Exception e) {
            log.error("delete KeyValue error, tableId:{}, exception:{}", tableId, e.toString(), e);
            throw e;
        }
    }
}
