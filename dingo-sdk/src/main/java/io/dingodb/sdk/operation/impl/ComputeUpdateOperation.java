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

@Slf4j
public class ComputeUpdateOperation implements IStoreOperation {
    private static final ComputeUpdateOperation INSTANCE = new ComputeUpdateOperation();

    private ComputeUpdateOperation() {
    }

    public static ComputeUpdateOperation getInstance() {
        return INSTANCE;
    }

    @Override
    public ResultForStore doOperation(ExecutorApi executorApi, CommonId tableId, ContextForStore paramContext) {
        try {
            if (paramContext == null || paramContext.getStartKeyListInBytes().size() < 1
                || paramContext.getOperationListInBytes().isEmpty()) {
                log.error("Parameters is null || table:{} has non key columns || compute operation is null", tableId);
            }

        } catch (Exception e) {

        }
        return null;
    }
}
