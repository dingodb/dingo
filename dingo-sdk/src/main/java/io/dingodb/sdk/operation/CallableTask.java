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

package io.dingodb.sdk.operation;

import io.dingodb.common.CommonId;
import io.dingodb.server.api.ExecutorApi;

import java.util.concurrent.Callable;

public class CallableTask implements Callable<ResultForStore> {

    private final ExecutorApi executorApi;
    private final IStoreOperation storeOperation;

    private final CommonId tableId;

    private final ContextForStore context;

    public CallableTask(ExecutorApi executorApi,
                        IStoreOperation storeOperation,
                        CommonId tableId,
                        ContextForStore context) {
        this.executorApi = executorApi;
        this.storeOperation = storeOperation;
        this.tableId = tableId;
        this.context = context;
    }

    @Override
    public ResultForStore call() throws Exception {
        ResultForStore subResult = storeOperation.doOperation(executorApi, tableId, context);
        return subResult;
    }
}
