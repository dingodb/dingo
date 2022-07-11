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
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.sdk.common.Record;
import io.dingodb.server.api.ExecutorApi;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ComputeOperation implements IBaseStoreOperation {

    @Override
    public boolean doOperation(ExecutorApi executorApi, CommonId tableId, byte[] keyInBytes, byte[] recordInBytes) {
        return false;
    }

    @Override
    public Record doCompute(ExecutorApi executorApi, CommonId tableId, byte[] keyInBytes, byte[] computes) {
        byte[] bytes = executorApi.operator(tableId, keyInBytes, computes);
        return ProtostuffCodec.read(bytes);
    }
}
