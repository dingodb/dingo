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
import io.dingodb.sdk.operation.UDFContext;
import io.dingodb.server.api.ExecutorApi;
import io.dingodb.verify.service.ExecutorService;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class UdfUpdateOperation implements IStoreOperation {

    private static final UdfUpdateOperation instance = new UdfUpdateOperation();

    private UdfUpdateOperation() {
    }

    public static UdfUpdateOperation getInstance() {
        return instance;
    }

    @Override
    public ResultForStore doOperation(ExecutorService executorService,
                                      CommonId tableId,
                                      ContextForStore parameters) {
        try {
            if (parameters == null
                || parameters.getStartKeyListInBytes().size() != parameters.getRecordList().size()) {
                log.error("Parameters is null || table:{} has non key columns", tableId);
                String errorMsg = "Invalid parameters for put operation";
                return new ResultForStore(false, errorMsg);
            }
            List<byte[]> keyList = parameters.getStartKeyListInBytes();
            List<KeyValue> failedKeyList = new ArrayList<>();
            UDFContext udfContext = parameters.getUdfContext();
            for (byte[] primaryKey : keyList) {
                if (!executorService.udfUpdate(tableId, primaryKey, udfContext.getUdfName(),
                    udfContext.getFunctionName(), udfContext.getUdfVersion())) {
                    failedKeyList.add(new KeyValue(primaryKey, null));
                }
            }
            if (failedKeyList.size() == 0) {
                return new ResultForStore(true, "OK");
            }
            return new ResultForStore(false, "udfUpdate Failed", failedKeyList);
        } catch (Exception e) {
            log.error("udfUpdate table:{} by KeyValue catch exception:{}", tableId, e.toString(), e);
            throw e;
        }
    }
}
