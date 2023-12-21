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

package io.dingodb.store.proxy.mapper;

import io.dingodb.sdk.service.entity.store.Op;
import io.dingodb.sdk.service.entity.store.TxnBatchGetRequest;
import io.dingodb.sdk.service.entity.store.TxnBatchRollbackRequest;
import io.dingodb.sdk.service.entity.store.TxnCommitRequest;
import io.dingodb.sdk.service.entity.store.TxnPrewriteRequest;
import io.dingodb.sdk.service.entity.store.TxnScanRequest;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.commit.TxnCommit;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;
import io.dingodb.store.api.transaction.data.rollback.TxnBatchRollBack;
import org.mapstruct.Mapping;

import java.util.List;

public interface TxnMapper {

    @Mapping(source = "isolationLevel", target = "context.isolationLevel")
    TxnPrewriteRequest preWriteTo(TxnPreWrite preWrite);

    @Mapping(source = "isolationLevel", target = "context.isolationLevel")
    TxnCommitRequest commitTo(TxnCommit commit);

    @Mapping(source = "isolationLevel", target = "context.isolationLevel")
    TxnBatchRollbackRequest rollbackTo(TxnBatchRollBack rollBack);

    @Mapping(source = "isolationLevel", target = "context.isolationLevel")
    TxnScanRequest scanTo(long startTs, IsolationLevel isolationLevel, StoreInstance.Range range);

    @Mapping(source = "isolationLevel", target = "context.isolationLevel")
    TxnBatchGetRequest batchGetTo(long startTs, IsolationLevel isolationLevel, List<byte[]> keys);


    default Op opTo(io.dingodb.store.api.transaction.data.Op op) {
        return Op.forNumber(op.getCode());
    }

}
