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

import io.dingodb.sdk.service.entity.common.DocumentSearchParameter;
import io.dingodb.sdk.service.entity.common.ScalarField;
import io.dingodb.sdk.service.entity.common.ScalarFieldType;
import io.dingodb.sdk.service.entity.store.Op;
import io.dingodb.sdk.service.entity.store.TxnBatchGetRequest;
import io.dingodb.sdk.service.entity.store.TxnBatchRollbackRequest;
import io.dingodb.sdk.service.entity.store.TxnCheckTxnStatusRequest;
import io.dingodb.sdk.service.entity.store.TxnCommitRequest;
import io.dingodb.sdk.service.entity.store.TxnPessimisticLockRequest;
import io.dingodb.sdk.service.entity.store.TxnPessimisticRollbackRequest;
import io.dingodb.sdk.service.entity.store.TxnPrewriteRequest;
import io.dingodb.sdk.service.entity.store.TxnScanRequest;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.DocumentValue;
import io.dingodb.store.api.transaction.data.DocumentWithScore;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.sdk.service.entity.store.TxnResolveLockRequest;
import io.dingodb.store.api.transaction.data.checkstatus.TxnCheckStatus;
import io.dingodb.store.api.transaction.data.commit.TxnCommit;
import io.dingodb.store.api.transaction.data.pessimisticlock.TxnPessimisticLock;
import io.dingodb.store.api.transaction.data.prewrite.TxnPreWrite;
import io.dingodb.store.api.transaction.data.resolvelock.TxnResolveLock;
import io.dingodb.store.api.transaction.data.rollback.TxnBatchRollBack;
import io.dingodb.store.api.transaction.data.rollback.TxnPessimisticRollBack;
import org.mapstruct.Mapping;
import org.mapstruct.Mappings;

import java.util.List;

public interface TxnMapper {

    @Mapping(source = "isolationLevel", target = "context.isolationLevel")
    TxnPrewriteRequest preWriteTo(TxnPreWrite preWrite);

    @Mapping(source = "isolationLevel", target = "context.isolationLevel")
    TxnCommitRequest commitTo(TxnCommit commit);

    @Mapping(source = "isolationLevel", target = "context.isolationLevel")
    TxnPessimisticLockRequest pessimisticLockTo(TxnPessimisticLock pessimisticLock);

    @Mapping(source = "isolationLevel", target = "context.isolationLevel")
    TxnPessimisticRollbackRequest pessimisticRollBackTo(TxnPessimisticRollBack txnPessimisticRollBack);

    @Mapping(source = "isolationLevel", target = "context.isolationLevel")
    TxnBatchRollbackRequest rollbackTo(TxnBatchRollBack rollBack);

    @Mappings({
        @Mapping(source = "isolationLevel", target = "context.isolationLevel"),
        @Mapping(source = "range.start", target = "range.range.startKey"),
        @Mapping(source = "range.end", target = "range.range.endKey"),
        @Mapping(source = "range.withStart", target = "range.withStart"),
        @Mapping(source = "range.withEnd", target = "range.withEnd")
    })
    TxnScanRequest scanTo(long startTs, IsolationLevel isolationLevel, StoreInstance.Range range);

    @Mapping(source = "isolationLevel", target = "context.isolationLevel")
    TxnBatchGetRequest batchGetTo(long startTs, IsolationLevel isolationLevel, List<byte[]> keys);

    TxnCheckTxnStatusRequest checkTxnTo(TxnCheckStatus txnCheck);

    @Mapping(source = "isolationLevel", target = "context.isolationLevel")
    TxnResolveLockRequest resolveTxnTo(TxnResolveLock txnResolve);

    default Op opTo(io.dingodb.store.api.transaction.data.Op op) {
        return Op.forNumber(op.getCode());
    }

    DocumentSearchParameter documentSearchParamTo(io.dingodb.store.api.transaction.data.DocumentSearchParameter documentSearchParameter);

    DocumentWithScore documentWithScoreTo(io.dingodb.sdk.service.entity.common.DocumentWithScore documentWithScore);

    default DocumentValue documentValueTo(io.dingodb.sdk.service.entity.common.DocumentValue documentValue) {
        return DocumentValue.builder()
            .fieldType(scalarFieldTypeTo(documentValue.getFieldType()))
            .fieldValue(scalarFieldTo(documentValue.getFieldValue()))
            .build();
    }

    default DocumentValue.ScalarFieldType scalarFieldTypeTo(io.dingodb.sdk.service.entity.common.ScalarFieldType scalarFieldType) {
        switch (scalarFieldType) {
            case BOOL:
                return DocumentValue.ScalarFieldType.BOOL;
            case INT32:
                return DocumentValue.ScalarFieldType.INTEGER;
            case INT64:
                return DocumentValue.ScalarFieldType.LONG;
            case FLOAT32:
                return DocumentValue.ScalarFieldType.FLOAT;
            case DOUBLE:
                return DocumentValue.ScalarFieldType.DOUBLE;
            case STRING:
                return DocumentValue.ScalarFieldType.STRING;
            case BYTES:
                return DocumentValue.ScalarFieldType.BYTES;
            default:
                throw new IllegalStateException("Unexpected value: " + scalarFieldType);
        }
    }


    default io.dingodb.store.api.transaction.data.ScalarField scalarFieldTo(
        ScalarField field
    ) {
        switch (field.getData().nest()) {
            case BOOL_DATA:
                return io.dingodb.store.api.transaction.data.ScalarField.builder()
                    .data(((ScalarField.DataNest.BoolData) field.getData()).isValue())
                    .build();
            case INT_DATA:
                return io.dingodb.store.api.transaction.data.ScalarField.builder()
                    .data(((ScalarField.DataNest.IntData) field.getData()).getValue())
                    .build();
            case LONG_DATA:
                return io.dingodb.store.api.transaction.data.ScalarField.builder()
                    .data(((ScalarField.DataNest.LongData) field.getData()).getValue())
                    .build();
            case FLOAT_DATA:
                return io.dingodb.store.api.transaction.data.ScalarField.builder()
                    .data(((ScalarField.DataNest.FloatData) field.getData()).getValue())
                    .build();
            case DOUBLE_DATA:
                return io.dingodb.store.api.transaction.data.ScalarField.builder()
                    .data(((ScalarField.DataNest.DoubleData) field.getData()).getValue())
                    .build();
            case STRING_DATA:
                return io.dingodb.store.api.transaction.data.ScalarField.builder()
                    .data(((ScalarField.DataNest.StringData) field.getData()).getValue())
                    .build();
            case BYTES_DATA:
                return io.dingodb.store.api.transaction.data.ScalarField.builder()
                    .data(((ScalarField.DataNest.BytesData) field.getData()).getValue())
                    .build();
            default:
                throw new IllegalStateException("Unexpected value: " + field.getData().nest());
        }
    }

    default io.dingodb.sdk.service.entity.common.DocumentValue documentValueTo(DocumentValue documentValue) {
        return io.dingodb.sdk.service.entity.common.DocumentValue.builder()
            .fieldType(scalarFieldTypeTo(documentValue.getFieldType()))
            .fieldValue(scalarFieldTo(documentValue.getFieldValue(), documentValue.getFieldType()))
            .build();
    }

    default ScalarFieldType scalarFieldTypeTo(DocumentValue.ScalarFieldType fieldType) {
        switch (fieldType) {
            case BOOL:
                return ScalarFieldType.BOOL;
            case INTEGER:
                return ScalarFieldType.INT32;
            case LONG:
                return ScalarFieldType.INT64;
            case FLOAT:
                return ScalarFieldType.FLOAT32;
            case DOUBLE:
                return ScalarFieldType.DOUBLE;
            case STRING:
                return ScalarFieldType.STRING;
            case BYTES:
                return ScalarFieldType.BYTES;
            default:
                throw new IllegalStateException("Unexpected value: " + fieldType);
        }
    }

    default ScalarField scalarFieldTo(
        io.dingodb.store.api.transaction.data.ScalarField field,
        DocumentValue.ScalarFieldType type
    ) {
        switch (type) {
            case BOOL:
                return ScalarField.builder().data(ScalarField.DataNest.BoolData.of((Boolean) field.getData())).build();
            case INTEGER:
                return ScalarField.builder().data(ScalarField.DataNest.IntData.of((Integer) field.getData())).build();
            case LONG:
                return ScalarField.builder().data(ScalarField.DataNest.LongData.of((Long) field.getData())).build();
            case FLOAT:
                return ScalarField.builder().data(ScalarField.DataNest.FloatData.of((Float) field.getData())).build();
            case DOUBLE:
                return ScalarField.builder().data(ScalarField.DataNest.DoubleData.of((Double) field.getData())).build();
            case STRING:
                return ScalarField.builder().data(ScalarField.DataNest.StringData.of((String) field.getData())).build();
            case BYTES:
                return ScalarField.builder().data(ScalarField.DataNest.BytesData.of((byte[]) field.getData())).build();
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

}
