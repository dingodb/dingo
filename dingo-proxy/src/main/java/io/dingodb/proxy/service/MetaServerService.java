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

package io.dingodb.proxy.service;

import com.google.protobuf.ByteString;
import io.dingodb.client.DingoClient;
import io.dingodb.client.common.IndexDefinition;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.common.util.Optional;
import io.dingodb.proxy.common.ProxyCommon;
import io.dingodb.proxy.error.ProxyError;
import io.dingodb.proxy.meta.MetaServiceGrpc;
import io.dingodb.proxy.meta.ProxyMeta;
import io.dingodb.sdk.common.index.Index;
import io.dingodb.sdk.common.index.IndexMetrics;
import io.dingodb.sdk.common.index.IndexParameter;
import io.dingodb.sdk.common.index.VectorIndexParameter;
import io.dingodb.proxy.annotation.GrpcService;
import io.dingodb.proxy.utils.Conversion;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.proxy.utils.Conversion.mapping;

@GrpcService
public class MetaServerService extends MetaServiceGrpc.MetaServiceImplBase {

    @Autowired
    private DingoClient dingoClient;

    @Override
    public void createIndex(
        ProxyMeta.CreateIndexRequest req,
        StreamObserver<ProxyMeta.CreateIndexResponse> resObserver) {
        ProxyMeta.CreateIndexResponse.Builder builder = ProxyMeta.CreateIndexResponse.newBuilder();
        ProxyError.Error.Builder error = ProxyError.Error.newBuilder();
        try {
            boolean createIndex = dingoClient.createIndex(
                req.getSchemaName(),
                req.getDefinition().getName(),
                mapping(req.getDefinition()));
            builder.setState(createIndex);
            error.setErrcode(ProxyError.Errno.OK);
        } catch (Exception e) {
            error.setErrcode(ProxyError.Errno.EINTERNAL).setErrmsg(e.getMessage());
        }
        resObserver.onNext(builder.setError(error.build()).build());
        resObserver.onCompleted();
    }

    @Override
    public void updateIndex(
        ProxyMeta.UpdateIndexRequest req,
        StreamObserver<ProxyMeta.UpdateIndexResponse> resObserver) {
        ProxyMeta.UpdateIndexResponse.Builder builder = ProxyMeta.UpdateIndexResponse.newBuilder();
        ProxyError.Error.Builder error = ProxyError.Error.newBuilder();

        try {
            boolean updateIndex = dingoClient.updateIndex(
                req.getSchemaName(),
                req.getDefinition().getName(),
                mapping(req.getDefinition()));
            builder.setState(updateIndex);
            error.setErrcode(ProxyError.Errno.OK);
        } catch (Exception e) {
            error.setErrcode(ProxyError.Errno.EINTERNAL).setErrmsg(e.getMessage());
        }

        resObserver.onNext(builder.setError(error.build()).build());
        resObserver.onCompleted();
    }

    @Override
    public void updateMaxElements(ProxyMeta.UpdateMaxElementsRequest req, StreamObserver<ProxyMeta.UpdateMaxElementsResponse> resObserver) {
        ProxyMeta.UpdateMaxElementsResponse.Builder builder = ProxyMeta.UpdateMaxElementsResponse.newBuilder();
        ProxyError.Error.Builder error = ProxyError.Error.newBuilder();

        try {
            Index oldIndex = dingoClient.getIndex(req.getSchemaName(), req.getIndexName());
            VectorIndexParameter vectorIndexParameter = oldIndex.getIndexParameter().getVectorIndexParameter();
            if (vectorIndexParameter.getHnswParam() == null) {
                error.setErrcode(ProxyError.Errno.EINTERNAL).setErrmsg("Only max_elements in hnsw can be modified");
                return;
            }
            vectorIndexParameter.getHnswParam().setMaxElements(req.getMaxElements());
            IndexDefinition newIndex = IndexDefinition.builder()
                .name(req.getIndexName())
                .replica(oldIndex.getReplica())
                .version(oldIndex.getVersion())
                .isAutoIncrement(oldIndex.getIsAutoIncrement())
                .autoIncrement(oldIndex.getAutoIncrement())
                .indexPartition(Optional.mapOrGet(oldIndex.getIndexPartition(), __ -> new PartitionDefinition(
                    oldIndex.getIndexPartition().getFuncName(),
                    oldIndex.getIndexPartition().getCols(),
                    oldIndex.getIndexPartition().getDetails().stream()
                        .map(d -> new PartitionDetailDefinition(d.getPartName(), d.getOperator(), d.getOperand()))
                        .collect(Collectors.toList())), () -> null))
                .indexParameter(new IndexParameter(
                    oldIndex.getIndexParameter().getIndexType(),
                    new VectorIndexParameter(
                        vectorIndexParameter.getVectorIndexType(),
                        vectorIndexParameter.getHnswParam())))
                .build();

            boolean updatedIndex = dingoClient.updateIndex(req.getSchemaName(), req.getIndexName(), newIndex);
            builder.setState(updatedIndex);

            error.setErrcode(ProxyError.Errno.OK);
        } catch (Exception e) {
            error.setErrcode(ProxyError.Errno.EINTERNAL).setErrmsg(e.getMessage());
        }

        resObserver.onNext(builder.setError(error.build()).build());
        resObserver.onCompleted();
    }

    @Override
    public void deleteIndex(ProxyMeta.DeleteIndexRequest req, StreamObserver<ProxyMeta.DeleteIndexResponse> resObserver) {
        ProxyMeta.DeleteIndexResponse.Builder builder = ProxyMeta.DeleteIndexResponse.newBuilder();
        ProxyError.Error.Builder error = ProxyError.Error.newBuilder();
        try {
            boolean dropIndex = dingoClient.dropIndex(req.getSchemaName(), req.getIndexName());
            builder.setState(dropIndex);
            error.setErrcode(ProxyError.Errno.OK);
        } catch (Exception e) {
            error.setErrcode(ProxyError.Errno.EINTERNAL).setErrmsg(e.getMessage());
        }

        resObserver.onNext(builder.setError(error.build()).build());
        resObserver.onCompleted();
    }

    @Override
    public void getIndex(ProxyMeta.GetIndexRequest req, StreamObserver<ProxyMeta.GetIndexResponse> resObserver) {
        ProxyMeta.GetIndexResponse.Builder builder = ProxyMeta.GetIndexResponse.newBuilder();
        ProxyError.Error.Builder error = ProxyError.Error.newBuilder();
        try {
            Index index = dingoClient.getIndex(req.getSchemaName(), req.getIndexName());
            builder.setDefinition(mapping(index));
            error.setErrcode(ProxyError.Errno.OK);
        } catch (Exception e) {
            error.setErrcode(ProxyError.Errno.EINTERNAL).setErrmsg(e.getMessage());
        }

        resObserver.onNext(builder.setError(error.build()).build());
        resObserver.onCompleted();
    }

    @Override
    public void getIndexes(ProxyMeta.GetIndexesRequest req, StreamObserver<ProxyMeta.GetIndexesResponse> resObserver) {
        ProxyMeta.GetIndexesResponse.Builder builder = ProxyMeta.GetIndexesResponse.newBuilder();
        ProxyError.Error.Builder error = ProxyError.Error.newBuilder();
        try {
            List<Index> indexes = dingoClient.getIndexes(req.getSchemaName());
            builder.addAllDefinitions(indexes.stream().map(Conversion::mapping).collect(Collectors.toList()));
            error.setErrcode(ProxyError.Errno.OK);
        } catch (Exception e) {
            error.setErrcode(ProxyError.Errno.EINTERNAL).setErrmsg(e.getMessage());
        }

        resObserver.onNext(builder.setError(error.build()).build());
        resObserver.onCompleted();
    }

    @Override
    public void getIndexNames(ProxyMeta.GetIndexNamesRequest req, StreamObserver<ProxyMeta.GetIndexNamesResponse> resObserver) {
        ProxyMeta.GetIndexNamesResponse.Builder builder = ProxyMeta.GetIndexNamesResponse.newBuilder();
        ProxyError.Error.Builder error = ProxyError.Error.newBuilder();
        try {
            List<Index> indexes = dingoClient.getIndexes(req.getSchemaName());
            builder.addAllNames(indexes.stream().map(Index::getName).collect(Collectors.toList()));
            error.setErrcode(ProxyError.Errno.OK);
        } catch (Exception e) {
            error.setErrcode(ProxyError.Errno.EINTERNAL).setErrmsg(e.getMessage());
        }

        resObserver.onNext(builder.setError(error.build()).build());
        resObserver.onCompleted();
    }

    @Override
    public void getIndexMetrics(ProxyMeta.GetIndexMetricsRequest req, StreamObserver<ProxyMeta.GetIndexMetricsResponse> resObserver) {
        ProxyMeta.GetIndexMetricsResponse.Builder builder = ProxyMeta.GetIndexMetricsResponse.newBuilder();
        ProxyError.Error.Builder error = ProxyError.Error.newBuilder();
        try {
            IndexMetrics indexMetrics = dingoClient.getIndexMetrics(req.getSchemaName(), req.getIndexName());
            builder
                .setRowsCount(indexMetrics.getRowsCount())
                .setMinKey(ByteString.copyFrom(indexMetrics.getMinKey()))
                .setMaxKey(ByteString.copyFrom(indexMetrics.getMaxKey()))
                .setPartCount(indexMetrics.getPartCount())
                .setIndexType(ProxyCommon.VectorIndexType.valueOf(indexMetrics.getIndexType().name()))
                .setCurrentCount(indexMetrics.getCurrentCount())
                .setDeletedCount(indexMetrics.getDeletedCount())
                .setMaxId(indexMetrics.getMaxId())
                .setMinId(indexMetrics.getMinId())
                .setMemoryBytes(indexMetrics.getMemoryBytes());
            error.setErrcode(ProxyError.Errno.OK);
        } catch (Exception e) {
            error.setErrcode(ProxyError.Errno.EINTERNAL).setErrmsg(e.getMessage());
        }

        resObserver.onNext(builder.setError(error.build()).build());
        resObserver.onCompleted();
    }
}
