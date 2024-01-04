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
import io.dingodb.client.common.IndexDefinition;
import io.dingodb.client.vector.VectorClient;
import io.dingodb.proxy.annotation.GrpcService;
import io.dingodb.proxy.common.ProxyCommon;
import io.dingodb.proxy.error.ProxyError;
import io.dingodb.proxy.meta.MetaServiceGrpc;
import io.dingodb.proxy.meta.ProxyMeta;
import io.dingodb.proxy.utils.Conversion;
import io.dingodb.sdk.service.entity.meta.IndexMetrics;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.proxy.utils.Conversion.mapping;

@Slf4j
@GrpcService
public class MetaServerService extends MetaServiceGrpc.MetaServiceImplBase {

    @Autowired
    private VectorClient vectorClient;

    @Override
    public void createIndex(
        ProxyMeta.CreateIndexRequest req,
        StreamObserver<ProxyMeta.CreateIndexResponse> resObserver) {
        ProxyMeta.CreateIndexResponse.Builder builder = ProxyMeta.CreateIndexResponse.newBuilder();
        ProxyError.Error.Builder error = ProxyError.Error.newBuilder();
        try {
            boolean createIndex = vectorClient.createIndex(req.getSchemaName(), mapping(req.getDefinition()));
            builder.setState(createIndex);
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
            vectorClient.updateMaxElements(req.getSchemaName(), req.getIndexName(), req.getMaxElements());
            builder.setState(true);
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
            boolean dropIndex = vectorClient.dropIndex(req.getSchemaName(), req.getIndexName());
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
            IndexDefinition index = vectorClient.getIndex(req.getSchemaName(), req.getIndexName());
            builder.setDefinition(mapping(index));
            error.setErrcode(ProxyError.Errno.OK);
        } catch (Exception e) {
            log.error("Get index error.", e);
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
            List<IndexDefinition> indexes = vectorClient.getIndexes(req.getSchemaName());
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
            List<IndexDefinition> indexes = vectorClient.getIndexes(req.getSchemaName());
            builder.addAllNames(indexes.stream().map(IndexDefinition::getName).collect(Collectors.toList()));
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
            IndexMetrics indexMetrics = vectorClient.getIndexMetrics(req.getSchemaName(), req.getIndexName());
            builder
                .setRowsCount(indexMetrics.getRowsCount())
                .setMinKey(ByteString.copyFrom(indexMetrics.getMinKey()))
                .setMaxKey(ByteString.copyFrom(indexMetrics.getMaxKey()))
                .setPartCount(indexMetrics.getPartCount())
                .setIndexType(ProxyCommon.VectorIndexType.valueOf(indexMetrics.getVectorIndexType().name()))
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
