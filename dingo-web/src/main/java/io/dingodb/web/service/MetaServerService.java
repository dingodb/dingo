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

package io.dingodb.web.service;

import io.dingodb.client.DingoClient;
import io.dingodb.client.common.IndexDefinition;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.common.util.Optional;
import io.dingodb.proxy.meta.MetaServiceGrpc;
import io.dingodb.proxy.meta.ProxyMeta;
import io.dingodb.sdk.common.index.Index;
import io.dingodb.sdk.common.index.IndexParameter;
import io.dingodb.sdk.common.index.VectorIndexParameter;
import io.dingodb.web.annotation.GrpcService;
import io.dingodb.web.utils.Conversion;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.web.utils.Conversion.mapping;

@GrpcService
public class MetaServerService extends MetaServiceGrpc.MetaServiceImplBase {

    @Autowired
    private DingoClient dingoClient;

    @Override
    public void createIndex(ProxyMeta.CreateIndexRequest req, StreamObserver<ProxyMeta.CreateIndexResponse> resObserver) {
        boolean createIndex = dingoClient.createIndex(req.getSchemaName(), req.getDefinition().getName(), mapping(req.getDefinition()));
        ProxyMeta.CreateIndexResponse response = ProxyMeta.CreateIndexResponse.newBuilder().setState(createIndex).build();
        resObserver.onNext(response);
        resObserver.onCompleted();
    }

    @Override
    public void updateIndex(ProxyMeta.UpdateIndexRequest req, StreamObserver<ProxyMeta.UpdateIndexResponse> resObserver) {
        boolean updateIndex = dingoClient.updateIndex(req.getSchemaName(), req.getDefinition().getName(), mapping(req.getDefinition()));
        ProxyMeta.UpdateIndexResponse response = ProxyMeta.UpdateIndexResponse.newBuilder().setState(updateIndex).build();

        resObserver.onNext(response);
        resObserver.onCompleted();
    }

    @Override
    public void updateMaxElements(ProxyMeta.UpdateMaxElementsRequest req, StreamObserver<ProxyMeta.UpdateMaxElementsResponse> resObserver) {
        Index oldIndex = dingoClient.getIndex(req.getSchemaName(), req.getIndexName());
        VectorIndexParameter vectorIndexParameter = oldIndex.getIndexParameter().getVectorIndexParameter();
        if (vectorIndexParameter.getHnswParam() == null) {
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
                new VectorIndexParameter(vectorIndexParameter.getVectorIndexType(), vectorIndexParameter.getHnswParam())))
            .build();

        boolean updatedIndex = dingoClient.updateIndex(req.getSchemaName(), req.getIndexName(), newIndex);

        ProxyMeta.UpdateMaxElementsResponse response = ProxyMeta.UpdateMaxElementsResponse.newBuilder().setState(updatedIndex).build();

        resObserver.onNext(response);
        resObserver.onCompleted();
    }

    @Override
    public void deleteIndex(ProxyMeta.DeleteIndexRequest req, StreamObserver<ProxyMeta.DeleteIndexResponse> resObserver) {
        boolean dropIndex = dingoClient.dropIndex(req.getSchemaName(), req.getIndexName());
        ProxyMeta.DeleteIndexResponse response = ProxyMeta.DeleteIndexResponse.newBuilder().setState(dropIndex).build();

        resObserver.onNext(response);
        resObserver.onCompleted();
    }

    @Override
    public void getIndex(ProxyMeta.GetIndexRequest req, StreamObserver<ProxyMeta.GetIndexResponse> resObserver) {
        Index index = dingoClient.getIndex(req.getSchemaName(), req.getIndexName());
        ProxyMeta.GetIndexResponse response = ProxyMeta.GetIndexResponse.newBuilder()
            .setDefinition(mapping(index))
            .build();

        resObserver.onNext(response);
        resObserver.onCompleted();
    }

    @Override
    public void getIndexes(ProxyMeta.GetIndexesRequest req, StreamObserver<ProxyMeta.GetIndexesResponse> resObserver) {
        List<Index> indexes = dingoClient.getIndexes(req.getSchemaName());
        ProxyMeta.GetIndexesResponse response = ProxyMeta.GetIndexesResponse.newBuilder()
            .addAllDefinitions(indexes.stream().map(Conversion::mapping).collect(Collectors.toList()))
            .build();

        resObserver.onNext(response);
        resObserver.onCompleted();
    }

    @Override
    public void getIndexNames(ProxyMeta.GetIndexNamesRequest req, StreamObserver<ProxyMeta.GetIndexNamesResponse> resObserver) {
        List<Index> indexes = dingoClient.getIndexes(req.getSchemaName());
        ProxyMeta.GetIndexNamesResponse response = ProxyMeta.GetIndexNamesResponse.newBuilder()
            .addAllNames(indexes.stream().map(Index::getName).collect(Collectors.toList()))
            .build();

        resObserver.onNext(response);
        resObserver.onCompleted();
    }
}
