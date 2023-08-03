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
import io.dingodb.proxy.meta.MetaServiceGrpc;
import io.dingodb.proxy.meta.ProxyMeta;
import io.dingodb.web.annotation.GrpcService;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;

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
        super.updateIndex(req, resObserver);
    }

    @Override
    public void updateMaxElements(ProxyMeta.UpdateMaxElementsRequest req, StreamObserver<ProxyMeta.UpdateMaxElementsResponse> resObserver) {
        super.updateMaxElements(req, resObserver);
    }

    @Override
    public void deleteIndex(ProxyMeta.DeleteIndexRequest req, StreamObserver<ProxyMeta.DeleteIndexResponse> resObserver) {
        super.deleteIndex(req, resObserver);
    }

    @Override
    public void getIndex(ProxyMeta.GetIndexRequest req, StreamObserver<ProxyMeta.GetIndexResponse> resObserver) {
        super.getIndex(req, resObserver);
    }

    @Override
    public void getIndexes(ProxyMeta.GetIndexesRequest req, StreamObserver<ProxyMeta.GetIndexesResponse> resObserver) {
        super.getIndexes(req, resObserver);
    }

    @Override
    public void getIndexNames(ProxyMeta.GetIndexNamesRequest req, StreamObserver<ProxyMeta.GetIndexNamesResponse> resObserver) {
        super.getIndexNames(req, resObserver);
    }
}
