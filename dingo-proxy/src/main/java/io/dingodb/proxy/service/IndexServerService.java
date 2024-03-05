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

import com.google.common.collect.Maps;
import io.dingodb.client.DingoClient;
import io.dingodb.client.common.VectorDistanceArray;
import io.dingodb.client.common.VectorScanQuery;
import io.dingodb.client.common.VectorSearch;
import io.dingodb.client.vector.VectorClient;
import io.dingodb.proxy.annotation.GrpcService;
import io.dingodb.proxy.common.ProxyCommon;
import io.dingodb.proxy.error.ProxyError;
import io.dingodb.proxy.index.IndexServiceGrpc;
import io.dingodb.proxy.index.ProxyIndex;
import io.dingodb.proxy.utils.Conversion;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.service.entity.common.VectorIndexMetrics;
import io.dingodb.sdk.service.entity.common.VectorSearchParameter;
import io.dingodb.sdk.service.entity.common.VectorWithId;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.dingodb.proxy.utils.Conversion.MAPPER;
import static io.dingodb.proxy.utils.Conversion.mapping;

@GrpcService
public class IndexServerService extends IndexServiceGrpc.IndexServiceImplBase {

    @Autowired
    private DingoClient dingoClient;

    @Autowired
    private VectorClient vectorClient;

    @Override
    public void vectorAdd(ProxyIndex.VectorAddRequest req, StreamObserver<ProxyIndex.VectorAddResponse> resObserver) {
        ProxyError.Error.Builder error = ProxyError.Error.newBuilder();
        ProxyIndex.VectorAddResponse.Builder builder = ProxyIndex.VectorAddResponse.newBuilder();
        try {
            List<VectorWithId> reqVectors = req.getVectorsList()
                .stream()
                .map(Conversion::mapping)
                .collect(Collectors.toList());
            List<VectorWithId> resVectors = vectorClient.vectorAdd(req.getSchemaName(), req.getIndexName(), reqVectors,
                req.getReplaceDeleted(),
                req.getIsUpdate());
            builder.addAllVectors(resVectors.stream().map(Conversion::mapping).collect(Collectors.toList()));
            error.setErrcode(ProxyError.Errno.OK);
        } catch (Exception e) {
            List<VectorWithId> result = new ArrayList<>();
            req.getVectorsList().forEach(v -> result.add(null));
            builder.addAllVectors(result.stream().map(Conversion::mapping).collect(Collectors.toList()));
            error.setErrcode(ProxyError.Errno.EINTERNAL).setErrmsg(e.getMessage());
        }
        resObserver.onNext(builder.setError(error.build()).build());
        resObserver.onCompleted();
    }

    @Override
    public void vectorUpsert(
        ProxyIndex.VectorAddRequest req,
        StreamObserver<ProxyIndex.VectorAddResponse> resObserver
    ) {
        ProxyError.Error.Builder error = ProxyError.Error.newBuilder();
        ProxyIndex.VectorAddResponse.Builder builder = ProxyIndex.VectorAddResponse.newBuilder();
        try {
            List<VectorWithId> reqVectors = req.getVectorsList()
                .stream()
                .map(Conversion::mapping)
                .collect(Collectors.toList());
            List<VectorWithId> resVectors = vectorClient.vectorUpsert(
                req.getSchemaName(),
                req.getIndexName(),
                reqVectors
            );
            builder.addAllVectors(resVectors.stream().map(Conversion::mapping).collect(Collectors.toList()));
            error.setErrcode(ProxyError.Errno.OK);
        } catch (Exception e) {
            List<VectorWithId> result = new ArrayList<>();
            req.getVectorsList().forEach(v -> result.add(null));
            builder.addAllVectors(result.stream().map(Conversion::mapping).collect(Collectors.toList()));
            error.setErrcode(ProxyError.Errno.EINTERNAL).setErrmsg(e.getMessage());
        }
        resObserver.onNext(builder.setError(error.build()).build());
        resObserver.onCompleted();
    }

    @Override
    public void vectorGet(ProxyIndex.VectorGetRequest req, StreamObserver<ProxyIndex.VectorGetResponse> resObserver) {
        ProxyIndex.VectorGetResponse.Builder builder = ProxyIndex.VectorGetResponse.newBuilder();
        ProxyError.Error.Builder error = ProxyError.Error.newBuilder();
        try {
            List<Long> ids = req.getVectorIdsList();
            Map<Long, VectorWithId> vectorWithIdMap = vectorClient.vectorBatchQuery(
                req.getSchemaName(),
                req.getIndexName(),
                new HashSet<>(ids),
                req.getWithoutVectorData(),
                req.getWithoutScalarData(),
                req.getSelectedKeysList());
            builder
                .addAllVectors(ids.stream().map(id -> mapping(vectorWithIdMap.get(id))).collect(Collectors.toList()));
            error.setErrcode(ProxyError.Errno.OK);
        } catch (Exception e) {
            error.setErrcode(ProxyError.Errno.EINTERNAL).setErrmsg(e.getMessage());
        }
        resObserver.onNext(builder.setError(error.build()).build());
        resObserver.onCompleted();
    }

    @Override
    public void vectorSearch(
        ProxyIndex.VectorSearchRequest req,
        StreamObserver<ProxyIndex.VectorSearchResponse> resObserver) {
        ProxyIndex.VectorSearchResponse.Builder builder = ProxyIndex.VectorSearchResponse.newBuilder();
        ProxyError.Error.Builder error = ProxyError.Error.newBuilder();
        try {
            ProxyCommon.VectorSearchParameter parameter = req.getParameter();
            VectorSearchParameter searchParameter = MAPPER.mapping(parameter);
            searchParameter.setWithoutTableData(true);
            List<VectorDistanceArray> vectorSearch = vectorClient.vectorSearch(
                req.getSchemaName(),
                req.getIndexName(),
                new VectorSearch(
                    searchParameter,
                    req.getVectorsList().stream().map(Conversion::mapping).collect(Collectors.toList()))
            );
            builder.addAllBatchResults(vectorSearch.stream().map(s -> ProxyIndex.VectorWithDistanceResult.newBuilder()
                .addAllVectorWithDistances(s.getVectorWithDistances().stream().map(r ->
                        ProxyCommon.VectorWithDistance.newBuilder()
                            .setId(r.getVectorWithId().getId())
                            .setVector(MAPPER.mapping(r.getVectorWithId().getVector()))
                            .putAllScalarData(mapping(r.getVectorWithId().getScalarData()))
                            .setDistance(r.getDistance())
                            .setMetricType(ProxyCommon.MetricType.valueOf(r.getMetricType().name()))
                            .build())
                    .collect(Collectors.toList())).build()).collect(Collectors.toList()));
            error.setErrcode(ProxyError.Errno.OK);
        } catch (Exception e) {
            error.setErrcode(ProxyError.Errno.EINTERNAL).setErrmsg(e.getMessage());
        }

        resObserver.onNext(builder.setError(error.build()).build());
        resObserver.onCompleted();
    }

    @Override
    public void vectorDelete(
        ProxyIndex.VectorDeleteRequest req,
        StreamObserver<ProxyIndex.VectorDeleteResponse> resObserver) {
        ProxyIndex.VectorDeleteResponse.Builder builder = ProxyIndex.VectorDeleteResponse.newBuilder();
        ProxyError.Error.Builder error = ProxyError.Error.newBuilder();
        try {
            long count = req.getIdsList().stream().distinct().count();
            if (req.getIdsCount() != count) {
                throw new DingoClientException("During the delete operation, duplicate ids are not allowed");
            }
            List<Boolean> vectorDelete = vectorClient.vectorDelete(
                req.getSchemaName(),
                req.getIndexName(),
                req.getIdsList());
            builder.addAllKeyStates(vectorDelete);
            error.setErrcode(ProxyError.Errno.OK);
        } catch (Exception e) {
            error.setErrcode(ProxyError.Errno.EINTERNAL).setErrmsg(e.getMessage());
        }
        resObserver.onNext(builder.setError(error.build()).build());
        resObserver.onCompleted();
    }

    @Override
    public void vectorGetBorderId(
        ProxyIndex.VectorGetBorderIdRequest req,
        StreamObserver<ProxyIndex.VectorGetBorderIdResponse> resObserver) {
        ProxyIndex.VectorGetBorderIdResponse.Builder builder = ProxyIndex.VectorGetBorderIdResponse.newBuilder();
        ProxyError.Error.Builder error = ProxyError.Error.newBuilder();
        try {
            Long id = vectorClient.vectorGetBorderId(req.getSchemaName(), req.getIndexName(), req.getGetMin());
            builder.setId(id);
            error.setErrcode(ProxyError.Errno.OK);
        } catch (Exception e) {
            error.setErrcode(ProxyError.Errno.EINTERNAL).setErrmsg(e.getMessage());
        }
        resObserver.onNext(builder.setError(error.build()).build());
        resObserver.onCompleted();
    }

    @Override
    public void vectorScanQuery(
        ProxyIndex.VectorScanQueryRequest req,
        StreamObserver<ProxyIndex.VectorScanQueryResponse> resObserver) {
        ProxyIndex.VectorScanQueryResponse.Builder builder = ProxyIndex.VectorScanQueryResponse.newBuilder();
        ProxyError.Error.Builder error = ProxyError.Error.newBuilder();
        try {
            VectorScanQuery vectorScanQuery = new VectorScanQuery(
                req.getVectorIdStart(),
                req.getIsReverseScan(),
                req.getMaxScanCount(),
                req.getVectorIdEnd(),
                req.getWithoutVectorData(),
                req.getWithoutScalarData(),
                req.getSelectedKeysList(),
                req.getWithoutTableData(),
                req.getUseScalarFilter(),
                req.getScalarForFilter().getScalarDataMap().entrySet().stream().collect(
                    Maps::newHashMap,
                    (map, entry) -> map.put(entry.getKey(), mapping(entry.getValue())),
                    Map::putAll));
            List<VectorWithId> withIds = vectorClient.vectorScanQuery(
                req.getSchemaName(),
                req.getIndexName(),
                vectorScanQuery);
            builder.addAllVectors(withIds.stream().map(Conversion::mapping).collect(Collectors.toList()));
            error.setErrcode(ProxyError.Errno.OK);
        } catch (Exception e) {
            error.setErrcode(ProxyError.Errno.EINTERNAL).setErrmsg(e.getMessage());
        }
        resObserver.onNext(builder.setError(error.build()).build());
        resObserver.onCompleted();
    }

    @Override
    public void vectorGetRegionMetrics(
        ProxyIndex.VectorGetRegionMetricsRequest req,
        StreamObserver<ProxyIndex.VectorGetRegionMetricsResponse> resObserver) {
        ProxyIndex.VectorGetRegionMetricsResponse.Builder builder = ProxyIndex.VectorGetRegionMetricsResponse.newBuilder();
        ProxyError.Error.Builder error = ProxyError.Error.newBuilder();
        try {
            VectorIndexMetrics metrics = vectorClient.getRegionMetrics(req.getSchemaName(), req.getIndexName());
            builder.setMetrics(ProxyCommon.VectorIndexMetrics.newBuilder()
                    .setVectorIndexType(ProxyCommon.VectorIndexType.valueOf(metrics.getVectorIndexType().name()))
                    .setCurrentCount(metrics.getCurrentCount())
                    .setDeletedCount(metrics.getDeletedCount())
                    .setMaxId(metrics.getMaxId())
                    .setMinId(metrics.getMinId())
                    .setMemoryBytes(metrics.getMemoryBytes())
                    .build());
            error.setErrcode(ProxyError.Errno.OK);
        } catch (Exception e) {
            error.setErrcode(ProxyError.Errno.EINTERNAL).setErrmsg(e.getMessage());
        }
        resObserver.onNext(builder.setError(error.build()).build());
        resObserver.onCompleted();
    }

    @Override
    public void vectorCount(
        ProxyIndex.VectorCountRequest req, StreamObserver<ProxyIndex.VectorCountResponse> resObserver) {
        ProxyIndex.VectorCountResponse.Builder builder = ProxyIndex.VectorCountResponse.newBuilder();
        ProxyError.Error.Builder error = ProxyError.Error.newBuilder();
        try {
            Long count = vectorClient.vectorCount(req.getSchemaName(), req.getIndexName());
            builder.setCount(count);
            error.setErrcode(ProxyError.Errno.OK);
        } catch (Exception e) {
            error.setErrcode(ProxyError.Errno.EINTERNAL).setErrmsg(e.getMessage());
        }

        resObserver.onNext(builder.setError(error.build()).build());
        resObserver.onCompleted();
    }
}
