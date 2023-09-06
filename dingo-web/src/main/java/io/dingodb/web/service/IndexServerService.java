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

import com.google.common.collect.Maps;
import io.dingodb.client.DingoClient;
import io.dingodb.client.common.VectorDistanceArray;
import io.dingodb.client.common.VectorSearch;
import io.dingodb.client.common.VectorWithId;
import io.dingodb.proxy.common.ProxyCommon;
import io.dingodb.proxy.index.IndexServiceGrpc;
import io.dingodb.proxy.index.ProxyIndex;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.index.VectorIndexParameter;
import io.dingodb.sdk.common.vector.VectorCalcDistance;
import io.dingodb.sdk.common.vector.VectorDistanceRes;
import io.dingodb.sdk.common.vector.VectorIndexMetrics;
import io.dingodb.sdk.common.vector.VectorScanQuery;
import io.dingodb.web.annotation.GrpcService;
import io.dingodb.web.utils.Conversion;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.dingodb.web.utils.Conversion.mapping;

@GrpcService
public class IndexServerService extends IndexServiceGrpc.IndexServiceImplBase {

    @Autowired
    private DingoClient dingoClient;

    @Override
    public void vectorAdd(ProxyIndex.VectorAddRequest req, StreamObserver<ProxyIndex.VectorAddResponse> resObserver) {
        List<VectorWithId> reqVectors = req.getVectorsList()
            .stream()
            .map(Conversion::mapping)
            .collect(Collectors.toList());
        List<VectorWithId> resVectors = dingoClient.vectorAdd(req.getSchemaName(), req.getIndexName(), reqVectors,
            req.getReplaceDeleted(),
            req.getIsUpdate());
        ProxyIndex.VectorAddResponse response = ProxyIndex.VectorAddResponse.newBuilder()
            .addAllVectors(resVectors.stream().map(Conversion::mapping).collect(Collectors.toList()))
            .build();
        resObserver.onNext(response);
        resObserver.onCompleted();
    }

    @Override
    public void vectorGet(ProxyIndex.VectorGetRequest req, StreamObserver<ProxyIndex.VectorGetResponse> resObserver) {
        List<Long> ids = req.getVectorIdsList();
        Map<Long, VectorWithId> vectorWithIdMap = dingoClient.vectorBatchQuery(
            req.getSchemaName(),
            req.getIndexName(),
            new HashSet<>(ids),
            req.getWithoutVectorData(),
            req.getWithoutScalarData(),
            req.getSelectedKeysList());
        ProxyIndex.VectorGetResponse response = ProxyIndex.VectorGetResponse.newBuilder()
            .addAllVectors(ids.stream().map(id -> mapping(vectorWithIdMap.get(id))).collect(Collectors.toList()))
            .build();
        resObserver.onNext(response);
        resObserver.onCompleted();
    }

    @Override
    public void vectorSearch(ProxyIndex.VectorSearchRequest req, StreamObserver<ProxyIndex.VectorSearchResponse> resObserver) {
        ProxyCommon.VectorSearchParameter parameter = req.getParameter();
        List<VectorDistanceArray> vectorSearch = dingoClient.vectorSearch(
            req.getSchemaName(),
            req.getIndexName(),
            new VectorSearch(mapping(parameter), req.getVectorsList().stream().map(Conversion::mapping).collect(Collectors.toList())));
        ProxyIndex.VectorSearchResponse response = ProxyIndex.VectorSearchResponse.newBuilder().addAllBatchResults(vectorSearch.stream().map(s ->
            ProxyIndex.VectorWithDistanceResult.newBuilder().addAllVectorWithDistances(s.getVectorWithDistances().stream().map(r ->
                    ProxyCommon.VectorWithDistance.newBuilder()
                        .setVectorWithId(ProxyCommon.VectorWithId.newBuilder()
                            .setId(r.getId())
                            .setVector(mapping(r.getVector()))
                            .setScalarData(mapping(r.getScalarData()))
                            .build())
                        .setDistance(r.getDistance())
                        .setMetricType(ProxyCommon.MetricType.valueOf(r.getMetricType().name()))
                        .build())
                .collect(Collectors.toList())).build()).collect(Collectors.toList())).build();

        resObserver.onNext(response);
        resObserver.onCompleted();
    }

    @Override
    public void vectorDelete(ProxyIndex.VectorDeleteRequest req, StreamObserver<ProxyIndex.VectorDeleteResponse> resObserver) {
        long count = req.getIdsList().stream().distinct().count();
        if (req.getIdsCount() != count) {
            throw new DingoClientException("During the delete operation, duplicate ids are not allowed");
        }
        List<Boolean> vectorDelete = dingoClient.vectorDelete(req.getSchemaName(), req.getIndexName(), req.getIdsList());
        ProxyIndex.VectorDeleteResponse response = ProxyIndex.VectorDeleteResponse.newBuilder().addAllKeyStates(vectorDelete).build();
        resObserver.onNext(response);
        resObserver.onCompleted();
    }

    @Override
    public void vectorGetBorderId(ProxyIndex.VectorGetBorderIdRequest req, StreamObserver<ProxyIndex.VectorGetBorderIdResponse> resObserver) {
        Long id = dingoClient.vectorGetBorderId(req.getSchemaName(), req.getIndexName(), req.getGetMin());
        ProxyIndex.VectorGetBorderIdResponse response = ProxyIndex.VectorGetBorderIdResponse.newBuilder().setId(id).build();
        resObserver.onNext(response);
        resObserver.onCompleted();
    }

    @Override
    public void vectorScanQuery(ProxyIndex.VectorScanQueryRequest req, StreamObserver<ProxyIndex.VectorScanQueryResponse> resObserver) {
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
        List<VectorWithId> withIds = dingoClient.vectorScanQuery(req.getSchemaName(), req.getIndexName(), vectorScanQuery);
        ProxyIndex.VectorScanQueryResponse response = ProxyIndex.VectorScanQueryResponse.newBuilder()
            .addAllVectors(withIds.stream().map(Conversion::mapping).collect(Collectors.toList()))
            .build();
        resObserver.onNext(response);
        resObserver.onCompleted();
    }

    @Override
    public void vectorCalcDistance(ProxyIndex.VectorCalcDistanceRequest req, StreamObserver<ProxyIndex.VectorCalcDistanceResponse> resObserver) {
        VectorCalcDistance calcDistance = new VectorCalcDistance(
            req.getVectorId(),
            VectorCalcDistance.AlgorithmType.valueOf(req.getAlgorithmType().name()),
            VectorIndexParameter.MetricType.valueOf(req.getMetricType().name()),
            req.getOpLeftVectorsList().stream().map(Conversion::mapping).collect(Collectors.toList()),
            req.getOpRightVectorsList().stream().map(Conversion::mapping).collect(Collectors.toList()),
            req.getIsReturnNormalize()
        );

        VectorDistanceRes distanceRes = dingoClient.vectorCalcDistance(req.getSchemaName(), req.getIndexName(), calcDistance);
        ProxyIndex.VectorCalcDistanceResponse response = ProxyIndex.VectorCalcDistanceResponse.newBuilder()
            .addAllOpLeftVectors(distanceRes.getLeftVectors().stream().map(Conversion::mapping).collect(Collectors.toList()))
            .addAllOpRightVectors(distanceRes.getRightVectors().stream().map(Conversion::mapping).collect(Collectors.toList()))
            .addAllDistances(distanceRes.getDistances().stream()
                .map(d -> ProxyIndex.VectorDistance.newBuilder()
                    .addAllInternalDistances(new ArrayList<>(d.getInternalDistances()))
                    .build())
                .collect(Collectors.toList()))
            .build();

        resObserver.onNext(response);
        resObserver.onCompleted();
    }

    @Override
    public void vectorGetRegionMetrics(ProxyIndex.VectorGetRegionMetricsRequest req, StreamObserver<ProxyIndex.VectorGetRegionMetricsResponse> resObserver) {
        VectorIndexMetrics metrics = dingoClient.getRegionMetrics(req.getSchemaName(), req.getIndexName());
        ProxyIndex.VectorGetRegionMetricsResponse response = ProxyIndex.VectorGetRegionMetricsResponse.newBuilder()
            .setMetrics(ProxyCommon.VectorIndexMetrics.newBuilder()
                .setVectorIndexType(ProxyCommon.VectorIndexType.valueOf(metrics.getVectorIndexType().name()))
                .setCurrentCount(metrics.getCurrentCount())
                .setDeletedCount(metrics.getDeletedCount())
                .setMaxId(metrics.getMaxId())
                .setMinId(metrics.getMinId())
                .setMemoryBytes(metrics.getMemoryBytes())
                .build())
            .build();
        resObserver.onNext(response);
        resObserver.onCompleted();
    }
}
