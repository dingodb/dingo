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

package io.dingodb.tool.service;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.vector.VectorCalcDistance;
import io.dingodb.sdk.service.Services;
import io.dingodb.sdk.service.UtilService;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.common.MetricType;
import io.dingodb.sdk.service.entity.common.ValueType;
import io.dingodb.sdk.service.entity.common.Vector;
import io.dingodb.sdk.service.entity.index.AlgorithmType;
import io.dingodb.sdk.service.entity.index.VectorCalcDistanceRequest;
import io.dingodb.sdk.service.entity.index.VectorCalcDistanceResponse;
import io.dingodb.sdk.service.entity.index.VectorDistance;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ToolService implements io.dingodb.tool.api.ToolService {

    public static final ToolService DEFAULT_INSTANCE = new ToolService();

    @AutoService(io.dingodb.tool.api.ToolServiceProvider.class)
    public static final class ToolServiceProvider implements io.dingodb.tool.api.ToolServiceProvider{

        @Override
        public ToolService get() {
            return DEFAULT_INSTANCE;
        }
    }
    private final Set<Location> coordinators;
    private ToolService() {
        coordinators = Services.parse(DingoConfiguration.instance().find("coordinators", String.class));
    }

    @Override
    public List<List<Float>> vectorCalcDistance(CommonId region,
                                                VectorCalcDistance distance) {
        VectorCalcDistanceRequest request = buildRequest(distance);
        UtilService utilService = Services.utilService(coordinators, region.seq, 30);
        VectorCalcDistanceResponse response = utilService.vectorCalcDistance(request);

        return response.getDistances().stream().map(VectorDistance::getInternalDistances).collect(Collectors.toList());
    }

    private static VectorCalcDistanceRequest buildRequest(VectorCalcDistance distance) {
        AlgorithmType algorithmType;
        switch (distance.getAlgorithmType().toUpperCase()) {
            case "HNSW":
                algorithmType = AlgorithmType.ALGORITHM_HNSWLIB;
                break;
            case "FLAT":
            default:
                algorithmType = AlgorithmType.ALGORITHM_FAISS;
                break;
        }
        MetricType metricType;
        switch (distance.getMetricType().toUpperCase()) {
            case "INNER_PRODUCT":
                metricType = MetricType.METRIC_TYPE_INNER_PRODUCT;
                break;
            case "COSINE":
                metricType = MetricType.METRIC_TYPE_COSINE;
                break;
            case "L2":
            default:
                metricType = MetricType.METRIC_TYPE_L2;
                break;
        }
        return VectorCalcDistanceRequest.builder()
            .algorithmType(algorithmType)
            .metricType(metricType)
            .opLeftVectors(distance.getLeftList().stream()
                .map(l -> Vector.builder().floatValues(l).dimension(distance.getDimension()).valueType(ValueType.FLOAT).build())
                .collect(Collectors.toList()))
            .opRightVectors(distance.getRightList().stream()
                .map(r -> Vector.builder().valueType(ValueType.FLOAT).dimension(distance.getDimension()).floatValues(r).build())
                .collect(Collectors.toList()))
            .isReturnNormlize(false)
            .build();
    }

}
