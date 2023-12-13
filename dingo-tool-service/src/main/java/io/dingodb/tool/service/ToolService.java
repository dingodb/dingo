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
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.SDKCommonId;
import io.dingodb.sdk.common.index.VectorIndexParameter;
import io.dingodb.sdk.common.vector.Vector;
import io.dingodb.sdk.common.vector.VectorCalcDistance;
import io.dingodb.sdk.common.vector.VectorDistance;
import io.dingodb.sdk.common.vector.VectorDistanceRes;
import io.dingodb.sdk.service.meta.MetaServiceClient;
import io.dingodb.sdk.service.util.UtilServiceClient;

import java.util.List;
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

    private final MetaServiceClient metaService;
    private final UtilServiceClient utilService;

    private ToolService() {
        String coordinators = DingoConfiguration.instance().find("coordinators", String.class);
        metaService = new MetaServiceClient(coordinators);
        utilService = new UtilServiceClient(metaService);
    }

    @Override
    public List<List<Float>> vectorCalcDistance(CommonId indexId,
                                                CommonId regionId,
                                                io.dingodb.common.vector.VectorCalcDistance vectorCalcDistance) {
        VectorDistanceRes vectorDistanceRes = utilService.vectorCalcDistance(mapping(indexId),
            mapping(regionId), mapping(vectorCalcDistance));
        return vectorDistanceRes.getDistances().stream()
            .map(VectorDistance::getInternalDistances).collect(Collectors.toList());
    }

    public static DingoCommonId mapping(CommonId commonId) {
        return new SDKCommonId(DingoCommonId.Type.values()[commonId.type.code], commonId.domain, commonId.seq);
    }

    public static VectorCalcDistance mapping(io.dingodb.common.vector.VectorCalcDistance vectorCalcDistance) {
        VectorCalcDistance.AlgorithmType algorithmType;
        switch (vectorCalcDistance.getAlgorithmType().toUpperCase()) {
            case "HNSW":
                algorithmType = VectorCalcDistance.AlgorithmType.ALGORITHM_HNSWLIB;
                break;
            case "FLAT":
            default:
                algorithmType = VectorCalcDistance.AlgorithmType.ALGORITHM_FAISS;
                break;
        }
        VectorIndexParameter.MetricType metricType;
        switch (vectorCalcDistance.getMetricType().toUpperCase()) {
            case "INNER_PRODUCT":
                metricType = VectorIndexParameter.MetricType.METRIC_TYPE_INNER_PRODUCT;
                break;
            case "COSINE":
                metricType = VectorIndexParameter.MetricType.METRIC_TYPE_COSINE;
                break;
            case "L2":
            default:
                metricType = VectorIndexParameter.MetricType.METRIC_TYPE_L2;
                break;
        }

        List<Vector> left = vectorCalcDistance.getLeftList().stream().map(e ->
            Vector.getFloatInstance(vectorCalcDistance.getDimension(), e)
        ).collect(Collectors.toList());

        List<Vector> right = vectorCalcDistance.getRightList().stream().map(e ->
            Vector.getFloatInstance(vectorCalcDistance.getDimension(), e)).collect(Collectors.toList());
        return new VectorCalcDistance(vectorCalcDistance.getVectorId(), algorithmType, metricType,
            left, right, false);
    }

}
