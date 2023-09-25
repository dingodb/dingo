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

package io.dingodb.exec.operator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.vector.VectorCalcDistance;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.tool.service.ToolService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class VectorPointDistanceOperator extends SoleOutOperator {

    private final RangeDistribution rangeDistribution;

    private final Integer vectorIndex;

    private final List<Float> targetVector;

    private final Integer dimension;

    private final String algType;

    private final String metricType;

    @JsonProperty("indexRegionId")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private CommonId indexTableId;

    ToolService toolService;

    List<Object[]> cache;

    public VectorPointDistanceOperator(RangeDistribution rangeDistribution,
                                       Integer vectorIndex,
                                       CommonId indexTableId,
                                       List<Float> targetVector,
                                       Integer dimension,
                                       String algType,
                                       String metricType
                                       ) {
        this.rangeDistribution = rangeDistribution;
        this.vectorIndex = vectorIndex;
        this.indexTableId = indexTableId;
        this.targetVector = targetVector;
        this.dimension = dimension;
        this.algType = algType;
        this.metricType = metricType;
        cache = new LinkedList<>();
        toolService = ToolService.instance;
    }

    @Override
    public void init() {
        super.init();
    }

    @Override
    public boolean push(int pin, @Nullable Object[] tuple) {
        cache.add(tuple);
        return true;
    }

    @Override
    public void fin(int pin, @Nullable Fin fin) {
        if (fin instanceof FinWithException) {
            output.fin(fin);
            return;
        }
        List<List<Float>> rightList = cache.stream().map(e ->
            (List<Float>) e[vectorIndex]
        ).collect(Collectors.toList());

        if (rightList.size() == 0) {
            output.fin(fin);
            return;
        }
        VectorCalcDistance vectorCalcDistance = VectorCalcDistance.builder()
            .leftList(Collections.singletonList(targetVector))
            .rightList(rightList)
            .dimension(dimension)
            .algorithmType(algType)
            .metricType(metricType)
            .build();
        List<Float> floatArray = toolService.vectorCalcDistance(
            indexTableId,
            rangeDistribution.getId(),
            vectorCalcDistance).get(0);

        for (int i = 0; i < cache.size(); i ++) {
            Object[] tuple = cache.get(i);
            Object[] result = Arrays.copyOf(tuple, tuple.length + 1);
            result[tuple.length] = floatArray.get(i);
            output.push(result);
        }
        cache.clear();

        output.fin(fin);
    }
}
