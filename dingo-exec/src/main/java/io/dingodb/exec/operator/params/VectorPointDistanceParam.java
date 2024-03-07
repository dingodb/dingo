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

package io.dingodb.exec.operator.params;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.type.TupleMapping;
import lombok.Getter;

import java.util.LinkedList;
import java.util.List;

@Getter
@JsonTypeName("vectorPoint")
@JsonPropertyOrder({"dimension"})
public class VectorPointDistanceParam extends AbstractParams {

    private final RangeDistribution rangeDistribution;

    private final Integer vectorIndex;

    private final List<Float> targetVector;
    @JsonProperty("dimension")
    private final Integer dimension;

    private final String algType;

    private final String metricType;

    private final CommonId indexTableId;

    private final List<Object[]> cache;

    private final TupleMapping selection;

    public VectorPointDistanceParam(
        RangeDistribution rangeDistribution,
        Integer vectorIndex,
        CommonId indexTableId,
        List<Float> targetVector,
        Integer dimension,
        String algType,
        String metricType,
        TupleMapping selection
    ) {
        this.rangeDistribution = rangeDistribution;
        this.vectorIndex = vectorIndex;
        this.targetVector = targetVector;
        this.dimension = dimension;
        this.algType = algType;
        this.metricType = metricType;
        this.indexTableId = indexTableId;
        cache = new LinkedList<>();
        this.selection = selection;
    }

    public void clear() {
        cache.clear();
    }

}
