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

import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.vector.VectorCalcDistance;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.VectorPointDistanceParam;
import io.dingodb.tool.api.ToolService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class VectorPointDistanceOperator extends SoleOutOperator {

    public static final VectorPointDistanceOperator INSTANCE = new VectorPointDistanceOperator();

    public VectorPointDistanceOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        VectorPointDistanceParam param = vertex.getParam();
        param.setContext(context);
        param.getCache().add(tuple);
        return true;
    }

    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        Edge edge = vertex.getSoleEdge();
        VectorPointDistanceParam param = vertex.getParam();
        TupleMapping selection = param.getSelection();
        List<Object[]> cache = param.getCache();
        if (fin instanceof FinWithException) {
            edge.fin(fin);
            return;
        }
        List<List<Float>> rightList = cache.stream().map(e ->
            (List<Float>) e[param.getVectorIndex()]
        ).collect(Collectors.toList());

        if (rightList.size() == 0) {
            edge.fin(fin);
            return;
        }
        VectorCalcDistance vectorCalcDistance = VectorCalcDistance.builder()
            .leftList(Collections.singletonList(param.getTargetVector()))
            .rightList(rightList)
            .dimension(param.getDimension())
            .algorithmType(param.getAlgType())
            .metricType(param.getMetricType())
            .build();
        List<Float> floatArray = ToolService.getDefault().vectorCalcDistance(
            param.getIndexTableId(),
            param.getRangeDistribution().getId(),
            vectorCalcDistance).get(0);

        for (int i = 0; i < cache.size(); i ++) {
            Object[] tuple = cache.get(i);
            Object[] result = Arrays.copyOf(tuple, tuple.length + 1);
            result[tuple.length] = floatArray.get(i);
            edge.transformToNext(param.getContext(), selection.revMap(result));
        }
        param.clear();

        edge.fin(fin);
    }
}
