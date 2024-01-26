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

import io.dingodb.common.CommonId;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.Optional;
import io.dingodb.common.vector.VectorSearchResponse;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.PartVectorParam;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@Slf4j
public final class PartVectorOperator extends FilterProjectSourceOperator {
    public static final PartVectorOperator INSTANCE = new PartVectorOperator();

    private PartVectorOperator() {
    }

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator(Vertex vertex) {
        PartVectorParam param = vertex.getParam();
        StoreInstance instance = StoreService.getDefault().getInstance(param.getTableId(), param.getPartId());
        List<Object[]> results = new ArrayList<>();

        // Get all table data response
        List<VectorSearchResponse> searchResponseList = instance.vectorSearch(
            param.getIndexId(),
            param.getFloatArray(),
            param.getTopN(),
            param.getParameterMap());
        for (VectorSearchResponse response : searchResponseList) {
            CommonId regionId = PartitionService.getService(
                    Optional.ofNullable(param.getTable().getPartitionStrategy())
                        .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
                .calcPartId(response.getKey(), param.getDistributions());
            StoreInstance storeInstance = StoreService.getDefault().getInstance(param.getTableId(), regionId);
            KeyValue keyValue = storeInstance.get(response.getKey());
            Object[] decode = param.getCodec().decode(keyValue);
            Object[] result = Arrays.copyOf(decode, decode.length + 1);
            result[decode.length] = response.getDistance();
            results.add(result);
        }
        return results.iterator();
    }

}
