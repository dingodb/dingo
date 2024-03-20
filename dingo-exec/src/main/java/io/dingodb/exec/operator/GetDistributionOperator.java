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
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.GetDistributionParam;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.IntStream;

public class GetDistributionOperator extends SourceOperator {
    public static final GetDistributionOperator INSTANCE = new GetDistributionOperator();

    private GetDistributionOperator() {
    }

    @Override
    public boolean push(Context context, Vertex vertex) {
        GetDistributionParam param = vertex.getParam();
        Table td = param.getTable();
        PartitionService ps = PartitionService.getService(
            Optional.ofNullable(td.getPartitionStrategy())
                .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));

        for (Object[] keyTuple : param.getKeyTuples()) {
            TupleMapping keyMapping = param.getKeyMapping();
            boolean allMatch = keyMapping.stream().allMatch(i -> Objects.isNull(keyTuple[i]));
            if (allMatch) {
                return false;
            }
            CommonId partId = ps.calcPartId(param.getCodec().encodeKey(keyTuple), param.getDistributions());
            RangeDistribution distribution = RangeDistribution.builder().id(partId).build();
            context.setDistribution(distribution);
            vertex.getSoleEdge().transformToNext(context, keyTuple);
        }

        return false;
    }

}
