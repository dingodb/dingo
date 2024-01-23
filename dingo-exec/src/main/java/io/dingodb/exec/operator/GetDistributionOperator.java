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
import io.dingodb.common.util.Optional;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Content;
import io.dingodb.exec.operator.params.GetDistributionParam;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;

public class GetDistributionOperator extends SourceOperator {
    public static final GetDistributionOperator INSTANCE = new GetDistributionOperator();

    private GetDistributionOperator() {
    }

    @Override
    public boolean push(Vertex vertex) {
        GetDistributionParam param = vertex.getParam();
        Table td = param.getTable();
        PartitionService ps = PartitionService.getService(
            Optional.ofNullable(td.getPartitionStrategy())
                .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));

        for (Object[] keyTuple : param.getKeyTuples()) {
            CommonId partId = ps.calcPartId(param.getCodec().encodeKey(keyTuple), param.getDistributions());
            RangeDistribution distribution = RangeDistribution.builder().id(partId).build();
            Content content = Content.builder().distribution(distribution).build();
            vertex.getSoleEdge().transformToNext(content, keyTuple);
        }

        return false;
    }

}
