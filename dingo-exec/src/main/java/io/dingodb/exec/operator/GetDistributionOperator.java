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
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.GetDistributionParam;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.stream.IntStream;

@Slf4j
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

        Integer retry = Optional.mapOrGet(DingoConfiguration.instance().find("retry", int.class), __ -> __, () -> 30);
        for (Object[] keyTuple : param.getKeyTuples()) {
            TupleMapping keyMapping = param.getKeyMapping();
            boolean allMatch = keyMapping.stream().allMatch(i -> Objects.isNull(keyTuple[i]));
            if (allMatch) {
                return false;
            }
            while (retry-- > 0) {
                try {
                    CommonId partId = ps.calcPartId(param.getCodec().encodeKey(keyTuple), param.getDistributions());
                    RangeDistribution distribution = RangeDistribution.builder().id(partId).build();
                    context.setDistribution(distribution);
                    vertex.getSoleEdge().transformToNext(context, keyTuple);
                    break;
                } catch (RegionSplitException e) {
                    LogUtils.error(log, e.getMessage());
                    NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distribution =
                        MetaService.root().getRangeDistribution(td.getTableId());
                    param.setDistributions(distribution);
                }
            }
        }

        return false;
    }

}
